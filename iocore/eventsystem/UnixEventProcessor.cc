/** @file

  A brief file description

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

#include "P_EventSystem.h" /* MAGIC_EDITING_TAG */
#include <sched.h>
#if TS_USE_HWLOC
#if HAVE_ALLOCA_H
#include <alloca.h>
#endif
#include <hwloc.h>
#endif
#include "ts/ink_defs.h"
#include "ts/hugepages.h"

/// Global singleton.
class EventProcessor eventProcessor;

#if TS_USE_HWLOC
static const hwloc_obj_type_t kAffinity_objs[] = 
   { HWLOC_OBJ_MACHINE, HWLOC_OBJ_NUMANODE, HWLOC_OBJ_SOCKET, HWLOC_OBJ_CORE
#if HAVE_HWLOC_OBJ_PU
         , HWLOC_OBJ_PU  
#endif
  };

static const char *const kAffinity_obj_names[] =
   { "[Unrestricted]",         "NUMA Node",        "Socket",         "Core"
         , "Logical CPU" 
   };

// Pretty print our CPU set
void pretty_print_cpuset(const char *thrname, hwloc_obj_type_t objtype, int affid, hwloc_const_cpuset_t cpuset)
{
  auto n = std::find( std::begin(kAffinity_objs), std::end(kAffinity_objs), objtype ) - std::begin(kAffinity_objs);
  n %= countof(kAffinity_objs); // remap end to index zero
  const char *objname = kAffinity_obj_names[n];

#if HWLOC_API_VERSION >= 0x00010100
  int cpu_mask_len = hwloc_bitmap_snprintf(NULL, 0, cpuset);
  char cpu_mask[cpu_mask_len+1];
  hwloc_bitmap_snprintf(cpu_mask, sizeof(cpu_mask), cpuset);
  Debug("iocore_thread", "EThread: %s -> %s# %d CPU Mask: %s", thrname, objname, affid, cpu_mask);
#else
  Debug("iocore_thread", "EThread: %s -> %s# %d", thrname, objname, affid);
#endif // HWLOC_API_VERSION
}


EventProcessor::EventProcessor()
{
  ink_zero(all_ethreads);
  ink_zero(all_dthreads);
  ink_mutex_init(&dedicated_thread_spawn_mutex);
  // Because ET_NET is compile time set to 0 it *must* be the first type registered.
  this->register_event_type("ET_NET");
}

EventProcessor::~EventProcessor()
{
  ink_mutex_destroy(&dedicated_thread_spawn_mutex);
}

Event *
EventProcessor::schedule_spawn(void (*f)(EThread *), EventType ev_type)
{
  ink_assert(ev_type < MAX_EVENT_TYPES);
  thread_group[ev_type]._spawnQueue.push_back(f);
  return nullptr;
}

EventType
EventProcessor::register_event_type(char const *name)
{
  ThreadGroupDescriptor *tg = &(thread_group[n_thread_groups++]);
  ink_release_assert(n_thread_groups <= MAX_EVENT_TYPES); // check for overflow

  tg->_name = ats_strdup(name);
  return n_thread_groups - 1;
}

EventType
EventProcessor::spawn_event_threads(char const *name, int n_threads, size_t stacksize)
{
  int ev_type = this->register_event_type(name);
  this->spawn_event_threads(ev_type, n_threads, stacksize);
  return ev_type;
}

EventType
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize, EThread::ThreadFxn &runFxn)
{
  ThreadGroupDescriptor *tg = &(thread_group[ev_type]);

  ink_release_assert(n_threads > 0);
  ink_release_assert((n_ethreads + n_threads) <= MAX_EVENT_THREADS);
  ink_release_assert(ev_type < MAX_EVENT_TYPES);

  stacksize = std::max(stacksize, static_cast<decltype(stacksize)>(INK_THREAD_STACK_MIN));
  // Make sure it is a multiple of our page size
  if (ats_hugepage_enabled()) {
    stacksize = INK_ALIGN(stacksize, ats_hugepage_size());
  } else {
    stacksize = INK_ALIGN(stacksize, ats_pagesize());
  }

  Debug("iocore_thread", "Thread stack size set to %zu", stacksize);

  pthread_barrier_t grpBarrier;
  pthread_barrier_init(&grpBarrier,nullptr,n_threads+1);

  for (int i = 0; i < n_threads; ++i) 
  {
    EThread *&epEthread = all_ethreads[n_ethreads + i];
    tg->start(grpBarrier, stacksize, epEthread, runFxn);
  }

  // gather up all threads to one point before completion ...
  pthread_barrier_wait(&grpBarrier);

  Debug("iocore_thread", "Created thread group '%s' id %d with %d threads", tg->_name.get(), ev_type, n_threads);

  return ev_type; // useless but not sure what would be better.
}

int
EventProcessor::start(int n_event_threads, size_t stacksize)
{
  // do some sanity checking.
  static bool started = false;
  ink_release_assert(!started);
  ink_release_assert(n_event_threads > 0 && n_event_threads <= MAX_EVENT_THREADS);
  started = true;

  int affinity = 1;
  REC_ReadConfigInteger(affinity, "proxy.config.exec_thread.affinity");

  thread_group[ET_NET]._affcfg = affinity;


  this->spawn_event_threads(ET_CALL, n_event_threads, stacksize);

  Debug("iocore_thread", "Created event thread group id %d with %d threads", ET_CALL, n_event_threads);
  return 0;
}

void
EventProcessor::shutdown()
{
}

Event *
EventProcessor::spawn_thread(Continuation *cont, const char *thr_name, size_t stacksize)
{
  tg->start(grpBarrier, stacksize, epEthread, runFxn);

  /* Spawning threads in a live system - There are two potential race conditions in this logic. The
     first is multiple calls to this method.  In that case @a all_dthreads can end up in a bad state
     as the same entry is overwritten while another is left uninitialized.

     The other is read/write contention where another thread (e.g. the stats collection thread) is
     iterating over the threads while the active count (@a n_dthreads) is being updated causing use
     of a not yet initialized array element.

     This logic covers both situations. For write/write the actual array update is locked. The
     potentially expensive set up is done outside the lock making the time spent locked small. For
     read/write it suffices to do the active count increment after initializing the array
     element. It's not a problem if, for one cycle, a new thread is skipped.
  */

  pthread_barrier_t grpBarrier;
  pthread_barrier_init(&grpBarrier,nullptr,n_threads+1);

  EThread *&epEthread = all_ethreads[n_ethreads + i];
  XXXX
  tg->start(grpBarrier, stacksize, epEthread, runFxn);

  // gather up all threads to one point before completion ...
  pthread_barrier_wait(&grpBarrier);
  {
    ink_scoped_mutex_lock lock(dedicated_thread_spawn_mutex);
    ink_release_assert(n_dthreads < MAX_EVENT_THREADS);
    all_dthreads[n_dthreads] = e->ethread;
    ++n_dthreads; // Be very sure this is after the array element update.
  }

  e->ethread->start(thr_name, nullptr, stacksize);

  return e;
}

ink_thread_t
EventProcessor::ThreadGroup::start(pthread_barrier_t &grpBarrier, EThread *&epEthread, int stacksize, const EThread::ThreadFxn &runFxn)
{
  EThread *&tgEthread = _threads[_count++]; // keep unique location
  int const affid = numa::next_affinity_id();
  ink_semaphore stackWait;

  auto launchFxnPassed = [this,tgEthread,epEthread,affid,&stackWait,&grpBarrier](EThread *t) -> int
  {
    ptrdiff_t id = ( &tgEthread - &_threads );

    {
      char thr_name[MAX_THREAD_NAME_LENGTH];
      snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[%s %d]", _name.get(), id);
      ink_set_thread_name(buff);
    }
    
    tgEthread = t;
    epEthread = t;

    t->id = id; // index
    t->tid_ = pthread_self();
    t->affid_ = affid;
    t->set_specific();

    // parallel with parent now 
    ink_sem_post(&stackWait); 
    
    // [ parent destroys semaphore ]

    // gather up all threads before running
    pthread_barrier_wait(&grpBarrier);

    // change to new cpuset [real delay]
    numa::assign_thread_cpuset_affinity(afftype, affid); 

    // perform all inits for EThread 
    for( auto &&cb : _spawnQueue ) {
      cb(t);
    }

    return id;
  };

  auto startFxn = [&runFxn,&launchFxnPassed]() -> int {
    // local copy [parent is blocked]
    EThread::InitFxn launchFxn = launchFxnPassed;
    return runFxn(launchFxn); 
  };

  auto threadHook = [](void *ptr) -> void*
    { return static_cast<decltype(startFxn)*>(ptr)->operator()(); };

  // Make sure it is a multiple of our page size
  auto page = (ats_hugepage_enabled() ? ats_hugepage_size() : ats_pagesize() );
  stacksize = aligned_spacing( stacksize, page );

  // change to new memory
  numa::assign_thread_memory_affinity(kAffinity_objs[_affcfg], affid); 

  alloc_stack(stacksize);

  ink_sem_init(&stackWait,0);

  auto tid = ink_thread_create(threadHook, &startFxn, false, stacksize, stack);

  // wait on child init 
  ink_sem_wait(&stackWait);
  ink_sem_destroy(&stackWait);

  return tid;
}

// eventProcessor.schedule_spawn(&initialize_thread_for_net, ET_NET);'
// eventProcessor.schedule_spawn(&initialize_thread_for_net, ET_DNS);'
// eventProcessor.schedule_spawn(&initialize_thread_for_udp_net, ET_UDP);'
// eventProcessor.schedule_spawn([](EThread *thread){ thread->server_session_pool = new ServerSessionPool; }, ET_NET);'

// eventProcessor.spawn_event_threads(ET_NET, n_threads, stacksize);
// eventProcessor.spawn_event_threads(ET_DNS, 1, stacksize);
// eventProcessor.spawn_event_threads("ET_TASK", std::max(1, task_threads), stacksize);
// eventProcessor.spawn_event_threads("ET_OCSP", 1, stacksize);
// eventProcessor.spawn_event_threads(ET_UDP, n_upd_threads, stacksize);
// eventProcessor.spawn_event_threads("ET_REMAP", num_threads, stacksize);
