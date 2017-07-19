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

#include <algorithm>

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
#endif

EventProcessor::ThreadFxn_t EventProcessor::
ThreadGroupDescriptor::s_dfltRunFxn = ThreadFxn_t( [](const InitFxn_t &readyFxn) { 
                                             std::unique_ptr<EThread> _base{new EThread{REGULAR,0}}; // store an EThread obj
                                             readyFxn(_base.get());
                                             _base->execute();
                                          } );

EventType
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize)
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
  ink_release_assert( pthread_barrier_init(&grpBarrier,nullptr,n_threads+1) == 0 );

  for (int i = 0; i < n_threads; ++i)
  {
    EThread *&epEthread = all_ethreads[n_ethreads_nxt++];
    tg->start(grpBarrier, epEthread, stacksize);
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

  thread_group[ET_CALL]._affcfg = kAffinity_objs[affinity];

  this->spawn_event_threads(ET_CALL, n_event_threads, stacksize);

  Debug("iocore_thread", "Created event thread group id %d with %d threads", ET_CALL, n_event_threads);
  return 0;
}

void
EventProcessor::shutdown()
{
}

ink_thread
EventProcessor::ThreadGroupDescriptor::start(pthread_barrier_t &grpBarrier, EThread *&epEthread, int stacksize, const ThreadFxn_t &runFxn)
{
  int const affid = numa::new_affinity_id();
  ink_semaphore stackWait;

  auto launchFxnPassed = [this,&epEthread,affid,&stackWait,&grpBarrier](EThread *t) -> int
  {
    EThread *&tgEthread = _thread[_count++]; // keep unique location
    ptrdiff_t id = ( &tgEthread - &_thread[0] );

    char thr_name[MAX_THREAD_NAME_LENGTH+1];
    snprintf(thr_name, sizeof(thr_name), "[%s %ld]", _name.get(), id);
    ink_set_thread_name(thr_name);
    
    tgEthread = t;
    epEthread = t;

    t->id = id; // index
    t->set_affinity_id(affid);
    t->set_specific();

    // parallel with parent now 
    ink_sem_post(&stackWait); 
    
    // gather up all threads before running
    pthread_barrier_wait(&grpBarrier);

    // change to new cpuset [real delay]
    numa::assign_thread_cpuset_by_affinity(_affcfg, affid); 

    auto cpuset = numa::get_cpuset_by_affinity(_affcfg, affid); 
    pretty_print_cpuset(thr_name, static_cast<hwloc_obj_type_t>(_affcfg), affid, cpuset);

    // perform all inits for EThread 
    for( auto &&cb : _spawnQueue ) {
      cb(t);
    }

    return id;
  };

  auto startFxn = [&runFxn,&launchFxnPassed]() {
    // local copy [parent is blocked]
    InitFxn_t launchFxn{launchFxnPassed};
    runFxn(launchFxn); 
  };

  // change to new memory
  numa::assign_thread_memory_by_affinity(_affcfg, affid); 

  return Thread::start(stackWait, stacksize, startFxn);
}


ink_thread 
EventProcessor::spawn_thread(Continuation *cont, const char *thr_name, size_t stacksize)
{
  EThread *&epEthread = all_dthreads[n_dthreads_nxt++];
  ink_semaphore stackWait;

  auto launchFxnPassed = [this,&epEthread,&stackWait,thr_name,cont](EThread *t) -> int
  {
    ink_set_thread_name(thr_name);

    epEthread = t;

    t->set_specific();

    // parallel with parent now 
    ink_sem_post(&stackWait);

    return 0;
  };

  auto startFxn = [&launchFxnPassed,cont]()
  {
    std::unique_ptr<EThread> _base{ new EThread };
    InitFxn_t launchFxn{launchFxnPassed};

    Continuation *contPtr = cont;

    launchFxn(_base.get());

    // parent can continue 

    contPtr->handleEvent(EVENT_IMMEDIATE, nullptr);
  };

  return Thread::start(stackWait, stacksize, startFxn);
}
