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
         , HWLOC_OBJ_PU   // include only if valid in HWLOC version
#endif
  };

static const char *const kAffinity_obj_names[] =
   { "[Unrestricted]",         "NUMA Node",        "Socket",         "Core"
         , "Logical CPU"  // [ignore if not found]
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

//
// s_dfltRunFxn: Default stateless thread lifetime hook.  [i.e. replaceable by custom fxns]
//
EventProcessor::ThreadFxn_t EventProcessor::
ThreadGroupDescriptor::s_dfltRunFxn = ThreadFxn_t( 
   // readyFxn functor 
   //    1) refers to a new thread-stack lambda "launchFxn" ...
   //    2) stack-constructed from the ref an parent-stack lambda 
   //    3) using both "start()::launchFxnPassed" and "start()::startFxn" ...
   //
   // therefore none of the data in startFxn or launchFxnPassed references can be used again
   [](const InitFxn_t &readyFxn) 
   {
     // parent thread is waiting
     std::unique_ptr<EThread> _base{new EThread{REGULAR,0}}; // store an EThread obj

     // init and set up EThread 
     readyFxn(_base.get());
     // [stackWait-releases parent when done]

     // free-running thread now
     _base->execute(); 

     // thread is complete
   } );

EventType
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize)
{
  ink_release_assert( 0 < n_threads && n_threads <= (MAX_EVENT_THREADS - n_ethreads) );
  ink_release_assert(ev_type < MAX_EVENT_TYPES);

  // Determine correct multiple of our page size
  auto page = (ats_hugepage_enabled() ? sizeof(MemoryPageHuge) : sizeof(MemoryPage) );

  // correctly define size of stack now
  stacksize = std::max(stacksize, static_cast<decltype(stacksize)>(MIN_STACKSIZE));
  stacksize = aligned_spacing( stacksize, page );

  Debug("iocore_thread", "Thread stack size set to %zu", stacksize);

  //
  // hold all threads until all and stored values are ready
  pthread_barrier_t grpBarrier;
  ink_release_assert( pthread_barrier_init(&grpBarrier,nullptr,n_threads+1) == 0 );

  for (int i = 0; i < n_threads; ++i)
  {
    // *atomic-allocate* a new EThread-hook in whole-system list
    EThread *&epEThread = all_ethreads[n_ethreads_nxt++];

    ink_release_assert( ! epEThread ); // must hold nothing

    // pass barrier and external EThread-hook into thread-group for spawn
    thread_group[ev_type].start(grpBarrier, epEThread, stacksize, ThreadGroupDescriptor::s_dfltRunFxn);

    // thread has allocated and initialized its EThread 
    // now only waiting on barrier
  }

  // all threads are ready when passed
  pthread_barrier_wait(&grpBarrier);

  // count threads as visible
  n_ethreads = n_ethreads_nxt; 

  Debug("iocore_thread", "Created thread group '%s' id %d with %d threads", thread_group[ev_type]._name.get(), ev_type, n_threads);

  return ev_type; // useless but not sure what would be better.
}

int
EventProcessor::start(int n_event_threads, size_t stacksize)
{
  // do some sanity checking.
  static bool started = false;

  ink_release_assert(!started);
  ink_release_assert( 0 < n_event_threads && n_event_threads <= MAX_EVENT_THREADS);

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
EventProcessor::ThreadGroupDescriptor::start(pthread_barrier_t &grpBarrier, EThread *&epEThread, int stacksize, const ThreadFxn_t &runFxn)
{
  int const affid = numa::new_affinity_id();
  ink_semaphore stackWait;

  auto launchFxnPassed = [this,&epEThread,affid,&stackWait,&grpBarrier](EThread *t) -> int
  {
    EThread *&tgEThread = _thread[_count++]; // keep unique location
    ptrdiff_t id = ( &tgEThread - &_thread[0] );

    char thr_name[MAX_THREAD_NAME_LENGTH+1];
    snprintf(thr_name, sizeof(thr_name), "[%s %ld]", _name.get(), id);
    ink_set_thread_name(thr_name);
    
    tgEThread = t;
    epEThread = t;

    t->id = id; // index
    t->set_affinity_id(affid);
    t->set_specific();

    // parallel with waiting parent now 
    ink_sem_post(&stackWait); 
    // parent is released to create next thread
    
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

  // change to new memory before creating stack
  numa::assign_thread_memory_by_affinity(_affcfg, affid); 

  // NOTE: pauses until stackFxn is thread-unneeded
  return Thread::start(stackWait, stacksize, startFxn);
}


ink_thread 
EventProcessor::spawn_thread(Continuation *cont, const char *thr_name, size_t stacksize)
{
  EThread *&epEThread = all_dthreads[n_dthreads_nxt++];
  ink_semaphore stackWait;

  // Determine correct multiple of our page size
  auto page = (ats_hugepage_enabled() ? sizeof(MemoryPageHuge) : sizeof(MemoryPage) );

  // correctly define size of stack now
  stacksize = std::max(stacksize, static_cast<decltype(stacksize)>(MIN_STACKSIZE));
  stacksize = aligned_spacing( stacksize, page );

  auto launchFxnPassed = [this,&epEThread,&stackWait,thr_name,cont](EThread *t) -> int
  {
    ink_set_thread_name(thr_name);

    epEThread = t;

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

  // NOTE: pauses until stackFxn is thread-unneeded
  auto r = Thread::start(stackWait, stacksize, startFxn);

  // ready to be included as valid now
  n_dthreads = n_dthreads_nxt;

  return r;
}
