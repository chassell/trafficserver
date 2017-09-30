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


void
EventMetricStatInit()
{
  // Get our statistics set up
  RecRawStatBlock *rsb = RecAllocateRawStatBlock(EThread::N_EVENT_STATS * EThread::N_EVENT_TIMESCALES);
  char name[256];

  for (int ts_idx = 0; ts_idx < EThread::N_EVENT_TIMESCALES; ++ts_idx) {
    for (int id = 0; id < EThread::N_EVENT_STATS; ++id) {
      snprintf(name, sizeof(name), "%s.%ds", EThread::STAT_NAME[id], EThread::SAMPLE_COUNT[ts_idx]);
      RecRegisterRawStat(rsb, RECT_PROCESS, name, RECD_INT, RECP_NON_PERSISTENT, id + (ts_idx * EThread::N_EVENT_STATS), NULL);
    }
  }

  // Name must be that of a stat, pick one at random since we do all of them in one pass/callback.
  RecRegisterRawStatSyncCb(name, EventMetricStatSync, rsb, 0);
}

int
EventMetricStatSync(const char *, RecDataT, RecData *, RecRawStatBlock *rsb, int)
{
  int id = 0;
  EThread::EventMetrics summary[EThread::N_EVENT_TIMESCALES];

  // scan the thread local values
  for (int i = 0; i < eventProcessor.n_ethreads; ++i) {
    eventProcessor.all_ethreads[i]->summarize_stats(summary);
  }

  ink_mutex_acquire(&(rsb->mutex));

  for (int ts_idx = 0; ts_idx < EThread::N_EVENT_TIMESCALES; ++ts_idx, id += EThread::N_EVENT_STATS) {
    EThread::EventMetrics *m = summary + ts_idx;
    // Discarding the atomic swaps for global writes, doesn't seem to actually do anything useful.
    rsb->global[id + EThread::STAT_LOOP_COUNT]->sum   = m->_count;
    rsb->global[id + EThread::STAT_LOOP_COUNT]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_COUNT);

    rsb->global[id + EThread::STAT_LOOP_WAIT]->sum   = m->_wait;
    rsb->global[id + EThread::STAT_LOOP_WAIT]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_WAIT);

    rsb->global[id + EThread::STAT_LOOP_TIME_MIN]->sum   = m->_loop_time._min;
    rsb->global[id + EThread::STAT_LOOP_TIME_MIN]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_TIME_MIN);
    rsb->global[id + EThread::STAT_LOOP_TIME_MAX]->sum   = m->_loop_time._max;
    rsb->global[id + EThread::STAT_LOOP_TIME_MAX]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_TIME_MAX);

    rsb->global[id + EThread::STAT_LOOP_EVENTS]->sum   = m->_events._total;
    rsb->global[id + EThread::STAT_LOOP_EVENTS]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_EVENTS);
    rsb->global[id + EThread::STAT_LOOP_EVENTS_MIN]->sum   = m->_events._min;
    rsb->global[id + EThread::STAT_LOOP_EVENTS_MIN]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_EVENTS_MIN);
    rsb->global[id + EThread::STAT_LOOP_EVENTS_MAX]->sum   = m->_events._max;
    rsb->global[id + EThread::STAT_LOOP_EVENTS_MAX]->count = 1;
    RecRawStatUpdateSum(rsb, id + EThread::STAT_LOOP_EVENTS_MAX);
  }

  ink_mutex_release(&(rsb->mutex));
  return REC_ERR_OKAY;
}

EventType
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize, const ThreadFxn_t &runFxn)
{
  ink_release_assert( 0 < n_threads && n_threads <= (MAX_EVENT_THREADS - n_ethreads) );
  ink_release_assert(ev_type < MAX_EVENT_TYPES);

  // Determine correct multiple of our page size
  auto page = (ats_hugepage_enabled() ? sizeof(MemoryPageHuge) : sizeof(MemoryPage) );

  // correctly define size of stack now
  stacksize = std::max(0UL+stacksize,0UL+MIN_STACKSIZE);
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
    thread_group[ev_type].start(grpBarrier, epEThread, stacksize, runFxn);

    // thread has allocated and initialized its EThread 
    // now only waiting on barrier
  }

  // all threads are ready when passed
  pthread_barrier_wait(&grpBarrier);
  pthread_barrier_destroy(&grpBarrier);

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

  auto launchFxnRef = [this,&epEThread,affid,&stackWait,&grpBarrier](EThread *t) -> int
  {
    // NOTE: "grab" unique location [atomic]
    EThread *&tgEThread = _thread[_count++];

    ptrdiff_t id = ( &tgEThread - &_thread[0] ); // record index of it

    char thr_name[MAX_THREAD_NAME_LENGTH+1];
    snprintf(thr_name, sizeof(thr_name), "[%s %ld]", _name.get(), id);
    ink_set_thread_name(thr_name);
    
    tgEThread = t;
    epEThread = t;

    t->id = id; // index
    t->set_affinity_id(affid);

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
      cb(t); // init calls
    }

    return id;
  };

  // change to new memory before creating stack (inherited by child)
  numa::assign_thread_memory_by_affinity(_affcfg, affid); 

  ink_thread r;

  // NOTE: both pause until launchFxnRef/stackWait are child-unneeded

  if ( runFxn ) {
    // custom callback
    r = Thread::start(stackWait, stacksize, [&launchFxnRef,&runFxn]() 
            { runFxn(launchFxnRef); });
  } else {

    // default callback
    r = Thread::start(stackWait, stacksize, [&launchFxnRef]() 
       {
             // parent thread is waiting ... so copy-pass lambda-params
             Thread::launch(new EThread{REGULAR,0}, launchFxnRef);

           // parent has continued --> passed params are untouchable
           this_thread()->execute(); // parent has continued on
       });
  }

  numa::reset_thread_memory_by_cpuset();

  return r;
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

  auto launchFxnRef = [this,&epEThread,&stackWait,thr_name,cont](EThread *t) -> int
  {
    ink_set_thread_name(thr_name);

    epEThread = t;

    // parallel with parent now 
    ink_sem_post(&stackWait);

    return 0;
  };

  auto startFxn = [&launchFxnRef,cont]()
  {
    auto contV = cont;
    // parent thread is waiting .. so copy lambda-params now
    Thread::launch(new EThread{DEDICATED, nullptr}, launchFxnRef);

    // parent has continued --> params are untouchable
    contV->handleEvent(EVENT_IMMEDIATE,nullptr); // use cont as entire call
  };

  // NOTE: pauses until launchFxnRef/stackWait are child-unneeded
  auto r = Thread::start(stackWait, stacksize, startFxn);

  // ready to be included as valid now
  n_dthreads = n_dthreads_nxt;

  return r;
}
