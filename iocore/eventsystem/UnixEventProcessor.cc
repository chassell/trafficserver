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
void pretty_print_cpuset(const char *thrname, hwloc_obj_type_t objtype, int lind, hwloc_const_cpuset_t cpuset)
{
  auto n = std::find( std::begin(kAffinity_objs), std::end(kAffinity_objs), objtype ) - std::begin(kAffinity_objs);
  n %= countof(kAffinity_objs); // remap end to index zero
  const char *objname = kAffinity_obj_names[n];

#if HWLOC_API_VERSION >= 0x00010100
  int cpu_mask_len = hwloc_bitmap_snprintf(NULL, 0, cpuset);
  char cpu_mask[cpu_mask_len+1];
  hwloc_bitmap_snprintf(cpu_mask, sizeof(cpu_mask), cpuset);
  Debug("iocore_thread", "EThread: %s -> %s# %d CPU Mask: %s", thrname, objname, lind, cpu_mask);
#else
  Debug("iocore_thread", "EThread: %s -> %s# %d", thrname, objname, lind);
#endif // HWLOC_API_VERSION
}

#endif

EventProcessor::EventProcessor() // : thread_initializer(this)
{
  ink_zero(all_ethreads);
  ink_zero(all_dthreads);
//  ink_zero(thread_group);
  ink_mutex_init(&dedicated_thread_spawn_mutex);
  // Because ET_NET is compile time set to 0 it *must* be the first type registered.
  this->register_event_type("ET_NET");
}

EventProcessor::~EventProcessor()
{
  ink_mutex_destroy(&dedicated_thread_spawn_mutex);
}

// eventProcessor.schedule_spawn(&initialize_thread_for_net, ET_NET);'
// eventProcessor.schedule_spawn(&initialize_thread_for_net, ET_DNS);'
// eventProcessor.schedule_spawn(&initialize_thread_for_udp_net, ET_UDP);'
// eventProcessor.schedule_spawn([](EThread *thread){ thread->server_session_pool = new ServerSessionPool; }, ET_NET);'
void
EventProcessor::schedule_spawn(void (*f)(EThread *), EventType ev_type)
{
  ink_assert(ev_type < MAX_EVENT_TYPES);
  thread_group[ev_type]._spawnQueue.push_back(f);
}

EventType
EventProcessor::register_event_type(char const *name)
{
  ThreadGroupDescriptor *tg = &(thread_group[n_thread_groups++]);
  ink_release_assert(n_thread_groups <= MAX_EVENT_TYPES); // check for overflow

  tg->_name = ats_strdup(name);
  return n_thread_groups - 1;
}

static inline size_t get_dflt_stacksize(size_t stacksize) 
{
  // Make sure it is a multiple of our page size
  auto page = (ats_hugepage_enabled() ? ats_hugepage_size() : ats_pagesize() );
  return aligned_spacing( stacksize, page );
}

EventType
EventProcessor::spawn_event_threads(char const *name, int n_threads, size_t stacksize)
{
  int ev_type = this->register_event_type(name);
  this->spawn_event_threads(ev_type, n_threads, stacksize);
  return ev_type;
}

// eventProcessor.spawn_event_threads(ET_NET, n_threads, stacksize);
// eventProcessor.spawn_event_threads(ET_DNS, 1, stacksize);
// eventProcessor.spawn_event_threads("ET_TASK", std::max(1, task_threads), stacksize);
// eventProcessor.spawn_event_threads("ET_OCSP", 1, stacksize);
// eventProcessor.spawn_event_threads(ET_UDP, n_upd_threads, stacksize);
// eventProcessor.spawn_event_threads("ET_REMAP", num_threads, stacksize);
EventType
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize)
{
  ink_release_assert(this->n_thread_groups < MAX_EVENT_TYPES);
  
  ev_type = n_thread_groups++;
  ThreadGroupDescriptor *const tg = &(thread_group[ev_type]);

  ink_release_assert(n_threads > 0);
  ink_release_assert((n_ethreads + n_threads) <= MAX_EVENT_THREADS);
  ink_release_assert(ev_type < MAX_EVENT_TYPES);

  Debug("iocore_thread", "Thread stack size set to %zu", get_dflt_stacksize( std::max(stacksize,size_t()+INK_THREAD_STACK_MIN)) );

  for (int i = 0; i < n_threads; ++i) 
  {
    EThread *t                   = new EThread;
    // EThread::EThread(ThreadType att, int anid) : id(anid), tt(att)
    t->id                        = i; // unfortunately needed to support affinity and NUMA logic.
    t->set_event_type(ev_type); // see-below
    tg->_thread[i]               = t;

    all_ethreads[n_ethreads + i] = t;
  }

  tg->_count = n_threads; // jump up
  n_ethreads += n_threads;

  // Separate loop to avoid race conditions between spawn events and updating the thread table for
  // the group. Some thread set up depends on knowing the total number of threads but that can't be
  // safely updated until all the EThread instances are created and stored in the table.
  for (int i = 0; i < n_threads; ++i) 
  {
    char thr_name[MAX_THREAD_NAME_LENGTH];
    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[%s %d]", tg->_name.get(), i);
    tg->_thread[i]->start(thr_name, ats_memalign(ats_pagesize(), stacksize), stacksize);
    /*
    // t->Thread::start(...)
// CLONE:
    // t->EThread::execute()
    // t->start_event->continuation->handleEvent(EVENT_IMMEDIATE, t->start_event);
          // this->thread_initializer.init()
    // this->EventProcessor::initThreadState(t)

       // Run all thread type initialization continuations that match the event types for this thread.
       // t->is_event_type(ev_type) == true // see-below
       // ev = tg->_spawnQueue.head

		 // queued by HttpSessionManager::init() .... ev_type == ET_NET
		   t->server_session_pool = new ServerSessionPool
	     // ev_type == ET_NET

	     // queued by UnixNetProcessor::init() ... ev_type == ET_DNS 
		 // queued by DNSProcessor::start() ... ev_type == ET_DNS 
	 	   ...
		   t->schedule_imm(get_NetHandler(thread));
		   t->schedule_every(inactivityCop, HRTIME_SECONDS(cop_freq));
		   t->ep->start(pd, thread->evfd, nullptr, EVENTIO_READ);
	     // ev_type == ET_DNS

		 // UDPNetProcessorInternal::start .... ev_type == ET_UDP
           ...
		   t->schedule_every(get_UDPPollCont(thread), -9);
		   t->schedule_imm(get_UDPNetHandler(thread));
		 // ev_type == ET_UDP
     */
  }

  return ev_type; // useless but not sure what would be better.
}

int
EventProcessor::start(int n_threads, size_t stacksize)
{
  // do some sanity checking.
  static bool started = false;
  ink_release_assert(!started);
  ink_release_assert(n_threads > 0 && n_threads <= MAX_EVENT_THREADS);
  ink_release_assert(n_threads > 0);
  ink_release_assert((this->n_ethreads + n_threads) <= MAX_EVENT_THREADS);

  started = true;
  // Thread_Affinity_Initializer.init();

  // prepare for callback for set_affinity() in queue which calls hwloc_set_thread_cpubind(... t->tid_, obj->cpuset, HWLOC_CPUBIND_STRICT);
  //
  // thread_group[ET_NET]._spawnQueue.push(make_event_for_scheduling(&Thread_Affinity_Initializer, EVENT_IMMEDIATE, nullptr));

  return 0;
}

void
EventProcessor::shutdown()
{
}

Event *
EventProcessor::spawn_thread(Continuation *cont, const char *thr_name, size_t stacksize)
{
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

  auto thr = new EThread(); // DedicatedEThread::create_thread();
  auto e = thr->start_event;
  {
    ink_scoped_mutex_lock lock(dedicated_thread_spawn_mutex);
    ink_release_assert(n_dthreads < MAX_EVENT_THREADS);
    all_dthreads[n_dthreads] = e->ethread;
    ++n_dthreads; // Be very sure this is after the array element update.
  }

  e->ethread->start(thr_name, nullptr, stacksize);

  return e;
}
