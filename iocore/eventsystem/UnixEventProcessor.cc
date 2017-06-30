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
  auto n = std::find( std::begin(affinity_objs), std::end(affinity_objs), objtype ) - std::begin(affinity_objs);
  n %= countof(affinity_objs); // remap end to index zero
  const char *objname = affinity_obj_names[n];

#if HWLOC_API_VERSION >= 0x00010100
  char cpu_mask[_kCpusCnt_+1];
  hwloc_bitmap_snprintf(cpu_mask, sizeof(cpu_mask), kCpuset);
  Debug("iocore_thread", "EThread: %s -> %s# %d CPU Mask: %s", thrname, objname, lind, cpu_mask);
#else
  Debug("iocore_thread", "EThread: %s -> %s# %d", thrname, objname, lind);
#endif // HWLOC_API_VERSION
}

#endif

EventProcessor::EventProcessor() : thread_initializer(this)
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

namespace
{
Event *
make_event_for_scheduling(Continuation *c, int event_code, void *cookie)
{
  Event *e = eventAllocator.alloc();

  e->init(c);
  e->mutex          = c->mutex;
  e->callback_event = event_code;
  e->cookie         = cookie;

  return e;
}
}

Event *
EventProcessor::schedule_spawn(Continuation *c, EventType ev_type, int event_code, void *cookie)
{
  Event *e = make_event_for_scheduling(c, event_code, cookie);
  ink_assert(ev_type < MAX_EVENT_TYPES);
  thread_group[ev_type]._spawnQueue.enqueue(e);
  XXX
  return e;
}

Event *
EventProcessor::schedule_spawn(void (*f)(EThread *), EventType ev_type)
{
  Event *e = make_event_for_scheduling(&Thread_Init_Func, EVENT_IMMEDIATE, reinterpret_cast<void *>(f));
  ink_assert(ev_type < MAX_EVENT_TYPES);
  thread_group[ev_type]._spawnQueue.enqueue(e);
  return e;
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
EventProcessor::spawn_event_threads(EventType ev_type, int n_threads, size_t stacksize)
{
  char thr_name[MAX_THREAD_NAME_LENGTH];
  int i;
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

  for (i = 0; i < n_threads; ++i) {
    EThread *t                   = EThread::create_thread(&thread_initializer,i,ev_type);
    all_ethreads[n_ethreads + i] = t;
    tg->_thread[i]               = t;
  }
  tg->_count = n_threads;
  n_ethreads += n_threads;

  // Separate loop to avoid race conditions between spawn events and updating the thread table for
  // the group. Some thread set up depends on knowing the total number of threads but that can't be
  // safely updated until all the EThread instances are created and stored in the table.
  for (i = 0; i < n_threads; ++i) {
    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[%s %d]", tg->_name.get(), i);
    void *stack = Thread_Affinity_Initializer.alloc_stack(tg->_thread[i], stacksize);
    tg->_thread[i]->start(thr_name, stack, stacksize);
  }

  Debug("iocore_thread", "Created thread group '%s' id %d with %d threads", tg->_name.get(), ev_type, n_threads);

  return ev_type; // useless but not sure what would be better.
}

// This is called from inside a thread as the @a start_event for that thread.  It chains to the
// startup events for the appropriate thread group start events.
void
EventProcessor::initThreadState(EThread *t)
{
  // Run all thread type initialization continuations that match the event types for this thread.
  for (int i = 0; i < MAX_EVENT_TYPES; ++i) 
  {
    if (! t->is_event_type(i)) {
       continue; that event type done here, roll thread start events of that type.
    }

    // To avoid race conditions on the event in the spawn queue, create a local one to actually send.
    // Use the spawn queue event as a read only model.
    for (Event *ev = thread_group[i]._spawnQueue.head; NULL != ev; ev = ev->link.next) 
    {
      Event nev;
      nev.init(ev->continuation, 0, 0);
      nev.set_thread(t);
      nev.callback_event = ev->callback_event;
      nev.cookie         = ev->cookie;
      ev->continuation->handleEvent(ev->callback_event, &nev);
    }
  }

}

static inline get_dflt_stacksize(size_t stacksize) 
{
  // Make sure it is a multiple of our page size
  auto page = (ats_hugepage_enabled() ? ats_hugepage_size() : ats_pagesize() );
  return aligned_spacing( stacksize, page );
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
  // thread_group[ET_CALL]._spawnQueue.push(make_event_for_scheduling(&Thread_Affinity_Initializer, EVENT_IMMEDIATE, nullptr));

  // this->spawn_event_threads(ET_CALL, n_threads, stacksize);

  ink_release_assert(n_thread_groups < MAX_EVENT_TYPES);

  ThreadGroupDescriptor *const tg = &(thread_group[n_thread_groups]);

  tg->_name = ats_strdup(name);

  Debug("iocore_thread", "Thread stack size set to %zu", get_dflt_stacksize( std::max(stacksize+0,INK_THREAD_STACK_MIN )) );

  // store pointers in advance
  Ethread *threads[n_threads] = { 0 };

  // detect this thread's set of NUMA nodes
  hwloc_nodeset_t thrNUMAs = hwloc_bitmap_alloc();
  hwloc_nodeset_t origNUMAs = hwloc_bitmap_alloc();
  hwloc_membind_policy_t origPolicy = HWLOC_MEMBIND_DEFAULT;

  // preserve the spawning thread's settings
  hwloc_get_membind_nodeset(ink_get_topology(), origNUMAs, &origPolicy, HWLOC_MEMBIND_THREAD);

  int affinity = 1;
  REC_ReadConfigInteger(affinity, "proxy.config.exec_thread.affinity");

  // create all affinity-groups to round-robin over..
  auto cpusets = get_cpu_sets(affinity);

  for( int n = n_threads ; 
  {
    hwloc_const_cpuset_t 
    hwloc_cpuset_to_nodeset(ink_get_topology(), thrCPUs, thrNUMAs);

    // temporarily match NUMA nodes for cpuset
    hwloc_set_membind_nodeset(ink_get_topology(), thrNUMAs, HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);

    stack = alloc_stack(stacksize);

    // reset back
    hwloc_set_membind_nodeset(ink_get_topology(), origNUMAs, origPolicy, HWLOC_MEMBIND_THREAD);
    hwloc_bitmap_free(origNUMAs);                                                                                                                      
  }

  for (int i = 0; i < n_threads; ++i) 
  {
    


    EThread *t                   = new EThread(REGULAR, n_ethreads + i);
    t->set_event_type(ev_type);
    t->id                        = i; // unfortunately needed to support affinity and NUMA logic.
    t->tid_                      = i; // unfortunately needed to support affinity and NUMA logic.
  }

    all_ethreads[n_ethreads + i] = t;
    tg->_thread[i]               = t;

    t->schedule_spawn(&thread_initializer);
    char thr_name[MAX_THREAD_NAME_LENGTH];
    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[%s %d]", tg->_name.get(), i);
  }
  tg->_count = n_threads;
  n_ethreads += n_threads;

  // Separate loop to avoid race conditions between spawn events and updating the thread table for
  // the group. Some thread set up depends on knowing the total number of threads but that can't be
  // safely updated until all the EThread instances are created and stored in the table.
  for (i = 0; i < n_threads; ++i) {
    void *stack = Thread_Affinity_Initializer.alloc_stack(tg->_thread[i], stacksize);
    tg->_thread[i]->start(ET_CALL, stack, stacksize);
  }

  Debug("iocore_thread", "Created thread group '%s' id %d with %d threads", tg->_name.get(), ev_type, n_threads);

  return ev_type; // useless but not sure what would be better.

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

  auto thr = DedicatedEThread::create_thread();
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
