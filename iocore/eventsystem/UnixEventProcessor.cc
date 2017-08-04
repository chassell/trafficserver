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

namespace {
#if TS_USE_HWLOC
const hwloc_obj_type_t kAffinity_objs[] = {
  HWLOC_OBJ_MACHINE, HWLOC_OBJ_NUMANODE, HWLOC_OBJ_SOCKET, HWLOC_OBJ_CORE
#if HAVE_HWLOC_OBJ_PU
  ,
  HWLOC_OBJ_PU // include only if valid in HWLOC version
#endif
};
#endif

const char *const kAffinity_obj_names[] = {
  "[Unrestricted]", "NUMA Node", "Socket", "Core", "Logical CPU" // [ignore if not found]
};
}

EventType
EventProcessor::spawn_event_threads(int n_threads, const char *et_name, size_t stacksize)
{
  char thr_name[MAX_THREAD_NAME_LENGTH];
  EventType new_thread_group_id;
  int i;

  ink_release_assert(n_threads > 0);
  ink_release_assert((n_ethreads + n_threads) <= MAX_EVENT_THREADS);
  ink_release_assert(n_thread_groups < MAX_EVENT_TYPES);

  new_thread_group_id = (EventType)n_thread_groups;

  int affinity = 1;
  REC_ReadConfigInteger(affinity, "proxy.config.exec_thread.affinity");

  for (i = 0; i < n_threads; i++) {
    numa::assign_thread_memory_by_affinity(kAffinity_objs[affinity], i);
    EThread *t                          = new EThread(REGULAR, n_ethreads + i);
    all_ethreads[n_ethreads + i]        = t;
    eventthread[new_thread_group_id][i] = t;
    t->set_event_type(new_thread_group_id);
  }

  n_threads_for_type[new_thread_group_id] = n_threads;
  for (i = 0; i < n_threads; i++) {
    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[%s %d]", et_name, i);
    eventthread[new_thread_group_id][i]->start(thr_name, stacksize);
  }

  numa::reset_thread_memory_by_cpuset();

  n_thread_groups++;
  n_ethreads += n_threads;
  Debug("iocore_thread", "Created thread group '%s' id %d with %d threads", et_name, new_thread_group_id, n_threads);

  return new_thread_group_id;
}

class EventProcessor eventProcessor;

int
EventProcessor::start(int n_event_threads, size_t stacksize)
{
  char thr_name[MAX_THREAD_NAME_LENGTH];
  int i;

  // do some sanity checking.
  static int started = 0;
  ink_release_assert(!started);
  ink_release_assert(n_event_threads > 0 && n_event_threads <= MAX_EVENT_THREADS);
  started = 1;

  n_ethreads      = n_event_threads;
  n_thread_groups = 1;

  {
    auto t = this_thread();
    ink_thread_setspecific(Thread::thread_data_key, nullptr);
    delete t;
  }

  for (i = 0; i < n_event_threads; i++) {
    EThread *t = new EThread(REGULAR, i);
    if (i == 0) {
      t->set_specific();
      global_mutex = t->mutex;
      Thread::get_hrtime_updated();
    }
    all_ethreads[i] = t;

    eventthread[ET_CALL][i] = t;
    t->set_event_type((EventType)ET_CALL);
  }
  n_threads_for_type[ET_CALL] = n_event_threads;

#if TS_USE_HWLOC
  int affinity = 1;
  REC_ReadConfigInteger(affinity, "proxy.config.exec_thread.affinity");
  hwloc_obj_type_t obj_type = kAffinity_objs[affinity];
  int obj_count             = 0;
  const char *obj_name      = kAffinity_obj_names[affinity];
  obj_count                 = hwloc_get_nbobjs_by_type(ink_get_topology(), obj_type);
  Debug("iocore_thread", "Affinity: %d %ss: %d PU: %d", affinity, obj_name, obj_count, ink_number_of_processors());

#endif

  for (i = 0; i < n_ethreads; i++) {
    numa::assign_thread_memory_by_affinity(obj_type, i);
    if (i > 0) {
      snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[ET_NET %d]", i);
      all_ethreads[i]->start(thr_name, stacksize);
    } else {
      ink_thread_self();
    }
#if TS_USE_HWLOC

    // Get our `obj` instance with index based on the thread number we are on.
    numa::hwloc_bitmap cpuset;
    hwloc_get_cpubind(ink_get_topology(), cpuset, HWLOC_CPUBIND_THREAD);
#if HWLOC_API_VERSION >= 0x00010100
    int cpu_mask_len = hwloc_bitmap_snprintf(NULL, 0, cpuset) + 1;
    char *cpu_mask   = (char *)alloca(cpu_mask_len);
    hwloc_bitmap_snprintf(cpu_mask, cpu_mask_len, cpuset);
    Debug("iocore_thread", "EThread: %d %s: %d CPU Mask: %s\n", i, obj_name, -1, cpu_mask);
#else
    Debug("iocore_thread", "EThread: %d %s: %d\n", i, obj_name, -1);
#endif // HWLOC_API_VERSION
#else
#endif // TS_USE_HWLOC
  }

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
  ink_release_assert(n_dthreads < MAX_EVENT_THREADS);
  Event *e = eventAllocator.alloc();

  e->init(cont, 0, 0);
  all_dthreads[n_dthreads] = new EThread(DEDICATED, e);
  e->ethread               = all_dthreads[n_dthreads];
  e->mutex = e->continuation->mutex = all_dthreads[n_dthreads]->mutex;
  n_dthreads++;
  e->ethread->start(thr_name, stacksize);

  return e;
}
