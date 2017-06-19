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

/****************************************************************************

  Basic Threads



**************************************************************************/
#include "P_EventSystem.h"
#include "ts/ink_string.h"
#include "ts/hugepages.h"

#include <memory>
///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

static ink_thread_key init_thread_key();

ink_hrtime Thread::cur_time                       = ink_get_hrtime_internal();
inkcoreapi ink_thread_key Thread::thread_data_key = init_thread_key();

Thread::Thread()
{
  mutex = new_ProxyMutex();
  MUTEX_TAKE_LOCK(mutex, (EThread *)this);
  mutex->nthread_holding += THREAD_MUTEX_THREAD_HOLDING;
}

Thread::~Thread()
{
  ink_release_assert(mutex->thread_holding == (EThread *)this);
  mutex->nthread_holding -= THREAD_MUTEX_THREAD_HOLDING;
  MUTEX_UNTAKE_LOCK(mutex, (EThread *)this);
}

ink_thread_key
init_thread_key()
{
  ink_thread_key_create(&Thread::thread_data_key, nullptr);
  return Thread::thread_data_key;
}

static void *
alloc_stack(size_t stacksize)
{
  void *stack = nullptr;

  if (ats_hugepage_enabled()) {
    stack = ats_alloc_hugepage(stacksize);
  }

  if (stack == nullptr) {
    stack = ats_memalign(ats_pagesize(), stacksize);
  }

  return stack;
}

using ThreadFxnPtr_t = std::unique_ptr<ThreadFunction>;

// generic lambda to hold/call the argument/lambda
static const auto applyArg = [](void*arg) -> void*
  { 
    ThreadFxnPtr_t{ static_cast<ThreadFunction*>(arg) }->operator()(); // grab, use and delete [exception-safe]
    return nullptr;
  };

// start a non affinity-sensitive thread
ink_thread Thread::start(const char *name, size_t stacksize, int i, ThreadFunction const &f)
{
  char thrName[MAX_THREAD_NAME_LENGTH];
  snprintf(thrName, sizeof(thrName), name, i);

  stacksize = ( stacksize ? : DEFAULT_STACKSIZE );

  // use vlaues after thread starts
  auto inThread = ThreadFxnPtr_t( new ThreadFunction{[=]()
    {
      ink_set_thread_name(thrName); // use fixed copied array
      this->set_specific();
      ( f ? f() : this->execute() );
    }});

  return ink_thread_create(applyArg, inThread.release(), false, stacksize, alloc_stack(stacksize));
}

#if TS_USE_HWLOC
static const auto _cpus_ = hwloc_topology_get_topology_cpuset( ink_get_topology() );
static const auto _cpusCnt_ = hwloc_bitmap_weight( _cpus_ );
static const auto _cpusAllowed_ = hwloc_topology_get_allowed_cpuset( ink_get_topology() );

static const auto _numas_ = hwloc_topology_get_topology_nodeset( ink_get_topology() );
static const auto _numasCnt_ = hwloc_bitmap_weight( _numas_ );

static const hwloc_obj_type_t affinity_objs[] = 
   { HWLOC_OBJ_MACHINE, HWLOC_OBJ_NUMANODE, HWLOC_OBJ_SOCKET, HWLOC_OBJ_CORE
#if HAVE_HWLOC_OBJ_PU
         , HWLOC_OBJ_PU  
#endif
  };

static const char *const affinity_obj_names[] =
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
  char cpu_mask[_cpusCnt_+1];
  hwloc_bitmap_snprintf(cpu_mask, sizeof(cpu_mask), cpuset);
  Debug("iocore_thread", "EThread: %s -> %s# %d CPU Mask: %s", thrname, objname, lind, cpu_mask);
#else
  Debug("iocore_thread", "EThread: %s -> %s# %d", thrname, objname, lind);
#endif // HWLOC_API_VERSION
}

// start an affinity-sensitive thread
ink_thread Thread::start(const char *name, size_t stacksize, int i, hwloc_obj_type_t affobj)
{
  char thrName[MAX_THREAD_NAME_LENGTH];
  snprintf(thrName, sizeof(thrName), name, i);

  stacksize = ( stacksize ? : DEFAULT_STACKSIZE );

  void *stack = nullptr;

  int affobjs = hwloc_get_nbobjs_by_type(ink_get_topology(), affobj);
  hwloc_obj_t obj = hwloc_get_obj_by_type(ink_get_topology(), affobj, ( affobjs ? i % affobjs : 0 ) );

  // assign correct set of CPUs
  hwloc_const_cpuset_t thrCPUs = ( obj ? obj->cpuset : _cpusAllowed_ );

  if ( obj ) {
    // show the subset being used
    pretty_print_cpuset(thrName, affobj, obj->logical_index, obj->cpuset);
  }

  // detect this thread's set of NUMA nodes
  hwloc_nodeset_t thrNUMAs = hwloc_bitmap_alloc();

  {
    hwloc_nodeset_t origNUMAs = hwloc_bitmap_alloc();
    hwloc_membind_policy_t origPolicy = HWLOC_MEMBIND_DEFAULT;

    hwloc_get_membind_nodeset(ink_get_topology(), origNUMAs, &origPolicy, HWLOC_MEMBIND_THREAD);
    hwloc_cpuset_to_nodeset(ink_get_topology(), thrCPUs, thrNUMAs);

    // temporarily match NUMA nodes for cpuset
    hwloc_set_membind_nodeset(ink_get_topology(), thrNUMAs, HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);

    stack = alloc_stack(stacksize);

    // reset back
    hwloc_set_membind_nodeset(ink_get_topology(), origNUMAs, origPolicy, HWLOC_MEMBIND_THREAD);
    hwloc_bitmap_free(origNUMAs);                                                                                                                      
  }

  // use vlaues after thread starts
  auto inThread = ThreadFxnPtr_t( new ThreadFunction{[=]()
    {
      ink_set_thread_name(thrName); // use fixed copied array
      hwloc_set_cpubind(ink_get_topology(), thrCPUs, HWLOC_CPUBIND_STRICT);
      hwloc_set_membind_nodeset(ink_get_topology(), thrNUMAs, HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);

      hwloc_bitmap_free(thrNUMAs); // finally done

      this->set_specific();
      this->execute();
    }});

  return ink_thread_create(applyArg, inThread.release(), false, stacksize, stack);
}
#endif


