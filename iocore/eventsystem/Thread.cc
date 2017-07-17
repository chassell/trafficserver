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

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

ink_hrtime Thread::cur_time = ink_get_hrtime_internal();
inkcoreapi ink_thread_key Thread::thread_data_key;

namespace
{
static bool initialized = ([]() -> bool {
  // File scope initialization goes here.
  ink_thread_key_create(&Thread::thread_data_key, nullptr);
  return true;
})();
}

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

///////////////////////////////////////////////
// Unix & non-NT Interface impl              //
///////////////////////////////////////////////


ink_thread
Thread::start(ink_semaphore &stackWait, unsigned stacksize, const ThreadFunction &hookFxn)
{
  auto threadHook = [](void *ptr) -> void*
    { static_cast<ThreadFunction*>(ptr)->operator()(); return nullptr; };

  // Make sure it is a multiple of our page size
  auto page = (ats_hugepage_enabled() ? sizeof(MemoryPageHuge) : sizeof(MemoryPage) );
  stacksize = aligned_spacing( stacksize, page );

  auto stack = ( ats_hugepage_enabled() ? new MemPageHuge[stacksize/sizeof(MemPageHuge)] : new MemPage[stacksize/sizeof(MemPage)] );

  ink_sem_init(&stackWait,0);

  auto tid = ink_thread_create(threadHook, const_cast<ThreadFunction*>(&hookFxn), false, stacksize, stack);

  // wait on child init 
  ink_sem_wait(&stackWait);
  ink_sem_destroy(&stackWait);

  return tid;
}
