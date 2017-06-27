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
#include "ts/ink_memory.h"

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
   : mutex( new_ProxyMutex() )
{
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
Thread::start(const char *name, void *stack, size_t stacksize, ThreadFunction const &f)
{
  char buff[MAX_THREAD_NAME_LENGTH]; ///< Name for the thread.
  snprintf(buff,sizeof(buff),name,tid_); // use the tid_ as is right now..

  // get full copy
  ThreadFunction fxn = f;
  // hold a mutex to delay child for a moment
  ink_mutex mutex;

  // capture params needed to start
  auto getStartFxnObj = ats_copy_to_unique_ptr([buff,this,fxn,&mutex]() -> ThreadFunction
     {
        ink_set_thread_name(buff); // use copied array
        this->set_specific();

        {
          ink_scoped_mutex_lock lock(&mutex); // wait until parent has written tid_
          ink_mutex_destroy(&mutex); // done with it...
        }

        return ( fxn ? fxn : std::bind(&Thread::execute,this) ); // call thread start
    } );

  // no captured values...
  auto pureCaller = [](void *ptr) -> void*
    {
      using GetStartFxnObj_ptr = decltype(getStartFxnObj.release());
      // create temp unique_ptr, generate final functor, free temp
      auto startFxn = ats_make_unique(static_cast<GetStartFxnObj_ptr>(ptr))->operator()();
      startFxn();
      return nullptr;
    };

  ink_mutex_init(&mutex);
  ink_scoped_mutex_lock lock(&mutex);

  // delay child until tid_ is written and returned

  tid_ = ink_thread_create(pureCaller, getStartFxnObj.release(), 0, 
                  ( stacksize ? stacksize : DEFAULT_STACKSIZE ), stack);
  return tid_;
}
