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
   : mutex( new_ProxyMutex() ), tid_(), serid_(++g_serid)
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

void Thread::spawn_init()
{
  set_specific();
}

///////////////////////////////////////////////
// Unix & non-NT Interface impl              //
///////////////////////////////////////////////

ink_thread
Thread::start(const char *name, void *stack, size_t stacksize, ThreadFunction const &thr)
{
  // wait until spawn_init completes
  ink_cond initDone;
  ink_mutex initMutex;

  ink_cond_init(&initDone);
  ink_mutex_init(&initMutex);

  ink_scoped_mutex_lock lock(&initMutex); // parent holds the mutex ...

  stacksize = ( stacksize ? stacksize : DEFAULT_STACKSIZE );

  bool ready = false;

  // set unchanged value of condition variable
  tid_ = ink_thread_self(); 

  // stack-lambda captures 
  //    (3) Thread-obj, (1) start-caller name ptr, (2) start fxn, 
  auto doSpawnInit = [this,name,&thr,&initDone,&initMutex]() -> ThreadFunction
    {
        { 
          char buff[MAX_THREAD_NAME_LENGTH]; ///< Name for the thread.
          snprintf(buff,sizeof(buff),name,tid_); // include the tid_ as passed...
          ink_set_thread_name(buff); // use captured buff-array
        }

        // do thread-pre-init ops
        this->spawn_init(); // handle spawn-time setup (set_specific)
        
        ThreadFunction fxn{ thr ? thr : std::bind(&Thread::execute,this) };

        { // cond_wait will release mutex
          ink_scoped_mutex_lock lock(&initMutex);

          // alter cond variable with lock
          this->tid_ = pthread_self();

          // notify (single) cond-blocked parent
          ink_cond_broadcast(&initDone);
        } // mutex-release allows parent to continue

        // NOTE: all (other than this) ptr-captures are invalid now...

        return std::move(fxn);
    };

  // no captured values...
  auto threadLifeFxn = [](void *ptr) -> void*
    {
      // WARNING: ptr is from waiting parent's stack..
      ThreadFunction threadFxn;

      {
         using SpawnInitFunctor_t = decltype(doSpawnInit);
         SpawnInitFunctor_t &init = *static_cast<SpawnInitFunctor_t*>(ptr);

         // perform spawn_init() then release parent with cond_signal
         threadFxn = init();
      }

      // call execution functor 
      threadFxn();
      return nullptr;
    };

  ink_thread_create(threadLifeFxn, &doSpawnInit, false, stacksize, stack);

  // wait until mutex returned with changed tid_ (ignore spurious wakeups)
  do 
    { ink_cond_wait(&initDone,&initMutex); } 
  while ( tid_ == ink_thread_self() );

  // parent holds mutex and child has completed spawn_init()

  ink_mutex_destroy(&initMutex);
  ink_cond_destroy(&initDone);

  return tid_;
}
