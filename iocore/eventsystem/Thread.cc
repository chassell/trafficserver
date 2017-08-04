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
#include "ts/jemallctl.h"

#include "P_EventSystem.h"
#include "I_Thread.h"
#include "ts/ink_string.h"

#include <chrono>

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

static ink_thread_key init_thread_key();

ProxyMutex *global_mutex                          = NULL;
ink_hrtime Thread::cur_time                       = 0;
inkcoreapi ink_thread_key Thread::thread_data_key = init_thread_key();

Thread::Thread()
{
  mutex     = new_ProxyMutex();
  mutex_ptr = mutex;
  MUTEX_TAKE_LOCK(mutex, (EThread *)this);
  mutex->nthread_holding = THREAD_MUTEX_THREAD_HOLDING;
}

static void
key_destructor(void *value)
{
  (void)value;
}

ink_thread_key
init_thread_key()
{
  ink_thread_key_create(&Thread::thread_data_key, key_destructor);
  return Thread::thread_data_key;
}

///////////////////////////////////////////////
// Unix & non-NT Interface impl              //
///////////////////////////////////////////////

struct thread_data_internal {
  ThreadFunction f;
  void *a;
  Thread *me;
  char name[MAX_THREAD_NAME_LENGTH];
};

static void *
spawn_thread_internal(void *a)
{
  thread_data_internal *p = (thread_data_internal *)a;

  p->me->set_specific();
  ink_set_thread_name(p->name);
  if (p->f)
    p->f(p->a);
  else
    p->me->execute();
  ats_free(a);
  return NULL;
}

ink_thread
Thread::start(const char *name, size_t stacksize, ThreadFunction f, void *a)
{
  thread_data_internal *p = (thread_data_internal *)ats_malloc(sizeof(thread_data_internal));

  p->f  = f;
  p->a  = a;
  p->me = this;
  memset(p->name, 0, MAX_THREAD_NAME_LENGTH);
  ink_strlcpy(p->name, name, MAX_THREAD_NAME_LENGTH);
  tid = ink_thread_create(spawn_thread_internal, (void *)p, 0, stacksize);

  return tid;
}

uint64_t &Thread::alloc_bytes_count_direct() 
{ 
  return *jemallctl::thread_allocatedp(); 
}

uint64_t &Thread::dealloc_bytes_count_direct() 
{ 
  return *jemallctl::thread_deallocatedp(); 
}

unsigned Continuation::HdlrAssignSet::s_hdlrAssignSetCnt = 0;

int
Continuation::handleEvent(int event, void *data)
{
  ink_release_assert( _handler );

  auto logPtr = ( _handlerLogPtr ? : std::make_shared<EventHdlrLog>() );
  auto &log = *logPtr;
  auto &cbrec = *_handlerRec;

  auto called = std::chrono::steady_clock::now();
  auto alloced = Thread::alloc_bytes_count(); 
  auto dealloced = Thread::dealloc_bytes_count();

  auto r = (*_handler)(this,event,data);

  auto duration = std::chrono::steady_clock::now() - called;
  auto span = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  uint16_t logv = 8*sizeof(unsigned int) - __builtin_clz(static_cast<unsigned int>(span));

  log.emplace_back( 
     EventHdlrLogRec{ this_thread()->alloc_bytes_count() - alloced,
                      this_thread()->dealloc_bytes_count() - dealloced,
                      cbrec.get_id(),
                      logv } 
     );

  // deleted continuation?
  if ( logPtr.use_count() < 2 ) {
    // done and time to print it...
  }
  return r;

}

