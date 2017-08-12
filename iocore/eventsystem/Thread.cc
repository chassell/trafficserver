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
#include "P_EventSystem.h" // include ahead of I_Thread
#include "I_Thread.h"

#include "ts/ink_string.h"
#include "ts/jemallctl.h"

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
  ink_thread_setspecific(Thread::thread_data_key, nullptr);
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

  jemallctl::set_thread_arena(0); // default init first

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

EVENT_HANDLER_RECORD(&Continuation::handleEvent, kCtorValues);

EventHdlrState::EventHdlrState()
   : _assignPoint( &kCtorValues )
{
  EventChainPtr_t dummy;
  auto *currChainP = ( Thread::thread_data_key && this_thread() ? &this_thread()->_currentCallChain : &dummy );
  auto &currChainPtr = *currChainP;
  auto chainRefs = currChainPtr.use_count();

  // not normal case? clean?
  if ( chainRefs == 1 && ! currChainPtr->back()._extCallerChainLen )
  {
    // use as init here
    _eventChainPtr = currChainPtr; // (increase refs)

    // take assign-callback and leave empty
    std::swap(_assignPoint,_eventChainPtr->front()._assignPoint); 
    Debug("conttrace","%p: init-created: %s %s %d",this,_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
    return;
  }

  _eventChainPtr = std::make_shared<EventChain_t>(); // new chain from this one
  _eventChainPtr->reserve(16); // add some room
  Debug("conttrace","%p: empty-created",this);

  // assignPoint starts with nullptr
}

EventCallContext::EventCallContext(EventHdlr_t assignPoint)
   : _assignPoint(assignPoint),
     _currentCallChain(this_thread()->_currentCallChain),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  auto chain = std::make_shared<EventChain_t>(); // new chain from this one
  chain->reserve(16); // add some room

  if ( ! assignPoint ) {
    assignPoint = &kCtorValues;
  }

  // swap temp chain to current
  chain->push_back( EventCalled{*this, chain} );
  Debug("conttrace","init-only-call: %s %s %d", assignPoint->_kLabel, assignPoint->_kFile, assignPoint->_kLine);
}

EventCallContext::EventCallContext(EventHdlrState &state, unsigned event)
   : _assignPoint(state),
     _state(&state),
     _currentCallChain(this_thread()->_currentCallChain),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  // swap hdlrstate's chain to current (if different)
  state.push_caller_record(*this, event);
}

EventCallContext::~EventCallContext()
{
  // use back-refs to return the actual caller that's now complete
  EventCalled::pop_caller_record(*this);
}

int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  EventCallContext _ctxt(*this,event);
  return (*_assignPoint->_kWrapFunc_Gen())(ptr,event,data);
}

int
EventHdlrState::operator()(Continuation *self,int event, void *data)
{
  EventCallContext _ctxt(*this,event);
  return (*_assignPoint->_kWrapHdlr_Gen())(self,event,data);
}

