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
#include "ts/CallbackDebug.h"

#include "ts/ink_string.h"
#include "ts/jemallctl.h"
#include "ts/ink_stack_trace.h"

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

CALL_FRAME_RECORD(&EventHdlrState::EventHdlrState, kCtorRecord);

namespace {
EventCalled::ChainPtr_t dummyCallChainPtr;
}


EventCalled::ChainPtr_t &get_current_call_chain_ref() 
{ 
  if ( ! Thread::thread_data_key || ! this_thread() ) {
    Debug("conttrace","%p: thread-specific is unset");
    return dummyCallChainPtr;
  }

  return this_thread()->_currentCallChain;
}


EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint( hdlr ?: &kCtorRecord )
{
  EventCalled::ChainPtr_t dummy;
  auto *currChainP = &get_current_call_chain_ref();
  auto &currChainPtr = *currChainP;
  auto chainRefs = currChainPtr.use_count();

  if ( ! currChainPtr && ! hdlr ) {
    ink_stack_trace_dump();
  }

  // not normal case? clean?
  if ( chainRefs == 1 && ! currChainPtr->back()._extCallerChainLen )
  {
    // use as init here
    _eventChainPtr = currChainPtr; // (increase refs)

    // take assign-callback and leave empty
    _assignPoint = _eventChainPtr->front()._assignPoint; 

    Debug("conttrace","%p: init-created: %s %s %d",this,_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
    return;
  }

  _eventChainPtr = std::make_shared<EventCalled::Chain_t>(); // new chain from this one
  _eventChainPtr->reserve(16); // add some room

  // we got an explicit assignPoint .. but no context?
  if ( ! currChainPtr && hdlr ) {
    // declare that our new top-context..
    currChainPtr = _eventChainPtr;
  } 
  else {
//    Debug("conttrace","%p: empty-created",this);
  }
}

/////////////////////////////////////////////////////
// create startup chain for constructors [if any]
/////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlr_t assignPoint)
   : _assignPoint(assignPoint),
     _currentCallChain(get_current_call_chain_ref()),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  auto chain = std::make_shared<EventCalled::Chain_t>(); // new chain from this one
  chain->reserve(16); // add some room

  // swap temp chain to current
  chain->push_back( EventCalled{*this, chain} );
  Debug("conttrace","init-only-call: %s %s %d", assignPoint->_kLabel, assignPoint->_kFile, assignPoint->_kLine);
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlrState &state, int event)
   : _assignPoint(state), // remember state's assignPoint
     _state(&state),
     _currentCallChain(get_current_call_chain_ref()),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  // swap hdlrstate's chain to current (if different)
  state.push_caller_record(*this, event);
}

int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  EventCallContext _ctxt{*this,event};
  return (*_assignPoint->_kWrapFunc_Gen())(ptr,event,data);
}

int
EventHdlrState::operator()(Continuation *self,int event, void *data)
{
  EventCallContext _ctxt{*this,event};
  return (*_assignPoint->_kWrapHdlr_Gen())(self,event,data);
}

size_t cb_sizeof_stack_context() { return sizeof(EventCallContext); }

TSContDebug *cb_init_stack_context(void *p, EventCHdlrAssignRecPtr_t recp)
{
  auto r = new(p) EventCallContext(reinterpret_cast<EventHdlr_t>(recp));
  return reinterpret_cast<TSContDebug*>(r);
}
