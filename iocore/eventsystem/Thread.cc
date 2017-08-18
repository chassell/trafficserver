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

namespace {

CALL_FRAME_RECORD(&EventHdlrState::EventHdlrState, kCtorRecord);

EventCalled::ChainPtr_t thread_chain()
{ 
  if ( ! Thread::thread_data_key || ! this_thread() ) {
    Debug("conttrace","thread-specific is unset");
    return EventCalled::ChainPtr_t();
  }

  return this_thread()->_currentCallChain;
}

EventCalled::ChainPtr_t &thread_chain_ref(EventCalled::ChainPtr_t &dummy) 
{ 
  if ( ! Thread::thread_data_key || ! this_thread() ) {
    Debug("conttrace","thread-specific is unset");
    return dummy;
  }

  return this_thread()->_currentCallChain;
}

EventCalled::ChainPtr_t new_ctor_chain(const EventCalled::ChainPtr_t &curr)
{
  auto chn = std::make_shared<EventCalled::Chain_t>(); // new chain from this one
  chn->reserve(16); // add some room
  
  auto assignPoint = ( curr ? curr->back()._assignPoint : &kCtorRecord );
  chn->push_back( EventCalled(*assignPoint,0) );
  return std::move(chn);
}


}

EventHdlrState::EventHdlrState(void *p)
{
  EventCalled::ChainPtr_t dummy;
  auto &currChainPtrRef = thread_chain_ref(dummy);

  ink_assert( ! p );

  if ( ! currChainPtrRef ) {
    ink_stack_trace_dump();
  }

  _eventChainPtr = ( currChainPtrRef.use_count() == 1 
                          ? currChainPtrRef
                          : new_ctor_chain(currChainPtrRef) );

  // copy in next-callback (or ctor-callback-rec)
  _assignPoint = _eventChainPtr->back()._assignPoint;

//  if ( _eventChainPtr == currChainPtrRef ) {
//    Debug("conttrace","%p: init-created: %s %s@%d",this,_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
//  }
}


//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr)
{
  ink_assert(&hdlr);

  _eventChainPtr = new_ctor_chain(thread_chain());
}

/////////////////////////////////////////////////////
// create startup chain for constructors [if any]
/////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlr_t point)
   : _assignPoint(&point),
     _currentCallChain(thread_chain_ref(_dummyChain)),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  // swap temp chain to current
  push_incomplete_call(new_ctor_chain(_currentCallChain), 0);
//  Debug("conttrace","init-only-call: %s %s@%d", point->_kLabel, point->_kFile, point->_kLine);
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlrState &state, int event)
   : _assignPoint(&state.operator EventHdlr_t()), // remember state's assignPoint
     _state(&state),
     _currentCallChain(thread_chain_ref(_dummyChain)),
     _allocCounterRef(*jemallctl::thread_allocatedp()),
     _deallocCounterRef(*jemallctl::thread_deallocatedp()),
     _allocStamp(_allocCounterRef),
     _deallocStamp(_deallocCounterRef)
{
  // swap hdlrstate's chain to current (if different)
  push_incomplete_call(state._eventChainPtr, event);
}

int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  EventCallContext _ctxt{*this,event};
  if ( _assignPoint->_kTSEventFunc ) {
    // direct C call.. 
    return (*_assignPoint->_kTSEventFunc)(ptr,event,data);
  }
  // C++ wrapper ...
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
  auto r = new(p) EventCallContext(reinterpret_cast<EventHdlr_t>(*recp));
  return reinterpret_cast<TSContDebug*>(r);
}

EventCalled::EventCalled(EventHdlr_t point, int event)
   : _assignPoint(&point), // constructor-only 
     _extCallerChain(thread_chain()),  // dflt
     _event(event),
     _extCallerChainLen( _extCallerChain ? _extCallerChain->size() : 0 )
{ 
}

EventHdlrState &EventHdlrState::operator=(nullptr_t)
{
  _assignPoint = &kCtorRecord;
  return *this;
}
