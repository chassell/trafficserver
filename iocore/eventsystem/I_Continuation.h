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

  @section details Details

  Continuations have a handleEvent() method to invoke them. Users
  can determine the behavior of a Continuation by suppling a
  "ContinuationHandler" (member function name) which is invoked
  when events arrive. This function can be changed with the
  "setHandler" method.

  Continuations can be subclassed to add additional state and
  methods.

 */

#ifndef _I_Continuation_h_
#define _I_Continuation_h_

#include "ts/ink_platform.h"
#include "ts/List.h"
#include "I_Lock.h"
#include "ts/ContFlags.h"

#include <functional>

class Event;
class ContinuationQueue;
class Processor;
class ProxyMutex;
class EThread;

//////////////////////////////////////////////////////////////////////////////
//
//  Constants and Type Definitions
//
//////////////////////////////////////////////////////////////////////////////

#define CONTINUATION_EVENT_NONE 0

#define CONTINUATION_DONE 0
#define CONTINUATION_CONT 1

/**
  Base class for all state machines to receive notification of
  events.

  The Continuation class represents the main abstraction mechanism
  used throughout the IO Core Event System to communicate its users
  the occurrence of an event. A Continuation is a lightweight data
  structure that implements a single method with which the user is
  called back.

  Continuations are typically subclassed in order to implement
  event-driven state machines. By including additional state and
  methods, continuations can combine state with control flow, and
  they are generally used to support split-phase, event-driven
  control flow.

  Given the multithreaded nature of the Event System, every
  continuation carries a reference to a ProxyMutex object to protect
  its state and ensure atomic operations. This ProxyMutex object
  must be allocated by continuation-derived classes or by clients
  of the IO Core Event System and it is required as a parameter to
  the Continuation's class constructor.

*/

class ContinuationBase
{
public:
  template <size_t N_EVENT, typename T_PASSED>
  struct Event { operator int() { return N_EVENT; } };

  /**
    Contructor of the Continuation object. It should not be used
    directly. Instead create an object of a derived type.

    @param amutex Lock to be set for this Continuation.

  */
  ContinuationBase(Ptr<ProxyMutex> const &amutex) 
     : mutex{amutex}, 
       // Pick up the control flags from the creating thread
       control_flags(::get_cont_flags().get_flags())
  { }

  ContinuationBase(ProxyMutex *amutex) 
     : mutex{amutex}, 
       // Pick up the control flags from the creating thread
       control_flags(::get_cont_flags().get_flags())
  { }

  ContinuationBase()
     : control_flags(::get_cont_flags().get_flags())
  { }

  virtual ~ContinuationBase() {}

  typename <class T_THIS, typename T_EVENT, typename T_PASSPTR>
  inline static int handleEventBase(T_THIS &obj,

//  const Ptr<ProxyMutex> &mutex() const { return mutex_; }
//  const Ptr<ProxyMutex> &set_mutex(const Ptr<ProxyMutex> &m) { return (mutex_ = m); }
  /**
    The ContinuationBase's lock.

    A reference counted pointer to the ContinuationBase's lock. This
    lock is initialized in the constructor and should not be set
    directly.

  */
  Ptr<ProxyMutex> mutex;
  /**
    Contains values for debug_override and future flags that
    needs to be thread local while this continuation is running
  */
  ContFlags control_flags;
private:

};

template <typename T_ARG>
class ContinuationArg : virtual public ContinuationBase
{
 public:
  /**
    Link to other continuations.

    A doubly-linked element to allow Lists of Continuations to be
    assembled.

  */
  LINK(Continuation, link);

  ContinuationArg(Ptr<ProxyMutex> const &amutex) : ContinuationBase(amutex) { }
  ContinuationArg(ProxyMutex *amutex) : ContinuationBase(amutex) { }
  ContinuationArg() { }

  template<
  auto set_next_call(NextCallFxnPtr fxn) 
    { nextCall_ = std::mem_fn(fxn); }

  void set_next_call(nullptr_t) 
    { nextCall_ = std::mem_fn(reinterpret_cast<NextCallFxnPtr>(&ContinuationTmpl::null)); }

  int handleEvent()
    { return handleEventBase(CONTINUATION_EVENT_NONE,nullptr); }

  int handleEvent(int event)
    { return handleEventBase(event,nullptr); }

  int handleEventBase(int event, T_PASSPTR *data)
    { return nextCall_(dynamic_cast<T_TOPOBJ*>(this),event,data); }

private:
  int null(int,T_PASSPTR*) { return 0; }
  /**
    The current continuation handler function.

    The current handler should not be set directly. In order to
    change it, first aquire the ContinuationTmpl's lock and then use
    the SET_HANDLER macro which takes care of the type casting
    issues.

  */
  anyEventCall_ = std::mem_fn(reinterpret_cast<NextCallFxnPtr>(&ContinuationTmpl::null));
#ifdef DEBUG
  const char *nextCallName_ = nullptr;
#endif
};

/**
  Sets the Continuation's handler. The preferred mechanism for
  setting the Continuation's handler.

  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_HANDLER(_h) this->set_next_call(_h,#_h)
#else
#define SET_HANDLER(_h) this->set_next_call(_h)
#define PUSH_HANDLER(_h) this->set_next_call(_h)
#endif

/**
  Sets a Continuation's handler.

  The preferred mechanism for setting the Continuation's handler.

  @param _c Pointer to a Continuation whose handler is being set.
  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_CONTINUATION_HANDLER(_c, _h) _c->set_next_call(_h,#_h)
#else
#define SET_CONTINUATION_HANDLER(_c, _h) _c->set_next_call(_h)
#endif


#define handleEvent(evt,ptr)      handleEventCall(Continuation::Event<evt>(),ptr)

#endif /*_Continuation_h_*/
