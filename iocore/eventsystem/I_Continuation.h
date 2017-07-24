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

class Continuation;
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

using ContinuationHandlerMethodPtr_t = int (Continuation::*)(int event, void *data);
using ContinuationHandlerFxn_t = int(Continuation *, int event, void *data);
using ContinuationHandlerFtor_t = std::function<ContinuationHandlerFxn_t>;

//typedef int (Continuation::*ContinuationHandler)(int event, void *data);
using ContinuationHandler = ContinuationHandlerMethodPtr_t;

class force_VFPT_to_top
{
public:
  virtual ~force_VFPT_to_top() {}
};

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

class Continuation : private force_VFPT_to_top
{
public:
  /**
    The current continuation handler function.

    The current handler should not be set directly. In order to
    change it, first aquire the Continuation's lock and then use
    the SET_HANDLER macro which takes care of the type casting
    issues.

  */
  ContinuationHandler       handler;

#ifdef DEBUG
  const char *handler_name;
#endif

  /**
    The Continuation's lock.

    A reference counted pointer to the Continuation's lock. This
    lock is initialized in the constructor and should not be set
    directly.

  */
  Ptr<ProxyMutex> mutex;

  /**
    Link to other continuations.

    A doubly-linked element to allow Lists of Continuations to be
    assembled.

  */
  LINK(Continuation, link);

  /**
    Contains values for debug_override and future flags that
    needs to be thread local while this continuation is running
  */
  ContFlags control_flags;

  void set_next_call(nullptr_t, ... ) { 
    _handlerApply = ContinuationHandlerFtor_t{}; 
    handler = nullptr;
  }

  void set_next_call(int, ... ) { 
    _handlerApply = ContinuationHandlerFtor_t{}; 
    handler = nullptr;
  }

  // class method-pointer full args
  template <class T_OBJ, typename T_ARG>
  inline auto set_next_call(int(T_OBJ::*fxn)(int,T_ARG), T_OBJ *obj, const char *name = 0) -> int
  {
    auto f = [fxn,obj](int event, void *arg) { return (obj->*fxn)(event,static_cast<T_ARG>(arg)); };
    return wrap_callback(f, (ContinuationHandler) fxn, name);
  };

  // class method with event arg
  template <class T_OBJ>
  inline auto set_next_call(int(T_OBJ::*fxn)(int), T_OBJ *obj, const char *name = 0) -> int
  {
    auto f = [fxn,obj](int event, void *) { return (obj->*fxn)(event); };
    return wrap_callback(f, (ContinuationHandler) fxn, name);
  }

  // class method with no args
  template <class T_OBJ>
  inline auto set_next_call(int(T_OBJ::*fxn)(void), T_OBJ *obj, const char *name = 0) -> int
  {
    auto f = [fxn,obj](int, void *) { return (obj->*fxn)(); };
    return wrap_callback(f, (ContinuationHandler) fxn, name);
  }

  // lambda or call with no object 
  template <class T_CALLABLE>
  inline auto set_next_call(T_CALLABLE const &callable, const char *name = 0) 
     -> decltype( callable()(1,nullptr), 0 )
  {
    using FunctorOp = decltype( T_CALLABLE::operator() );
    using SecondArg = typename std::function<FunctorOp>::second_argument_type;

    // should copy it
    auto f = [callable](int event, void *arg) { return callable(event,static_cast<SecondArg>(arg)); };
    return wrap_callback(f, (ContinuationHandler) handleEvent, name);
  }

  /**
    Receives the event code and data for an Event.

    This function receives the event code and data for an event and
    forwards them to the current continuation handler. The processor
    calling back the continuation is responsible for acquiring its
    lock.

    @param event Event code to be passed at callback (Processor specific).
    @param data General purpose data related to the event code (Processor specific).
    @return State machine and processor specific return code.

  */
  int
  handleEvent(int event = CONTINUATION_EVENT_NONE, void *data = 0)
  {
    return _handlerApply(this,event,data);
  }

  /**
    Contructor of the Continuation object. It should not be used
    directly. Instead create an object of a derived type.

    @param amutex Lock to be set for this Continuation.

  */
  Continuation(ProxyMutex *amutex = NULL);

private:
  template <class T_FUNCTOR>
  int wrap_callback(T_FUNCTOR const &ftor, ContinuationHandler fxn, const char *name);

  void save_call_context();
  int push_context();
  void restore_context(int);

private:
  ContinuationHandlerFtor_t _handlerApply;
  int                       _handlerArena = 0;
};

/**
Sets the Continuation's handler. The preferred mechanism for
setting the Continuation's handler.

  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_HANDLER(_h) this->set_next_call(_h,this,#_h+0U)
#else
#define SET_HANDLER(_h) this->set_next_call(_h,this)
#endif

/**
  Sets a Continuation's handler.

  The preferred mechanism for setting the Continuation's handler.

  @param _c Pointer to a Continuation whose handler is being set.
  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_CONTINUATION_HANDLER(_c, _h) (_c)->set_next_call((_h),(_c),#_h+0)
#else
#define SET_CONTINUATION_HANDLER(_c, _h) (_c)->set_next_call((_h),(_c))
#endif

inline Continuation::Continuation(ProxyMutex *amutex)
  : handler(),
#ifdef DEBUG
    handler_name(),
#endif
    mutex(amutex)
{
  // Pick up the control flags from the creating thread
  this->control_flags.set_flags(get_cont_flags().get_flags());
}

template <class T_FUNCTOR>
inline int Continuation::wrap_callback(T_FUNCTOR const &ftor, ContinuationHandler fxn, const char *name)
{
  handler = fxn;
#ifdef DEBUG
  handler_name = name;
#endif
  save_call_context();

  _handlerApply = [ftor](Continuation *self, int event, void *vparg) -> int
  {
    auto tmp = self->push_context(); 
    auto r = ftor(event,vparg);
    self->restore_context(tmp);
    return r;
  };

  return 0;
}

#endif /*_Continuation_h_*/
