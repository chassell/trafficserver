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

class Event;

class Continuation : private force_VFPT_to_top
{
public:
  using ContinuationHandler = int (Continuation::*)(int event, Event *data);

protected:
  /**
    The current continuation handler function.

    The current handler should not be set directly. In order to
    change it, first aquire the Continuation's lock and then use
    the SET_HANDLER macro which takes care of the type casting
    issues.

  */
  ContinuationHandler handler_ = nullptr;

public:

  template <typename T_HDLR>
  void set_handler(T_HDLR hdlr) { handler_ = static_cast<ContinuationHandler>(hdlr); }

#ifdef DEBUG
  const char *handlerName_ = nullptr;

  template <typename T_HDLR>
  void set_handler(T_HDLR hdlr, const char *name) 
  { 
    handler_ = static_cast<ContinuationHandler>(hdlr); 
    handlerName_ = name;
  }
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
  handleEvent(int event = CONTINUATION_EVENT_NONE, Event *data = nullptr)
  {
    return (this->*handler_)(event, data);
  }

  /**
    Contructor of the Continuation object. It should not be used
    directly. Instead create an object of a derived type.

    @param amutex Lock to be set for this Continuation.

  */
  Continuation(ProxyMutex *amutex = nullptr);
  Continuation(Ptr<ProxyMutex> &amutex);
};

template <typename T_OBJ>
class ContinuationTmpl : public Continuation
{
public:
  using ContinuationHandler = int (ContinuationTmpl::*)(int event, T_OBJ *data);

  template <typename T_HDLR>
  void set_handler(T_HDLR hdlr) { handler_ = reinterpret_cast<Continuation::ContinuationHandler>( static_cast<ContinuationHandler>(hdlr) ); }

  template <typename T_HDLR>
  void push_handler(T_HDLR hdlr) 
  {
    ink_assert( ! handlerDelayed_ );

    handlerDelayed_ = static_cast<ContinuationHandler>(hdlr);
    set_handler(hdlr);
  }

  void pop_handler()
  {
    ink_assert( handlerDelayed_ );

#ifdef DEBUG
    set_handler(handlerDelayed_,handlerDelayedName_);
    handlerDelayedName_ = nullptr;
#else
    set_handler(handlerDelayed_);
#endif

    handlerDelayed_ = nullptr;
  }

#ifdef DEBUG
  template <typename T_HDLR>
  void set_handler(T_HDLR hdlr, const char *name) 
  { 
    set_handler(hdlr);
    handlerName_ = name;
  }

  template <typename T_HDLR>
  void push_handler(T_HDLR hdlr, const char *name) 
  {
    push_handler(hdlr);
    handlerDelayedName_ = handlerName_;
  }
#endif

  int
  handleEvent(int event = CONTINUATION_EVENT_NONE, T_OBJ *data = nullptr)
  {
    return (this->*reinterpret_cast<ContinuationHandler>(handler_))(event, data);
  }

  ContinuationTmpl(ProxyMutex *amutex = nullptr) : Continuation{amutex}, handlerDelayed_(nullptr) 
     { }
  ContinuationTmpl(Ptr<ProxyMutex> &amutex) : Continuation{amutex}, handlerDelayed_(nullptr) 
     { }

private:
  ContinuationHandler handlerDelayed_ = nullptr;
#ifdef DEBUG
  const char *handlerDelayedName_ = nullptr;
#endif
};

/**
  Sets the Continuation's handler. The preferred mechanism for
  setting the Continuation's handler.

  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_HANDLER(_h) set_handler(_h,#_h)
#else
#define SET_HANDLER(_h) set_handler(_h)
#endif

/**
  Sets a Continuation's handler.

  The preferred mechanism for setting the Continuation's handler.

  @param _c Pointer to a Continuation whose handler is being set.
  @param _h Pointer to the function used to callback with events.

*/
#ifdef DEBUG
#define SET_CONTINUATION_HANDLER(_c, _h) (_c->set_handler(_h,#_h))
#else
#define SET_CONTINUATION_HANDLER(_c, _h) (_c->set_handler(_h))
#endif

#ifdef DEBUG
#define PUSH_HANDLER(_h) (this->push_handler(_h,#_h))
#else
#define PUSH_HANDLER(_h) (this->push_handler(_h))
#endif

#define POP_HANDLER(_h) (pop_handler())

inline Continuation::Continuation(Ptr<ProxyMutex> &amutex)
  : mutex(amutex)
{
  // Pick up the control flags from the creating thread
  this->control_flags.set_flags(get_cont_flags().get_flags());
}

inline Continuation::Continuation(ProxyMutex *amutex)
  : mutex(amutex)
{
  // Pick up the control flags from the creating thread
  this->control_flags.set_flags(get_cont_flags().get_flags());
}

#endif /*_Continuation_h_*/
