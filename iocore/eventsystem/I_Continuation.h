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
  "Hdlr" (member function name) which is invoked
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

#include <chrono>

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

class Continuation : private force_VFPT_to_top
{
public:
  struct HdlrAssignRec;
  struct EventHdlrLogRec;

  using Hdlr_t = const HdlrAssignRec*;
  using HdlrMethodPtr_t = int (Continuation::*)(int event, void *data);
  using HdlrFxn_t       = int(Continuation *, int event, void *data);
  using HdlrFxnFxn_t    = HdlrFxn_t*(void);
  using HdlrFtor_t      = std::function<HdlrFxn_t>;

  using EventHdlrLog = std::vector<EventHdlrLogRec>;
  using EventHdlrLogPtr = std::shared_ptr<EventHdlrLog>;


  /**
    The current continuation handler function.

    The current handler should not be set directly. In order to
    change it, first aquire the Continuation's lock and then use
    the SET_HANDLER macro which takes care of the type casting
    issues.

  */
  const HdlrAssignRec &
  handler()
  {
    return *_handlerRec;
  }

#ifdef DEBUG
  const char *handler_name = nullptr;
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

  struct HdlrAssignGrp;
  struct HdlrAssignRec 
  {
    HdlrAssignGrp    &_assignGrp;      // static-global for compile
    const char *const _label;        // at point of assign
    const char *const _file;         // at point of assign
    uint32_t    const _line:20;      // at point of assign
    uint32_t    const _assignID:8;   // unique for each in compile-object
    const HdlrMethodPtr_t _fxnPtr;   // distinct method-pointer (not usable)
    const HdlrFxnFxn_t *_cbGenerator; // ref to custom wrapper function-ptr callable 

    operator Hdlr_t() const { return this; }
    unsigned int id() const { return _assignID; };
    unsigned int grpid() const;

    template <class T_OBJ, typename T_ARG>
    bool operator!=(int(T_OBJ::*b)(int,T_ARG)) const
    {
      return _fxnPtr != (HdlrMethodPtr_t) b;
    }
  };

  struct HdlrAssignGrp
  {
    enum { MAX_ASSIGN_COUNTER = 60, MAX_ASSIGNGRPS = 255 };

  private:
    static unsigned             s_hdlrAssignGrpCnt;
    static const HdlrAssignGrp *s_assignGrps[MAX_ASSIGNGRPS];

  public:
    static void                 printLog(void *ptr, EventHdlrLogPtr logPtr);

    HdlrAssignGrp();

    inline unsigned int add_if_unkn(const HdlrAssignRec &rec);

    static Hdlr_t lookup(const EventHdlrLogRec &rec);

  private:
    Hdlr_t                      _assigns[MAX_ASSIGN_COUNTER] = { nullptr };
    unsigned int                _id;
  };

  struct EventHdlrLogRec
  {
    unsigned int _assignID:8;
    unsigned int _assignGrpID:8;
    size_t       _allocDelta:32;
    size_t       _deallocDelta:32;
    unsigned int _logUSec:16;

    operator Hdlr_t() const { return HdlrAssignGrp::lookup(*this); }
  };

  inline void 
  set_next_hdlr(nullptr_t)
  {
    _handlerRec = nullptr;
    _handler = nullptr;
  };

  // class method-pointer full args
  inline void 
  set_next_hdlr(Hdlr_t &prev)
  {
    _handlerRec = prev;
    _handler = (*prev->_cbGenerator)();  // exe loader-time value only
  };

  // class method-pointer full args
  inline void
  set_next_hdlr(const HdlrAssignRec &cbAssign)
  {
    _handlerRec = &cbAssign;
    _handler = (*cbAssign._cbGenerator)(); // exe loader-time value only
  };

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
  handleEvent(int event = CONTINUATION_EVENT_NONE, void *data = 0);

  /**
    Contructor of the Continuation object. It should not be used
    directly. Instead create an object of a derived type.

    @param amutex Lock to be set for this Continuation.

  */
  Continuation(ProxyMutex *amutex = NULL);

private:
  Hdlr_t           _handlerRec = nullptr; // (cause to change upon first use)
  HdlrFxn_t       *_handler = nullptr;    // actual wrapper-fxn-ptr compiled
  HdlrFtor_t       _handlerApply;
  EventHdlrLogPtr  _handlerLogPtr;        // current stored log
};

using ContinuationHandler = Continuation::Hdlr_t;

namespace {
Continuation::HdlrAssignGrp s_fileAssignGrp;
}

/*
  Sets the Continuation's handler. The only mechanism for
  setting the Continuation's handler.

  @param _h Pointer to the function used to callback with events.

*/
#define CLEAR_HANDLER() this->set_next_hdlr(nullptr);

#define SET_CONTINUATION_HANDLER(obj,_h) \
   ({                                                      \
     constexpr auto kHdlrAssignID = __COUNTER__;               \
     static_assert(kHdlrAssignID < Continuation::HdlrAssignGrp::MAX_ASSIGN_COUNTER, \
        "cont-targets exceeded file limit");               \
     static constexpr auto kHdlrAssignRec =                       \
                     Continuation::HdlrAssignRec{          \
                       s_fileAssignGrp,                    \
                       ((#_h)+0),                          \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                       kHdlrAssignID,                      \
                      (Continuation::HdlrMethodPtr_t) _h,  \
                      &::cb_wrapper<decltype(_h),(_h)>::gen_hdlrfxn  \
                     };                                    \
     (obj)->set_next_hdlr(kHdlrAssignRec);                 \
   })

#define SET_HANDLER(_h) SET_CONTINUATION_HANDLER(this,_h)

#define SET_SAVED_HANDLER(_var) this->set_next_hdlr(_var)

/**
  Sets a Continuation's handler.

  The preferred mechanism for setting the Continuation's handler.

  @param _c Pointer to a Continuation whose handler is being set.
  @param _h Pointer to the function used to callback with events.

*/

inline Continuation::Continuation(ProxyMutex *amutex) : mutex(amutex)
{
  // Pick up the control flags from the creating thread
  this->control_flags.set_flags(get_cont_flags().get_flags());
}

template<typename T_FN, T_FN FXN>
struct cb_wrapper;

template<class T_OBJ, typename T_ARG, int(T_OBJ::*FXN)(int,T_ARG)>
struct cb_wrapper<int(T_OBJ::*)(int,T_ARG),FXN>
{
  static Continuation::HdlrFxn_t *gen_hdlrfxn(void)
  {
    return [](Continuation *self, int event, void *arg) {
       return (static_cast<T_OBJ*>(self)->*FXN)(event, static_cast<T_ARG>(arg));
     };
  }
};

template<class T_OBJ, class T_AOBJ, int(T_OBJ::*FXN)(T_AOBJ*)>
struct cb_wrapper<int(T_OBJ::*)(T_AOBJ*),FXN>
{
  static Continuation::HdlrFxn_t *gen_hdlrfxn(void)
  {
    return [](Continuation *self, int event, void *arg) {
       return (static_cast<T_OBJ*>(self)->*FXN)(static_cast<T_AOBJ*>(arg));
     };
  }
};

template<class T_OBJ, int(T_OBJ::*FXN)(int)>
struct cb_wrapper<int(T_OBJ::*)(int),FXN>
{
  static Continuation::HdlrFxn_t *gen_hdlrfxn(void)
  {
    return [](Continuation *self, int event, void *) {
       return (static_cast<T_OBJ*>(self)->*FXN)(event);
     };
  }
};

template<class T_OBJ, int(T_OBJ::*FXN)(void)>
struct cb_wrapper<int(T_OBJ::*)(void),FXN>
{
  static Continuation::HdlrFxn_t *gen_hdlrfxn(void)
  {
    return [](Continuation *self, int, void *) {
       return (static_cast<T_OBJ*>(self)->*FXN)();
     };
  }
};

inline unsigned int Continuation::HdlrAssignGrp::add_if_unkn(const HdlrAssignRec &rec) 
{
   if ( ! _assigns[rec.id()] ) {
      _assigns[rec.id()] = &rec; // should be room
   }
   return _id;
}

#endif /*_Continuation_h_*/
