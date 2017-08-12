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

#ifndef _I_DebugCont_h_
#define _I_DebugCont_h_

#include "ts/apidefs.h"

#include <vector>
#include <memory>
#include <type_traits>
#include <cstddef>
#include <chrono>

struct Continuation;
struct EventHdlrAssignRec;
struct EventCalled;
struct EventCallContext;

using EventChain_t = std::vector<EventCalled>;
using EventChainPtr_t = std::shared_ptr<EventChain_t>;
using EventChainWPtr_t = std::weak_ptr<EventChain_t>;
using EventHdlr_t = const EventHdlrAssignRec *;

using EventHdlrMethodPtr_t = int (Continuation::*)(int event, void *data);
using EventHdlrFxn_t       = int(Continuation *, int event, void *data);
// using TSEventFuncSig = int(TSCont contp, TSEvent event, void *edata);

using EventHdlrFxnPtr_t    = EventHdlrFxn_t *;
// using TSEventFunc = = TSEventFuncSig *;

using EventHdlrCompare_t    = bool(EventHdlrMethodPtr_t);
using EventFuncCompare_t    = bool(TSEventFunc);

using EventHdlrFxnGen_t    = EventHdlrFxnPtr_t(void);
using TSEventFuncGen_t     = TSEventFunc(void);
using EventHdlrCompareGen_t = EventHdlrCompare_t *(void);
using EventFuncCompareGen_t = EventFuncCompare_t *(void);

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventHdlrAssignRec 
{
 public:
  template<typename T_FN, T_FN FXN>
  struct const_cb_callgen;

  struct const_cb_callgen_base
  {
    static EventHdlrCompare_t *cmphdlr(void)
      { return [](EventHdlrMethodPtr_t) -> bool { return false; }; }
    static EventFuncCompare_t *cmpfunc(void)
      { return [](TSEventFunc) -> bool { return false; }; }
    static EventHdlrFxn_t *hdlr(void)
      { return [](Continuation *, int, void *) { return 0; }; }
    static TSEventFuncSig *func(void)
      { return [](TSCont, TSEvent, void *) { return 0; }; }
  };

 public:
  using Ptr_t = const EventHdlrAssignRec *;
  using Ref_t = const EventHdlrAssignRec &;

  bool operator!=(EventHdlrMethodPtr_t a) const 
     { return ! _kEqualHdlr_Gen || ! _kEqualHdlr_Gen()(a); }
  bool operator!=(TSEventFunc b) const 
     { return ! _kEqualFunc_Gen || ! _kEqualFunc_Gen()(b); }

 public:
  const char *const _kLabel;   // at point of assign
  const char *const _kFile;    // at point of assign
  uint32_t    const _kLine:16; // at point of assign

  EventHdlrCompareGen_t *const _kEqualHdlr_Gen; // distinct method-pointer (not usable)
  EventFuncCompareGen_t *const _kEqualFunc_Gen; // distinct method-pointer (not usable)

  EventHdlrFxnGen_t     *const _kWrapHdlr_Gen; // ref to custom wrapper function-ptr callable 
  TSEventFuncGen_t      *const _kWrapFunc_Gen; // ref to custom wrapper function-ptr callable 
};

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
class EventHdlrState 
{
 public:
  using Ptr_t = EventHdlr_t;
  using Ref_t = EventHdlrAssignRec::Ref_t;

 public:
  EventHdlrState();
  ~EventHdlrState();

  operator EventHdlr_t() 
  {
    return _assignPoint;
  }

  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*a)(int,T_ARG)) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  bool operator!=(TSEventFunc b) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<TSEventFunc>(b);
  }

  EventHdlrState &operator=(nullptr_t) 
  {
    _assignPoint = nullptr;
    return *this;
  }

  EventHdlrState &operator=(TSEventFunc f) {
    return *this;
  }

  // class method-pointer full args
  EventHdlrState &operator=(Ptr_t &prev)
  {
    _assignPoint = prev;
    return *this;
  }

  // class method-pointer full args
  EventHdlrState &operator=(Ref_t cbAssign)
  {
    _assignPoint = &cbAssign;
    return *this;
  }

  void push_caller_record(const EventCallContext &ctxt, unsigned event) const;

  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);


private:
  Ptr_t           _assignPoint = nullptr;
  EventChainPtr_t _eventChainPtr;         // current stored "path"
};

struct EventCallContext
{
  using steady_clock = std::chrono::steady_clock;
  using time_point = steady_clock::time_point;

  // explicitly define a "called" record.. and *maybe* a call-context too
  EventCallContext(EventHdlr_t next = nullptr);
  EventCallContext(EventHdlrState &state, unsigned event);
  ~EventCallContext();

  EventHdlr_t     const _assignPoint;
  EventHdlrState* const _state = nullptr;
  EventChainPtr_t      &_currentCallChain;
  uint64_t             &_allocCounterRef;
  uint64_t             &_deallocCounterRef;

  uint64_t        const _allocStamp;
  uint64_t        const _deallocStamp;
  time_point      const _start = steady_clock::now();

  // on stack (or as a member)
  void *operator new(const std::size_t) = delete; 
};

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventCalled
{
  EventCalled(const EventCallContext &ctxt, EventChainPtr_t const &toswap, unsigned event = 0);
  static EventCalled *pop_caller_record(const EventCallContext &done);

  static void printLog(const EventChainPtr_t &chainPtr, char const *const msg);
  
  // fill in deltas
  void completed(const EventCallContext &ctxt, const EventChainPtr_t &chain);
  void trim_call(const EventChainPtr_t &chain);

  // upon ctor
  EventHdlr_t               _assignPoint = nullptr;   // CB dispatched (null if for ctor)

  // upon ctor
  EventChainPtr_t           _extCallerChain;          // ext chain for caller (null if internal)
                                                      // NOTE: use to switch back upon return

  // upon ctor
  uint16_t                  _event = 0;            // specific event-int used on CB

  // upon ctor
  uint8_t                   _extCallerChainLen = 0; // extern caller's chain-size upon entry (or local)

  // upon return
  uint8_t                   _intReturnedChainLen = 0; // local chain-size upon final CB return

  // upon return
  uint32_t                  _allocDelta = 0;       // actual delta upon return
  // upon return
  uint32_t                  _deallocDelta = 0;     // actual delta upon return

  // upon return
  float                     _delay = 0;               // total lapsed time [0.0 only if incomplete]
};

// 
// DeferredCall::defer(...)      
//    [static templated call]
// DeferredCall::handleEvent --> this->call.operator()      
//    [strongly typed field]
//
// INKVConnInternal::init() 
// INKContInternal::handle_event --> INKContInternal::m_event_func.operator(TSCont,TSEvent,void*)
//        [created with fxn pointer]
// INKVConnInternal::init() 
// INKVConnInternal::handle_event --> INKContInternal::m_event_func.operator(TSCont,TSEvent,void*)
//        [created with fxn pointer]

#define EVENT_HANDLER_RECORD(_h, name) \
     using decay_t = typename std::decay<decltype(_h)>::type; \
     static constexpr auto name =                          \
                    EventHdlrAssignRec{                   \
                       ((#_h)+0),                          \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmphdlr,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmpfunc,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::hdlr, \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::func \
                    }

// define null as "caller" context... and set up next one
#define SET_NEXT_HANDLER_RECORD(_h)                   \
            EVENT_HANDLER_RECORD(_h, kHdlrAssignRec); \
            EventCallContext _ctxt(&kHdlrAssignRec);

// standard Continuation handler signature(s)
template<class T_OBJ, typename T_ARG, int(T_OBJ::*FXN)(int,T_ARG)>
struct EventHdlrAssignRec::const_cb_callgen<int(T_OBJ::*)(int,T_ARG),FXN>
   : public const_cb_callgen_base
{
  static EventHdlrCompare_t *cmphdlr(void) {
    return [](EventHdlrMethodPtr_t method) { 
      return reinterpret_cast<EventHdlrMethodPtr_t>(FXN) == method; 
    };
  }
  static EventHdlrFxn_t *hdlr(void)
  {
    return [](Continuation *self, int event, void *arg) {
       return (static_cast<T_OBJ*>(self)->*FXN)(event, static_cast<T_ARG>(arg));
     };
  }
};

#define LOG_SKIPPABLE_EVENTHDLR(_h) \
  template <>                                                               \
  EventHdlrCompare_t *EventHdlrAssignRec::const_cb_callgen<decltype(_h),(_h)>::cmphdlr() \
     { return nullptr; }

#endif // _I_DebugCont_h_

////////////////////////////////////////////////////////////////////////
#if defined(__TS_API_H__) && ! defined(_I_DebugCont_TSAPI_)
#define _I_DebugCont_TSAPI_

#define TSThreadCreate(func,data)                              \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSThreadCreate(func,data);                      \
             })

#define TSContCreate(func,mutexp)                              \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSContCreate(reinterpret_cast<TSEventFunc>(func),mutexp);                      \
             })
#define TSTransformCreate(func,txnp)                           \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSTransformCreate(reinterpret_cast<TSEventFunc>(func),txnp);                 \
             })
#define TSVConnCreate(func,mutexp)                             \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSVConnCreate(reinterpret_cast<TSEventFunc>(func),mutexp);                     \
             })

// standard TSAPI event handler signature
template <TSEventFunc FXN>
struct EventHdlrAssignRec::const_cb_callgen<TSEventFunc,FXN>
   : public const_cb_callgen_base
{
  static EventFuncCompare_t *cmpfunc(void) {
    return [](TSEventFunc fxn) { return FXN == fxn; };
  }
  static TSEventFuncSig *func(void)
  {
    return [](TSCont cont, TSEvent event, void *data) {
       return (*FXN)(cont,event,data);
     };
  }
};

// extra case that showed up
template <void(*FXN)(TSCont,TSEvent,void *data)>
struct EventHdlrAssignRec::const_cb_callgen<void(*)(TSCont,TSEvent,void *data),FXN>
   : public const_cb_callgen_base
{
  static EventFuncCompare_t *cmpfunc(void) {
    return [](TSEventFunc fxn) { 
      return reinterpret_cast<TSEventFunc>(FXN) == fxn; 
    };
  }
  static TSEventFuncSig *func(void)
  {
    return [](TSCont cont, TSEvent event, void *data) {
       (*FXN)(cont,event,data);
       return 0;
     };
  }
};


template <TSThreadFunc FXN>
struct EventHdlrAssignRec::const_cb_callgen<TSThreadFunc,FXN>
   : public const_cb_callgen_base
{
};

#endif
