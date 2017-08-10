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

using EventChain_t = std::vector<EventCalled>;
using EventChainPtr_t = std::shared_ptr<EventChain_t>;
using EventChainWPtr_t = std::weak_ptr<EventChain_t>;
using EventHdlr_t = const EventHdlrAssignRec *;

using EventHdlrFxn_t       = int(Continuation *, int event, void *data);
using EventHdlrFxnPtr_t    = EventHdlrFxn_t *;
using EventHdlrMethodPtr_t = int (Continuation::*)(int event, void *data);
using EventHdlrCompare_t    = bool(EventHdlrMethodPtr_t, EventHdlrFxnPtr_t);
using EventHdlrFxnGen_t    = EventHdlrFxn_t*(void);
using EventHdlrCompareGen_t = EventHdlrCompare_t *(void);

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventHdlrAssignRec 
{
  using Ptr_t = const EventHdlrAssignRec *;
  using Ref_t = const EventHdlrAssignRec &;

  const char *const _label;   // at point of assign
  const char *const _file;    // at point of assign
  uint32_t    const _line:16; // at point of assign

  const EventHdlrCompareGen_t *_equalHdlr_Gen; // distinct method-pointer (not usable)
  const EventHdlrFxnGen_t     *_wrapHdlr_Gen; // ref to custom wrapper function-ptr callable 

public:
  void set() const;

  template<typename T_FN, T_FN FXN>
  struct gen_wrap_hdlr;

  // SFINAE check that FXN constant can be converted to HdlrMethodPtr_t
  template<typename T_FN, T_FN FXN>
  inline static auto gen_equal_hdlr_test() -> 
     decltype( reinterpret_cast<EventHdlrMethodPtr_t>(FXN), &std::declval<EventHdlrCompare_t>() )
  {
    // use first arg only
    return [](EventHdlrMethodPtr_t method, EventHdlrFxnPtr_t fxn) {
       return method && reinterpret_cast<EventHdlrMethodPtr_t>(FXN) == method;
    };
  }

  // SFINAE check that FXN constant can be converted to HdlrFxnPtr_t
  template<typename T_FN, T_FN FXN>
  inline static auto gen_equal_hdlr_test(void) -> 
     decltype( reinterpret_cast<EventHdlrFxnPtr_t>(FXN), &std::declval<EventHdlrCompare_t>() )
  {
    // use latter arg only
    return [](EventHdlrMethodPtr_t method, EventHdlrFxnPtr_t fxn) {
       return fxn && reinterpret_cast<EventHdlrFxnPtr_t>(FXN) == fxn;
    };
  }

  bool operator!=(EventHdlrMethodPtr_t a) const 
     { return ! _equalHdlr_Gen || ! _equalHdlr_Gen()(a,nullptr); }
  bool operator!=(EventHdlrFxnPtr_t b) const 
     { return ! _equalHdlr_Gen || ! _equalHdlr_Gen()(nullptr,b); }
};

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
class EventHdlrState 
{
  using Ptr_t = EventHdlr_t;
  using Ref_t = EventHdlrAssignRec::Ref_t;

  Ptr_t         _handlerRec = nullptr; // (cause to change upon first use)
  EventChainPtr_t _eventChainPtr;      // current stored "path"

public:
  EventHdlrState();

  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);

  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*a)(int,T_ARG)) const {
    return ! _handlerRec || *_handlerRec != reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  bool operator!=(TSEventFunc b) const {
    return ! _handlerRec || *_handlerRec != reinterpret_cast<EventHdlrFxnPtr_t>(b);
  }

  EventHdlrState &operator=(nullptr_t) 
  {
    _handlerRec = nullptr;
    return *this;
  }

  EventHdlrState &operator=(TSEventFunc f) {
    return *this;
  }

  // class method-pointer full args
  EventHdlrState &operator=(Ptr_t &prev)
  {
    _handlerRec = prev;
    return *this;
  }

  // class method-pointer full args
  EventHdlrState &operator=(Ref_t cbAssign)
  {
    _handlerRec = &cbAssign;
    return *this;
  }
};

struct EventCallContext
{
  using steady_clock = std::chrono::steady_clock;
  using time_point = steady_clock::time_point;

  // explicitly define a "called" record.. and *maybe* a call-context too
  EventCallContext(EventHdlr_t next = nullptr);
  ~EventCallContext();

  EventHdlr_t  _calledHdlrRec = nullptr;
  uint64_t     _allocCounter = jemallctl::alloc_bytes_count();
  uint64_t     _deallocCounter = jemallctl::dealloc_bytes_count();
  time_point   _start = steady_clock::now();

  EventHdlr_t  _nextHdlrRec = nullptr;
};

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventCalled
{
  EventCalled(EventHdlr_t cb, unsigned event, const EventChain_t &callerChain);
  
  // fill in deltas
  void completed(const EventCallContext &ctxt);

  // upon ctor
  EventHdlr_t               _assignPoint;         // CB dispatched (null if for ctor)

  // upon ctor
  EventChainPtr_t           _extCallerChain;      // ext chain for caller (null if internal)
                                                  // NOTE: use to switch back upon return

  // upon ctor
  unsigned int              _event:16;            // specific event-int used on CB

  // upon ctor
  unsigned int              _extCallerChainEnd:8; // extern caller's chain-size upon entry (or local)

  // upon return
  unsigned int              _intReturnChainEnd:8; // local chain-size upon final CB return

  // upon return
  size_t                    _allocDelta:32;       // actual delta upon return
  // upon return
  size_t                    _deallocDelta:32;     // actual delta upon return

  // upon return
  float                     _delay;               // total lapsed time
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
                      &EventHdlrAssignRec::gen_equal_hdlr_test<decay_t,(_h)>,  \
                      &EventHdlrAssignRec::gen_wrap_hdlr<decay_t,(_h)>::fxn       \
                     }

// define null as "caller" context... and set up next one
#define SET_NEXT_HANDLER_RECORD(_h)                   \
            EVENT_HANDLER_RECORD(_h, kHdlrAssignRec); \
            EventCallContext _ctxt(&kHdlrAssignRec);

// standard Continuation handler signature(s)
template<class T_OBJ, typename T_ARG, int(T_OBJ::*FXN)(int,T_ARG)>
struct EventHdlrAssignRec::gen_wrap_hdlr<int(T_OBJ::*)(int,T_ARG),FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *arg) {
       return (static_cast<T_OBJ*>(self)->*FXN)(event, static_cast<T_ARG>(arg));
     };
  }
};

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
struct EventHdlrAssignRec::gen_wrap_hdlr<TSEventFunc,FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *data) {
       return (*FXN)(reinterpret_cast<TSCont>(self), static_cast<TSEvent>(event), data);
     };
  }
};

// standard TSAPI thread init signature
template<TSThreadFunc FXN>
struct EventHdlrAssignRec::gen_wrap_hdlr<TSThreadFunc,FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *data) {
      return 0;  // never used
    };
  }
};

// extra case that showed up
template <void(*FXN)(TSCont,TSEvent,void *data)>
struct EventHdlrAssignRec::gen_wrap_hdlr<void(*)(TSCont,TSEvent,void *data),FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *data) {
       return (*FXN)(reinterpret_cast<TSCont>(self), static_cast<TSEvent>(event), data),0;
     };
  }
};

#endif
