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
#include "ts/CCallbackDebug.h"

#include <vector>
#include <memory>
#include <type_traits>
#include <cstddef>
#include <chrono>
#include <cassert>

struct Continuation;
struct EventHdlrAssignRec;
struct EventCalled;
struct EventCallContext;
class EventHdlrState;

using EventHdlr_t = const EventHdlrAssignRec &;
using EventHdlrP_t = const EventHdlrAssignRec *;

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
    static EventHdlrCompare_t *cmphdlr_nolog(void)
      { return nullptr; }
    static EventFuncCompare_t *cmpfunc(void)
      { return [](TSEventFunc) -> bool { return false; }; }
    static EventHdlrFxn_t *hdlr(void)
      { return [](Continuation *, int, void *) { return 0; }; }
    static TSEventFuncSig *func(void)
      { return [](TSCont, TSEvent, void *) { return 0; }; }
  };

 public:
  using Ptr_t = const EventHdlrAssignRec *;

  bool operator!=(EventHdlrMethodPtr_t a) const 
     { return ! _kEqualHdlr_Gen || ! _kEqualHdlr_Gen()(a); }
  bool operator!=(TSEventFunc b) const 
     { return ! _kEqualFunc_Gen || ! _kEqualFunc_Gen()(b); }

  bool no_log() const {
    return ! _kEqualHdlr_Gen();
  }

 public:
  const char *const _kLabel;   // at point of assign
  const char *const _kFile;    // at point of assign
  uint32_t    const _kLine;    // at point of assign

  TSEventFunc const _kTSEventFunc; // direct callback ptr (from C)

  EventHdlrCompareGen_t *const _kEqualHdlr_Gen; // distinct method-pointer (not usable)
  EventFuncCompareGen_t *const _kEqualFunc_Gen; // distinct method-pointer (not usable)

  EventHdlrFxnGen_t     *const _kWrapHdlr_Gen; // ref to custom wrapper function-ptr callable 
  TSEventFuncGen_t      *const _kWrapFunc_Gen; // ref to custom wrapper function-ptr callable 
};

static_assert( offsetof(EventHdlrAssignRec, _kLabel) == offsetof(EventCHdlrAssignRec, _kLabel), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kFile) == offsetof(EventCHdlrAssignRec, _kFile), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kLine) == offsetof(EventCHdlrAssignRec, _kLine), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kTSEventFunc) == offsetof(EventCHdlrAssignRec, _kCallback), "offset layout mismatch");

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventCalled
{
  using Chain_t = std::vector<EventCalled>;
  using ChainPtr_t = std::shared_ptr<Chain_t>;

  EventCalled(EventHdlr_t point, int event = 0);
  EventCalled(void *p = NULL);

  bool no_log() const {
    return ! _assignPoint || _assignPoint->no_log();
  }

  static void printLog(Chain_t::const_iterator const &begin, Chain_t::const_iterator const &end, const char *msg);
  
  // fill in deltas
  void completed(EventCallContext const &ctxt, ChainPtr_t const &chain);
  void trim_call(Chain_t &chain);

  // upon ctor
  EventHdlrP_t               _assignPoint = nullptr;   // CB dispatched (null if for ctor)

  // upon ctor
  ChainPtr_t                _extCallerChain;          // ext chain for caller (null if internal)
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

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
struct EventCallContext
{
  using steady_clock = std::chrono::steady_clock;
  using time_point = steady_clock::time_point;

  // explicitly define a "called" record.. and *maybe* a call-context too
  explicit EventCallContext(EventHdlr_t next);
  EventCallContext(EventHdlrState &state, int event);
  ~EventCallContext();

  operator EventHdlr_t() const;

  void push_incomplete_call(EventCalled::ChainPtr_t const &calleeChain, int event) const;
  EventCalled *pop_caller_record();

  bool no_log() const {
    return ! _assignPoint || _assignPoint->no_log();
  }

  EventHdlrP_t     const _assignPoint = nullptr;
  EventHdlrState* const _state = nullptr;

  EventCalled::ChainPtr_t _dummyChain;
  EventCalled::ChainPtr_t &_currentCallChain;
  uint64_t             &_allocCounterRef;
  uint64_t             &_deallocCounterRef;

  uint64_t        const _allocStamp;
  uint64_t        const _deallocStamp;
  time_point      const _start = steady_clock::now();

};

///////////////////////////////////////////////////////////////////////////////////
///
///////////////////////////////////////////////////////////////////////////////////
class EventHdlrState 
{
  friend EventCallContext;

 public:
  using Ptr_t = EventHdlrP_t;

 public:
  explicit EventHdlrState(void *p = NULL); // treat as NULL
  explicit EventHdlrState(EventHdlr_t assigned);

  ~EventHdlrState();

  operator EventHdlr_t() const {
    return *_assignPoint;
  }

  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*a)(int,T_ARG)) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  bool operator!=(TSEventFunc b) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<TSEventFunc>(b);
  }

  void reset_last_assign(EventHdlr_t cbAssign)
  {
    if ( ! _eventChainPtr || _eventChainPtr->empty() ) {
      return;
    }

    _eventChainPtr->back()._assignPoint = &cbAssign;
  }

  EventHdlrState &operator=(nullptr_t);

  EventHdlrState &operator=(TSEventFunc f) 
  {
    assert( _assignPoint );
    assert( ! _assignPoint->_kTSEventFunc || _assignPoint->_kTSEventFunc == f ); // must match 
    return *this;
  }

  // class method-pointer full args
  EventHdlrState &operator=(EventHdlr_t cbAssign)
  {
    _assignPoint = &cbAssign;
    return *this;
  }

  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);

private:
  EventHdlrP_t            _assignPoint;
  EventCalled::ChainPtr_t _eventChainPtr;         // current stored "path"
};

inline EventCallContext::operator EventHdlr_t() const {
  return _state ? _state->operator EventHdlr_t()
                : *_assignPoint;
}


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
                       nullptr,                            \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmphdlr,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmpfunc,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::hdlr, \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::func \
                    }

#define LABEL_FRAME_RECORD(str, name) \
     static auto name =                          \
                    EventHdlrAssignRec{                   \
                       (str+0),                          \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                       nullptr,                            \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmphdlr_nolog,  \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmpfunc,  \
                      &EventHdlrAssignRec::const_cb_callgen_base::hdlr, \
                      &EventHdlrAssignRec::const_cb_callgen_base::func \
                    }

#define CALL_FRAME_RECORD(_h, name) LABEL_FRAME_RECORD(#_h, name)

// define null as "caller" context... and set up next one
#define SET_NEXT_HANDLER_RECORD(_h)                   \
            EVENT_HANDLER_RECORD(_h, kHdlrAssignRec); \
            EventCallContext _ctxt_kHdlrAssignRec{kHdlrAssignRec}

#define NEW_LABEL_FRAME_RECORD(str, name)                   \
            LABEL_FRAME_RECORD(str, const name); \
            EventHdlrState _state_ ## name{name}; \
            EventCallContext _ctxt_ ## name{_state_ ## name, 0}

namespace {

inline std::string trim_to_filename(const char *ptr, const char *name)
{
  std::string label = ptr;
  auto slash = label.rfind('/');
  if ( slash != label.npos ) {
    label.erase(0,slash+1);
  }
  label += "::";
  label += name;
  return std::move(label);
}


}


#define NEW_PLUGIN_FRAME_RECORD(path, symbol, name)              \
            auto label = trim_to_filename(path,symbol);          \
            LABEL_FRAME_RECORD(label.c_str(), const name);       \
            EventHdlrState _state_ ## name{name};            \
            EventCallContext _ctxt_ ## name{_state_ ## name, 0}


#define NEW_CALL_FRAME_RECORD(_h, name)                   \
            CALL_FRAME_RECORD(_h, const name); \
            EventHdlrState _state_ ## name{name}; \
            EventCallContext _ctxt_ ## name{_state_ ## name, 0}


#define RESET_ORIG_FRAME_RECORD(name)   \
            _state_ ## name.reset_last_assign(name)


#define _RESET_LABEL_FRAME_RECORD(str, name)   \
         {                                    \
            LABEL_FRAME_RECORD(str, const name);    \
            _state_ ## name.reset_last_assign(name); \
         }

#define RESET_LABEL_FRAME_RECORD(str, name)   \
            _RESET_LABEL_FRAME_RECORD(str, name)

#define RESET_CALL_FRAME_RECORD(_h, name)   \
            _RESET_LABEL_FRAME_RECORD(#_h, name)


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
#if defined(__cplusplus) && defined(__TS_API_H__) && ! defined(_I_DebugCont_TSAPI_)
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
