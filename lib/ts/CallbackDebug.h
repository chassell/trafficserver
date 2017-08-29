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
#if ! defined(__cplusplus) 
#include "ts/CCallbackDebug.h"
#endif

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
  bool operator==(EventHdlrMethodPtr_t a) const 
     { return _kEqualHdlr_Gen && _kEqualHdlr_Gen()(a); }
  bool operator==(TSEventFunc b) const 
     { return _kEqualFunc_Gen && _kEqualFunc_Gen()(b); }

  bool is_no_log() const {
    return ! _kEqualHdlr_Gen();
  }
  bool is_frame_rec() const {
    return _kEqualHdlr_Gen == EventHdlrAssignRec::const_cb_callgen_base::cmphdlr_nolog;
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

///////////////////////////////////////////////////////////////////////////////////
/// Call record to a specific assign-point's handler
///////////////////////////////////////////////////////////////////////////////////
struct EventCalled
{
  struct Chain : public std::vector<EventCalled>
  {
     ~Chain();
  };

  using ChainPtr_t = std::shared_ptr<Chain>;
  using ChainWPtr_t = std::weak_ptr<Chain>;

  // same chain 
  EventCalled(EventHdlr_t assign, int event);

  // create record of calling-in with event
  EventCalled(const EventCallContext &ctxt, EventHdlr_t assign, int event);

  // create record of calling-out (no event)
  EventCalled(const EventCalled &prev, const EventCallContext &ctxt);
  EventCalled(void *p = NULL);

  bool is_no_log() const;
  bool is_no_log_adj() const;
  bool is_frame_rec() const { return _hdlrAssign->is_frame_rec(); }
  bool waiting() const { return _delay != 0.0; }

  static int printLog(std::ostream &out, Chain::const_iterator const &begin, Chain::const_iterator const &end, const char *msg, const void *ptr);
  
  // fill in deltas
  void completed(EventCallContext const &ctxt);
  bool trim_call() const;

  static bool trim_call(Chain &chain); 

  Chain::iterator calling_iterator() const;
  Chain::iterator called_iterator() const;
  const EventCalled &called() const;
  const EventCalled &calling() const;
  EventCalled &calling();

  // upon ctor
  EventHdlrP_t        const _hdlrAssign;   // full callback info 

  // upon ctor
  ChainWPtr_t         const _callingChain;        // chain held by caller's context (from context)
  ChainPtr_t          const _calledChain;         // chain for the next recorded called object

  // upon call
  uint16_t            const _event = 0;            // specific event-int used on CB
  uint8_t             const _callingChainLen = 0; // extern caller's chain-size upon entry (or local)
  uint8_t             const _calledChainLen = 0; // local chain-size upon final CB return

  // upon completion
  int32_t                  _allocDelta = 0;       // actual delta upon return
  int32_t                  _deallocDelta = 0;     // actual delta upon return
  float                    _delay = 0;            // total lapsed time [0.0 only if incomplete]
};



///////////////////////////////////////////////////////////////////////////////////
/// Context on stack as ctor-dtor pair of calls
///////////////////////////////////////////////////////////////////////////////////
struct EventCallContext
{
  using UPtr_t = std::unique_ptr<EventCallContext>;

  using steady_clock = std::chrono::steady_clock;
  using time_point = steady_clock::time_point;

 public:
  // HdlrState can record this event occurring
  EventCallContext(const EventCallContext &ctxt) = delete;
  EventCallContext(const EventHdlrState &state, EventCallContext *octxtp, const EventCalled::ChainPtr_t &chain, int event);
  ~EventCallContext();

  // report handler that this context has in place
  EventCalled       &active_event()       { return _chainPtr->operator[](_chainInd); };
  EventCalled const &active_event() const { return _chainPtr->operator[](_chainInd); };

  void reset_top_frame(EventHdlr_t hdlr);
  void completed() { active_event().completed(*this); }
 public:
  static void set_ctor_initial_callback(EventHdlr_t hdlr);

 private:
  void push_incomplete_call(EventHdlr_t rec, int event) const;
  void pop_caller_record();

 public:
  static thread_local EventCallContext        *st_currentCtxt;
  static thread_local uint64_t                &st_allocCounterRef;
  static thread_local uint64_t                &st_deallocCounterRef;

 public:
  const void              *const _statep = nullptr;
  EventCallContext        *const _waiting = st_currentCtxt;

  EventHdlrP_t                   _dfltAssignPoint = nullptr;

  EventCalled::ChainPtr_t const _chainPtr; // fixed upon creation
//  EventCalled::Chain         &_chain;
  unsigned                const _chainInd; // note: iterators can be invalidated

  ptrdiff_t               const _allocStamp = 0; // reset when ctor is done
  ptrdiff_t               const _deallocStamp = 0; // reset when ctor is done
  time_point              const _start;
};



///////////////////////////////////////////////////////////////////////////////////
/// Context for a single callback-assignable object ... within one module/purpose
///////////////////////////////////////////////////////////////////////////////////
class EventHdlrState 
{
  friend EventCallContext;

 public:
  // default initializer (ptr ignored)
  explicit EventHdlrState(void *p = NULL); // treat as NULL

  // init with a handler-rec
  explicit EventHdlrState(EventHdlr_t assigned);
  EventHdlrState(const EventHdlrState &state) = delete;

  ~EventHdlrState();

 public:
  void reset_top_frame();

  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);

 public:
  operator EventHdlr_t() const { return *_assignPoint; }
  operator EventHdlrP_t() const { return _assignPoint; }
  operator EventCalled::ChainPtr_t() const { return _scopeContext->_chainPtr; }

  // allow comparisons with EventHdlrs
  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*a)(int,T_ARG)) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  // allow comparisons with TSEventFuncs
  bool operator!=(TSEventFunc b) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<TSEventFunc>(b);
  }

  // allow comparisons with EventHdlrs
  template <class T_OBJ, typename T_ARG>
  bool operator==(int(T_OBJ::*a)(int,T_ARG)) const {
    return _assignPoint && *_assignPoint == reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  // allow comparisons with TSEventFuncs
  bool operator==(TSEventFunc b) const {
    return _assignPoint && *_assignPoint == reinterpret_cast<TSEventFunc>(b);
  }

  void operator=(nullptr_t) = delete;

  void operator=(TSEventFunc f);

  // class method-pointer full args
  void operator=(EventHdlr_t cbAssign) { _assignPoint = &cbAssign; }

  // class method-pointer full args
  void operator=(const EventHdlrState &orig)
  {
    _assignPoint = orig._assignPoint;
    // all other values can remain intact
  }

private:
  EventHdlrP_t                  _assignPoint = nullptr;  // latest callback assigned for use
  EventCallContext::UPtr_t      _scopeContext; // call-record after full alloc
};

inline void EventHdlrState::operator=(TSEventFunc f) 
{
  assert( _assignPoint );
  assert( ! _assignPoint->_kTSEventFunc || _assignPoint->_kTSEventFunc == f ); // must match 
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

const char *cb_alloc_plugin_label(const char *path, const char *symbol);

// define null as "caller" context... and set up next one
#define SET_NEXT_HANDLER_RECORD(_h)                   \
          {                                                               \
            EVENT_HANDLER_RECORD(_h, kHdlrAssignRec);                     \
            EventCallContext::set_ctor_initial_callback(kHdlrAssignRec);  \
          }

#define NEW_LABEL_FRAME_RECORD(str, name)          \
            LABEL_FRAME_RECORD(str, const name);   \
            EventHdlrState _state_ ## name{name};  \
            _state_ ## name.reset_top_frame()

#define NEW_PLUGIN_FRAME_RECORD(path, symbol, name)                            \
            LABEL_FRAME_RECORD(cb_alloc_plugin_label(path,symbol),const name); \
            EventHdlrState _state_ ## name{name};                              \
            _state_ ## name.reset_top_frame()

#define NEW_CALL_FRAME_RECORD(_h, name)                \
            CALL_FRAME_RECORD(_h, const name);         \
            EventHdlrState _state_ ## name{name};      \
            _state_ ## name.reset_top_frame()

#define RESET_ORIG_FRAME_RECORD(name)   \
          _state_ ## name = name;                 \
          _state_ ## name.reset_top_frame();  \


#define _RESET_LABEL_FRAME_RECORD(str, name)       \
         {                                         \
            LABEL_FRAME_RECORD(str, const name);   \
            RESET_ORIG_FRAME_RECORD(name);         \
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


// thread functs should not be wrapped currently at all
template <TSThreadFunc FXN>
struct EventHdlrAssignRec::const_cb_callgen<TSThreadFunc,FXN>
   : public const_cb_callgen_base
{
};

#endif
