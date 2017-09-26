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

#ifndef _I_CallbackDebug_h_
#define _I_CallbackDebug_h_

#include "ts/apidefs.h" // for TSCont and TSEventFunc
#include "ts/ink_mutex.h"
#include "ts/ink_error.h"

#include <atomic>
#include <vector>
#include <memory>
#include <type_traits>
#include <cstddef>
#include <chrono>
#include <cassert>

struct Continuation;
struct EventHdlrAssignRec;
struct EventCalled;
struct EventChain;
struct EventCallContext;
class EventHdlrState;

using EventHdlr_t = const EventHdlrAssignRec &;
using EventHdlrP_t = const EventHdlrAssignRec *;

using EventHdlrMethodPtr_t = int (Continuation::*)(int event, void *data);
using EventHdlrFxn_t       = int(Continuation *, int event, void *data);
using EventFuncFxn_t     = int(TSCont contp, TSEvent event, void *edata);

using EventHdlrFxnPtr_t    = EventHdlrFxn_t *;
using EventFuncFxnPtr_t    = EventFuncFxn_t *;

using EventHdlrCompare_t    = bool(EventHdlrMethodPtr_t);
using EventFuncCompare_t    = bool(EventFuncFxnPtr_t);

using EventHdlrFxnGen_t    = EventHdlrFxnPtr_t(void);
using EventFuncFxnGen_t    = EventFuncFxnPtr_t(void);
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
      { return [](EventHdlrMethodPtr_t) -> bool { assert(0); return false; }; }
    static EventFuncCompare_t *cmpfunc(void)
      { assert(0); return [](EventFuncFxnPtr_t) -> bool { assert(0); return false; }; }
    static EventHdlrFxnPtr_t hdlr(void)
      { assert(0); return [](Continuation *, int, void *) { assert(0); return 0; }; }
    static EventFuncFxnPtr_t func(void)
      { assert(0); return [](TSCont, TSEvent, void *) { assert(0); return 0; }; }
  };

 public:
  using Ptr_t = const EventHdlrAssignRec *;

  bool operator!=(EventHdlrMethodPtr_t a) const 
     { return ! _kEqualHdlr_Gen || ! _kEqualHdlr_Gen()(a); }
  bool operator!=(EventFuncFxnPtr_t b) const 
     { return ! _kEqualFunc_Gen || ! _kEqualFunc_Gen()(b); }

  template <class T_OBJ, typename T_DATA>
  bool operator==(int (T_OBJ::*a)(int event, T_DATA data)) const 
     { return _kEqualHdlr_Gen && _kEqualHdlr_Gen()(reinterpret_cast<EventHdlrMethodPtr_t>(a)); }

  bool operator==(EventFuncFxnPtr_t b) const 
     { return _kEqualFunc_Gen && _kEqualFunc_Gen()(b); }

  bool is_no_log() const {
    return ! _kEqualHdlr_Gen();
  }
  bool is_frame_rec() const {
    return _kLabel[0] == '#';
  }
  bool is_plugin_rec() const {
    return strstr(_kLabel,"::@"); // slow.. but called rarely
  }

 public:
  const char *const _kLabel;   // at point of assign
  const char *const _kFile;    // at point of assign
  uint32_t    const _kLine;    // at point of assign

  EventFuncFxnPtr_t const _kTSEventFunc; // direct callback ptr (from C)

  EventHdlrCompareGen_t *const _kEqualHdlr_Gen; // distinct method-pointer (not usable)
  EventFuncCompareGen_t *const _kEqualFunc_Gen; // distinct method-pointer (not usable)

  EventHdlrFxnGen_t     *const _kWrapHdlr_Gen; // ref to custom wrapper function-ptr callable 
  EventFuncFxnGen_t     *const _kWrapFunc_Gen; // ref to custom wrapper function-ptr callable 

# if (GCC_VERSION >= 6000)
 private:
  EventHdlrAssignRec() = delete;
  EventHdlrAssignRec(const EventHdlrAssignRec &) = delete;
#endif
};

extern const EventHdlrAssignRec &kHdlrAssignEmpty;
extern const EventHdlrAssignRec &kHdlrAssignNoDflt;

///////////////////////////////////////////////////////////////////////////////////
/// Call record to a specific assign-point's handler
///////////////////////////////////////////////////////////////////////////////////
struct EventCalled
{
  using ChainIter_t = std::vector<EventCalled>::iterator;
  using ChainPtr_t = std::shared_ptr<EventChain>;
  using ChainWPtr_t = std::weak_ptr<EventChain>;

  EventCalled(EventCalled &&old) = default;

  // same chain 
  EventCalled(unsigned i, EventHdlr_t assign, int event);

  // create record of calling-in with event
  EventCalled(unsigned i, const EventCallContext &ctxt, EventHdlr_t assign, int event);

  // create record of calling-out (no event)
  EventCalled(unsigned i, EventHdlr_t prevHdlr, const EventCallContext &ctxt, int event);

  bool is_no_log_mem() const;
  bool is_no_log() const;
  bool is_frame_rec() const { return _hdlrAssign->is_frame_rec(); }
  bool waiting() const { return _delay != 0.0; }

  // fill in deltas
  void completed(EventCallContext const &ctxt, const char *msg);
  bool trim_check() const;
  bool trim_back_prep();

  ChainIter_t calling_iterator() const;
  ChainIter_t called_iterator() const;
  const EventCalled &called() const;
  const EventCalled &calling() const;
  EventCalled &calling();

  bool has_calling() const { return ! _callingChain.expired() && _callingChainLen; }
  bool has_called() const { return _calledChain && _calledChainLen; }

  // upon ctor
  EventHdlrP_t        const _hdlrAssign = &kHdlrAssignEmpty;          // full callback struct 

  // upon ctor
  ChainWPtr_t         _callingChain;        // chain held by caller's context (from context)
  ChainPtr_t          _calledChain;         // chain for the next recorded called object

  // upon call
  uint16_t            const _i; // extern caller's chain-size upon entry (or local)
  uint16_t            const _event = 0;            // specific event-int used on CB
  uint16_t            const _callingChainLen = 0; // extern caller's chain-size upon entry (or local)
  uint16_t            const _calledChainLen = 0; // local chain-size upon final CB return

  // upon completion
  int64_t                  _allocDelta = 0;       // actual delta upon return
  int64_t                  _deallocDelta = 0;     // actual delta upon return
  float                    _delay = 0;            // total lapsed time [0.0 only if incomplete]

 private:
  EventCalled() = delete;
  EventCalled(const EventCalled &old) = delete;
};



struct EventChain : public std::vector<EventCalled>
{
   EventChain();
   EventChain(uint32_t oid, uint16_t cnt);
   ~EventChain();

   unsigned id() const { return _id; }
   unsigned oid() const { return _oid; }

   bool trim_check();
   bool trim_back(); 

   static std::atomic_uint s_ident;

   ink_mutex _owner = PTHREAD_MUTEX_INITIALIZER; // created for start
   int64_t _allocTotal = 0ULL;
   int64_t _deallocTotal = 0ULL;

   const uint32_t _id = s_ident++;
   const uint32_t _oid = 0;

   uint16_t _cnt = 0; // loop over as needed

   int printLog(std::ostringstream &out, unsigned ibegin, unsigned iend, const char *msg);

  private:
   EventChain(const EventChain &) = delete;
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
  explicit EventCallContext();
  EventCallContext(EventHdlr_t hdlr, const EventCalled::ChainPtr_t &chain, int event);
  ~EventCallContext();

  unsigned id() const { return _chainPtr->id(); }
  unsigned oid() const { return _chainPtr->oid(); }

  // report handler that this context has in place
  bool               has_active() const { return _chainInd < _chainPtr->size(); }
  bool               is_incomplete() const { return _chainInd < _chainPtr->size() && ! _chainPtr->operator[](_chainInd)._delay; }
  EventCalled       &active_event()       { return _chainPtr->operator[](std::min(_chainInd+0UL,_chainPtr->size()-1)); };
  EventCalled const &active_event() const { return _chainPtr->operator[](std::min(_chainInd+0UL,_chainPtr->size()-1)); };

  EventHdlr_t active_hdlr() const { return *active_event()._hdlrAssign; }
  EventHdlrP_t active_hdlrp() const { return active_event()._hdlrAssign; }

  EventCalled::ChainPtr_t create_thread_jump_chain(EventHdlr_t hdlr, int event);
  void reset_frame(EventHdlr_t hdlr);
  bool complete_call(const char *msg);
  bool restart_upper_stamp(const char *msg);

  static bool remove_mem_delta(int id, int32_t adj);
  static bool remove_mem_delta(const char *str, int32_t adj);
  bool reverse_mem_delta(int32_t adj) { return reverse_mem_delta("<self>",adj); }
  bool reverse_mem_delta(const char *str, int32_t adj);
 public:
  static void set_ctor_initial_callback(EventHdlr_t hdlr);
  static void clear_ctor_initial_callback();

 private:
  void push_call_chain_pair(EventHdlr_t rec, int event);
  void push_call_entry(EventHdlr_t rec, int event);
  void push_initial_call(EventHdlr_t rec);

 public:
  static thread_local EventCallContext        *st_currentCtxt;
  static thread_local EventHdlrP_t            st_dfltAssignPoint;
  static thread_local uint64_t                &st_allocCounterRef;
  static thread_local uint64_t                &st_deallocCounterRef;

 public:
  EventCallContext        *const _waiting = st_currentCtxt;

  EventCalled::ChainPtr_t       _chainPtr; // fixed upon creation
  unsigned                      _chainInd = ~0U; // because iterators can be invalidated

  uint64_t                const _allocStamp = ~0ULL; // reset when ctor is done
  uint64_t                const _deallocStamp = ~0ULL; // reset when ctor is done
  time_point              const _start;

 private:
  EventCallContext(const EventCallContext &ctxt) = delete;
};



///////////////////////////////////////////////////////////////////////////////////
/// Context for a single callback-assignable object ... within one module/purpose
///////////////////////////////////////////////////////////////////////////////////
class EventHdlrState 
{
  friend EventCallContext;

 public:
  // default initializer to catch earliest allocation stamps
  explicit EventHdlrState(void *p = NULL);
  // init with a handler-rec
  explicit EventHdlrState(EventHdlr_t assigned);

  ~EventHdlrState();

 public:
  operator EventHdlr_t() const { return *_assignPoint; }
  operator EventHdlrP_t() const { return _assignPoint; }
  operator const EventCalled::ChainPtr_t&() const { return _scopeContext._chainPtr; }

  unsigned id() const { return _scopeContext.id(); }
  unsigned oid() const { return _scopeContext.oid(); }

  // allow comparisons with EventHdlrs
  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*a)(int,T_ARG)) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  // allow comparisons with EventFuncFxnPtr_t
  bool operator!=(EventFuncFxnPtr_t b) const {
    return ! _assignPoint || *_assignPoint != reinterpret_cast<EventFuncFxnPtr_t>(b);
  }

  // allow comparisons with EventHdlrs
  template <class T_OBJ, typename T_ARG>
  bool operator==(int(T_OBJ::*a)(int,T_ARG)) const {
    return _assignPoint && *_assignPoint == reinterpret_cast<EventHdlrMethodPtr_t>(a);
  }

  // allow comparisons with EventFuncFxnPtr_t
  bool operator==(EventFuncFxnPtr_t b) const {
    return _assignPoint && *_assignPoint == reinterpret_cast<EventFuncFxnPtr_t>(b);
  }

  void operator=(nullptr_t) = delete;

  void operator=(EventFuncFxnPtr_t f);

  // class method-pointer full args
  void operator=(EventHdlr_t cbAssign) { _assignPoint = &cbAssign; }

  // class method-pointer full args
  void operator=(const EventHdlrState &orig)
  {
    _assignPoint = orig._assignPoint;
    // all other values can remain intact
  }

 public:
  static void reset_curr_frame(EventHdlr_t hdlr)
  {
    assert( EventCallContext::st_currentCtxt );
    EventCallContext::st_currentCtxt->reset_frame(hdlr);
  }

  static void allocate_hook(void *p, unsigned n)
  {
    st_preAllocCounter = EventCallContext::st_allocCounterRef - n;
    st_preCtorObject = p;
    st_preCtorObjectEnd = static_cast<char *>(p) + n;
  }

  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);

private:
  void remove_ctor_delta();

  static thread_local uint64_t st_preAllocCounter;
  static thread_local void *st_preCtorObject;
  static thread_local void *st_preCtorObjectEnd;

  EventHdlrP_t     _assignPoint = nullptr;  // latest callback assigned for use
  EventCallContext _scopeContext; // call-record after full alloc

  EventHdlrState(const EventHdlrState &state) = delete;
};

inline void EventHdlrState::operator=(EventFuncFxnPtr_t f) 
{
  assert( _assignPoint );
  assert( ! _assignPoint->_kTSEventFunc || _assignPoint->_kTSEventFunc == f ); // must match 
}

extern "C" {
const char *cb_alloc_plugin_label(const char *path, const char *symbol);
int64_t     cb_get_thread_alloc_surplus();
void        cb_allocate_hook(void *,unsigned n);
void        cb_remove_mem_delta(const char *, unsigned n);
}

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

int
inline EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
//  auto profState = enter_new_state(*_assignPoint);
//  auto profName = ( profState ? jemallctl::thread_prof_name() : "" );

  EventCallContext _ctxt{*this, _scopeContext._chainPtr, event};
  EventCallContext::st_currentCtxt = &_ctxt; // reset upon dtor

  // XXX thread conflict here XXX
  _scopeContext._chainPtr = _ctxt._chainPtr; // detach if thread-unsafe!

  int r = 0;

  ////////// perform call

  if ( _assignPoint->_kTSEventFunc ) {
    // direct C call.. 
    r = (*_assignPoint->_kTSEventFunc)(ptr,event,data);
  } else {
    // C++ wrapper ...
    r = (*_assignPoint->_kWrapFunc_Gen())(ptr,event,data);
  }

//  reset_old_state(profState, profName);

  ////////// restore
  return r;
}

int
inline EventHdlrState::operator()(Continuation *self,int event, void *data)
{
//  auto profState = enter_new_state(*_assignPoint);
//  auto profName = ( profState ? jemallctl::thread_prof_name() : "" );

  auto r = 0;

  {

  EventCallContext ctxt{*this, _scopeContext._chainPtr, event};
  EventCallContext::st_currentCtxt = &ctxt; // reset upon dtor

  _scopeContext._chainPtr = ctxt._chainPtr; // detach if thread-unsafe!

  r = (*_assignPoint->_kWrapHdlr_Gen())(self,event,data);
  }

//  reset_old_state(profState, profName);

  return r;
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

#define EVENT_HANDLER(_h) \
  *({                                                         \
     using decay_t = typename std::decay<decltype(_h)>::type; \
     static constexpr EventHdlrAssignRec _kHdlrAssignRec_{  \
                       ((#_h)+0),                          \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                       nullptr,                            \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmphdlr,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::cmpfunc,  \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::hdlr,     \
                      &EventHdlrAssignRec::const_cb_callgen<decay_t,(_h)>::func      \
                    };                                                               \
     &_kHdlrAssignRec_;                                                             \
  })

#define LABEL_FRAME_GLOBAL(str,name) \
     constexpr const EventHdlrAssignRec name{  \
                       (("#" str)+0),                        \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                       nullptr,                            \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmphdlr,  \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmpfunc,  \
                      &EventHdlrAssignRec::const_cb_callgen_base::hdlr, \
                      &EventHdlrAssignRec::const_cb_callgen_base::func \
                    }

#define ALLOC_PLUGIN_FRAME(path,symbol) \
    *(                                                                     \
     new EventHdlrAssignRec{                                             \
                       cb_alloc_plugin_label(path,symbol),                \
                       (__FILE__+0),                                      \
                       __LINE__,                                          \
                       nullptr,                                           \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmphdlr, \
                      &EventHdlrAssignRec::const_cb_callgen_base::cmpfunc, \
                      &EventHdlrAssignRec::const_cb_callgen_base::hdlr, \
                      &EventHdlrAssignRec::const_cb_callgen_base::func \
                    }                                                  \
    )

#define LABEL_FRAME(str) *({ static LABEL_FRAME_GLOBAL(str,_kLabelAssignRec_); &_kLabelAssignRec_; })
#define CALL_FRAME(_h)    LABEL_FRAME(#_h)

#define NAMED_CALL_FRAME(name,_h) \
   LABEL_FRAME_GLOBAL(#_h,name)

// define null as "caller" context... and set up next one
#define NEW_LABEL_FRAME(str, name)          \
            EventHdlrState _state_ ## name(LABEL_FRAME(str))

#define NEW_PLUGIN_FRAME(path, symbol, name)   \
            EventHdlrState _state_ ## name(ALLOC_PLUGIN_FRAME(path,symbol))

#define NEW_CALL_FRAME(_h, name)                \
            EventHdlrState _state_ ## name(CALL_FRAME(_h))

#define RESET_LABEL_FRAME(str) \
            EventHdlrState::reset_curr_frame(LABEL_FRAME(str))

#define RESET_CALL_FRAME(_h, name)   \
            EventHdlrState::reset_curr_frame(CALL_FRAME(_h))

#define CREATE_EVENT_FRAME(str, chain)  \
         EventCallContext ctxt(LABEL_FRAME(str), (chain), 0)


#define LOG_SKIPPABLE_EVENTHDLR(_h) \
  template <>                                                               \
  EventHdlrCompare_t *EventHdlrAssignRec::const_cb_callgen<decltype(_h),(_h)>::cmphdlr() \
     { return nullptr; }

#define SET_NEXT_HANDLER(_h)                         \
            EventCallContext::set_ctor_initial_callback(EVENT_HANDLER(_h))

class INKContInternal;
class INKVConnInternal;
class HttpSM;
class Event;
class Continuation;

#endif // _I_CallbackDebug_h_
