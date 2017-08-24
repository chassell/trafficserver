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

struct Continuation;
struct EventHdlrAssignRec;
struct EventHdlrAssignGrp;
struct EventHdlrLogRec;
struct EventHdlrID;

using EventHdlrLog = std::vector<EventHdlrLogRec>;
using EventHdlrChain = std::vector<EventHdlrID>;
using EventHdlrLogPtr = std::shared_ptr<EventHdlrLog>;
using EventHdlrChainPtr = std::shared_ptr<EventHdlrChain>;

using EventHdlrFxn_t       = int(Continuation *, int event, void *data);
using EventHdlrFxnPtr_t    = EventHdlrFxn_t *;
using EventHdlrMethodPtr_t = int (Continuation::*)(int event, void *data);
using EventHdlrCompare_t    = bool(EventHdlrMethodPtr_t);
using EventHdlrFxnGen_t    = EventHdlrFxn_t*(void);
using EventHdlrCompareGen_t = EventHdlrCompare_t *(void);

struct EventHdlrAssignRec 
{
  using Ptr_t = const EventHdlrAssignRec *;
  using Ref_t = const EventHdlrAssignRec &;

  enum { MAX_ASSIGN_COUNTER = 60, MAX_ASSIGNGRPS = 255 };

  EventHdlrAssignGrp    &_assignGrp;      // static-global for compile
  const char *const _label;        // at point of assign
  const char *const _file;         // at point of assign
  uint32_t    const _line:20;      // at point of assign
  uint32_t    const _assignID:8;   // unique for each in compile-object
  const EventHdlrCompareGen_t *_cbCompareGen;   // distinct method-pointer (not usable)
  const EventHdlrFxnGen_t *_cbGenerator;     // ref to custom wrapper function-ptr callable 

public:
  unsigned int id() const { return _assignID; };
  unsigned int grpid() const;

  void set() const;

  template<typename T_FN, T_FN FXN>
  struct gen_hdlrfxn;

  template<typename T_FN, T_FN FXN>
  inline static auto gen_hdlrcompare() -> 
     decltype( reinterpret_cast<EventHdlrMethodPtr_t>(FXN), &std::declval<EventHdlrCompare_t>() )
  {
    return [](EventHdlrMethodPtr_t ofxn) {
       return reinterpret_cast<EventHdlrMethodPtr_t>(FXN) == ofxn;
    };
  }

  template<typename T_FN, T_FN FXN>
  inline static auto gen_hdlrcompare(void) -> 
     decltype( reinterpret_cast<EventHdlrFxnPtr_t>(FXN), &std::declval<EventHdlrCompare_t>() )
  {
    return nullptr;
  }

  bool operator!=(EventHdlrMethodPtr_t b) const { return _cbCompareGen && _cbCompareGen()(b); }
};

class EventHdlrState 
{
  using Ptr_t = EventHdlrAssignRec::Ptr_t;
  using Ref_t = EventHdlrAssignRec::Ref_t;

  Ptr_t             _handlerRec = nullptr; // (cause to change upon first use)
  EventHdlrLogPtr   _handlerLogPtr;        // current stored log
  EventHdlrChainPtr _handlerChainPtr;      // current stored "path"

public:
  int operator()(Continuation *self,int event,void *data);
  int operator()(TSCont,TSEvent,void *data);

  template <class T_OBJ, typename T_ARG>
  bool operator!=(int(T_OBJ::*b)(int,T_ARG)) const {
    return ! _handlerRec || *_handlerRec != reinterpret_cast<EventHdlrMethodPtr_t>(b);
  }

  EventHdlrState &operator=(decltype(nullptr))
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

template<class T_OBJ, typename T_ARG, int(T_OBJ::*FXN)(int,T_ARG)>
struct EventHdlrAssignRec::gen_hdlrfxn<int(T_OBJ::*)(int,T_ARG),FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *arg) {
       return (static_cast<T_OBJ*>(self)->*FXN)(event, static_cast<T_ARG>(arg));
     };
  }
};

struct EventHdlrAssignGrp
{
private:
  static unsigned             s_hdlrAssignGrpCnt;
  static const EventHdlrAssignGrp *s_assignGrps[EventHdlrAssignRec::MAX_ASSIGNGRPS];

public:
  static void                       printLog(void *ptr, EventHdlrLogPtr logPtr, EventHdlrChainPtr chain);
  static EventHdlrAssignRec::Ptr_t lookup(const EventHdlrID &rec);

  EventHdlrAssignGrp();

  unsigned int add_if_unkn(EventHdlrAssignRec::Ref_t rec);

private:
  EventHdlrAssignRec::Ptr_t _assigns[EventHdlrAssignRec::MAX_ASSIGN_COUNTER] = { nullptr };
  unsigned int              _id;
};

struct EventHdlrLogRec
{
  size_t       _allocDelta:32;
  size_t       _deallocDelta:32;

  float        _delay;
};

struct EventHdlrID
{
  unsigned int _assignID:8;
  unsigned int _assignGrpID:8;
  unsigned int _event:16;

  operator EventHdlrAssignRec::Ptr_t() const { return EventHdlrAssignGrp::lookup(*this); }
};


namespace {
EventHdlrAssignGrp s_fileAssignGrp;
}

struct tsapi_cont {};

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
                       s_fileAssignGrp,                    \
                       ((#_h)+0),                          \
                       (__FILE__+0),                       \
                       __LINE__,                           \
                       __COUNTER__,                        \
                      &EventHdlrAssignRec::gen_hdlrcompare<decay_t,(_h)>,  \
                      &EventHdlrAssignRec::gen_hdlrfxn<decay_t,(_h)>::fxn       \
                     };                                    \
     static_assert(name._assignID < EventHdlrAssignRec::MAX_ASSIGN_COUNTER, \
        "cont-targets exceeded file limit")

#define SET_NEXT_HANDLER_RECORD(_h)                   \
            EVENT_HANDLER_RECORD(_h, kHdlrAssignRec); \
            kHdlrAssignRec.set()

#endif

#if defined(__TS_API_H__) && ! defined(_I_DebugCont_API_)
#define _I_DebugCont_API_

template <int (*FXN)(tsapi_cont*, TSEvent, void*) >
struct EventHdlrAssignRec::gen_hdlrfxn<int (*)(tsapi_cont*, TSEvent, void*),FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *data) {
       return (*FXN)(reinterpret_cast<TSCont>(self), static_cast<TSEvent>(event), data);
     };
  }
};

template<void *(*FXN)(void*)>
struct EventHdlrAssignRec::gen_hdlrfxn<void *(*)(void*),FXN>
{
  static EventHdlrFxn_t *fxn(void)
  {
    return [](Continuation *self, int event, void *data) {
      return 0;  // never used
    };
  }
};

#define TSThreadCreate(func,data)                              \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSThreadCreate(func,data);                      \
             })

#define TSContCreate(func,mutexp)                              \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSContCreate(func,mutexp);                      \
             })
#define TSTransformCreate(func,txnp)                           \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSTransformCreate(func,txnp);                 \
             })
#define TSVConnCreate(func,mutexp)                             \
            ({                                                 \
               SET_NEXT_HANDLER_RECORD(func);                  \
               TSVConnCreate(func,mutexp);                     \
             })

#endif
