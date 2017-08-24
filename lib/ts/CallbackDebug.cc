/** @file

  A brief file description

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this ile except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

/****************************************************************************

  Basic Threads



**************************************************************************/
#include "ts/CallbackDebug.h"

#include "ts/jemallctl.h"
#include "ts/ink_assert.h"
#include "ts/Diags.h"
#include "ts/ink_stack_trace.h"

#include <chrono>
#include <string>
#include <algorithm>
#include <iostream>
#include <memory>
#include <cstring>

namespace {
const ink_thread_key s_currentCallChainTLS = 
  []() {
    ink_thread_key key;
    ink_thread_key_create(&key, nullptr);
    ink_thread_setspecific(key, nullptr);
    return key;
  }();
}

///////////////////////////////////////////////
// common interface impl                     //
///////////////////////////////////////////////

bool EventCalled::no_log() const
{
  if ( ! _hdlrAssign ) {
    return true; // don't log
  }

  // do log if memory changed
  if ( _allocDelta || _deallocDelta ) {
    return false; // DO log
  }

  // if flagged .. it's not important enough
  if (  _hdlrAssign->no_log() ) {
    return true;
  }

  // if no important event was delivered..
  return ( ! _event && ! _delay );
}



void EventCalled::printLog(Chain_t::const_iterator const &obegin, Chain_t::const_iterator const &oend, const char *omsg, const void *ptr)
{
  auto begin = obegin;
  auto last = oend;

  ptrdiff_t memTotal = 0;
  float delayTotal = 0;

  --last;

  // skip constructor/boring callers in print
  while ( begin != last && begin->no_log() ) {
    memTotal += begin->_allocDelta;
    memTotal -= begin->_deallocDelta;
    delayTotal += begin->_delay;

    ++begin;
  }

  ptrdiff_t i = last - begin;

  ptrdiff_t memAccount = 0;
  double timeAccount = 0;

  std::string addonbuff;
  const char *addon = "";

  if ( ptr ) {
    addonbuff.resize(16);
    snprintf(const_cast<char*>(addonbuff.data()),addonbuff.size(),"%p ",ptr);
    addonbuff.erase(0,5);
    addon = addonbuff.data();
  }

  for( auto iter = last ; iter >= begin ; --i, --iter )
  {
    auto &call = *iter;
    auto callingChain = call._callingChain.lock();

    ptrdiff_t memDelta = call._allocDelta;
    memDelta -= call._deallocDelta;

    float delay = call._delay - timeAccount;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    if ( callingChain ) 
    {
      memTotal += memDelta;
      delayTotal += delay;
    }

    const EventHdlrAssignRec &rec = *call._hdlrAssign;

//    const char *e = ( last == begin 1 ? "  " 
//                      : iter == begin ? "~~" 
//                                : ! i ? "!!" 
//                                      : "##" );

    const char *debug = "conttrace           ";
    std::string debugStr = rec._kLabel;

    do {
       auto colon = debugStr.rfind("::");
       if ( colon != debugStr.npos ) { 
         debugStr.erase(colon); // to the end
       }

       auto amp = debugStr.rfind('&');
       if ( amp == debugStr.npos ) {
         break;
       }

       debugStr.erase(0,amp+1); // from the start
       debugStr.insert(0,"cont_");
       debugStr.resize(20,' ');

       debug = debugStr.c_str();
    } while(false);

    std::string msgbuff;
    const char *msg = omsg;

    if ( callingChain ) 
    {
      auto extCaller = call.calling()._hdlrAssign->_kLabel;

      auto extCallerTrail = strrchr(extCaller,'&');
      if ( ! extCallerTrail ) {
         extCallerTrail = strrchr(extCaller,')');
      }
      if ( ! extCallerTrail ) {
        extCallerTrail = extCaller-1;
      }

      ++extCallerTrail;

      msgbuff.resize(strlen(msg) + strlen(extCallerTrail) + 10, '\0');
      snprintf(const_cast<char*>(msgbuff.data()),msgbuff.size(),"<-- %s %s",extCallerTrail,msg);
      msg = msgbuff.data();
    }

    auto callback = strrchr(rec._kLabel,'&');
    if ( ! callback ) {
       callback = strrchr(rec._kLabel,')');
    }
    if ( ! callback ) {
      callback = rec._kLabel-1;
    }

    ++callback;

    if ( last == begin ) 
    {
      Debug(debug,"                 :%05u[ mem %9ld time ~%5.1f%s]                %s %s@%d %s%s",
               call._event, memDelta, delay / div, units, callback, rec._kFile, rec._kLine, addon, msg);
      return;
    }

    if ( ! memAccount ) {
	Debug(debug,"              (%ld):%05u[ mem %9ld time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s%s",
           i, call._event, memDelta, delay / div, units, timeAccount / div, units, 
           callback, rec._kFile, rec._kLine, addon, msg);
    } else {
	Debug(debug,"              (%ld):%05u[ mem %9ld (+%9ld) time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s%s",
           i, call._event, memDelta - memAccount, memAccount, delay / div, units, timeAccount / div, units, 
           callback, rec._kFile, rec._kLine, addon, msg);
    }

    memAccount = memDelta;
    timeAccount = call._delay;

    // account not add if called from other chain ...
    if ( callingChain ) {
      memAccount = 0;
      timeAccount = 0;
    }
  }

  const char *units = ( delayTotal >= 10000 ? "ms" : "us" ); 
  float div = ( delayTotal >= 10000 ? 1000.0 : 1.0 ); 

  Debug("conttrace","                 :____[ mem %9ld time ~%5.1f%s]                         %s%s",
	   memTotal, delayTotal / div, units, addon, omsg);
}

void EventCalled::trim_call(Chain_t &chain)
{
//  if ( ! _assignPoint || this == &chain.front() ) {
  if ( ! _hdlrAssign ) {
    return; // no constructor-calls
  }

  if ( _allocDelta != _deallocDelta ) {
    return; // need no memory lost
  }

//  auto i = this - &chain.front();

  if ( (this-1)->_hdlrAssign != _hdlrAssign && ! no_log() ) {
    return; // go ahead only if a direct-repeat of a boring call
  }

//  if ( no_log() ) {
//   Debug("conttrace","trim #%ld %s", i, _assignPoint->_kLabel);
//  }

//  chain.erase( chain.begin() + i );
}


void EventCallContext::push_incomplete_call(EventHdlr_t rec, int event) const
{
  if ( ! _waiting || _waiting == this ) 
  {
    _chain.push_back( EventCalled(rec,event) );

    if ( ! rec.no_log() && ! _waiting ) {
      Debug("conttrace","starting-push[-->#%lu]: [%05d] %s %s@%d",_chain.size(),
                 event, rec._kLabel, rec._kFile, rec._kLine);
    } else if ( _waiting ) {
      Debug("conttrace","push-call#%lu: %s %s@%d [%d]",_chain.size(),rec._kLabel,rec._kFile,rec._kLine, event);
    }
    return;
  }

  // push record for called frame
  _chain.push_back( EventCalled(*_waiting,rec,event) );
  // push record for calling frame
  _waiting->_chain.push_back( EventCalled(*this) );

//    if ( rec.no_log() ) {
//      return;
//    }
//
//    auto &back = next._callingChain->back();
//    Debug("conttrace","separate-object-push[#%lu->#%lu]: [%05d] %s %s@%d --> [%05d] %s %s@%d", _callingChainLen, _currentCallChain->size(),
//             back._event, back._assignPoint->_kLabel, back._assignPoint->_kFile, back._assignPoint->_kLine,
//             event, _assignPoint->_kLabel, _assignPoint->_kFile, _assignPoint->_kLine);
}

EventCalled::EventCalled(EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _event(event)
{ }


EventCalled::EventCalled(EventCallContext &octxt, EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _callingChain( octxt._chainPtr ),
     _event(event),
     _callingChainLen( octxt._chain.size() )
{ }

EventCalled::EventCalled(EventCallContext &ctxt)
   : _calledChain( ctxt._chainPtr ),
     _calledChainLen( ctxt._chain.size() )
{ }

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlrState &state, int event)
   : _state(&state),
     _chainPtr(state._chainPtr),
     _chain(*state._chainPtr),
     _chainInd(state._chainPtr->size())
{

  if ( ! _waiting ) {
    ink_stack_trace_dump();
  }

  // create entry using current rec
  push_incomplete_call(static_cast<EventHdlr_t>(state), event);
}

thread_local EventCallContext *EventCallContext::st_currentCtxt = nullptr;
thread_local uint64_t         &EventCallContext::st_allocCounterRef = *jemallctl::thread_allocatedp();
thread_local uint64_t         &EventCallContext::st_deallocCounterRef = *jemallctl::thread_deallocatedp();

void EventCallContext::set_ctor_initial_callback(EventHdlr_t rec)
{
  ink_assert( st_currentCtxt );
  st_currentCtxt->_dfltAssignPoint = &rec;
}

EventHdlrState::EventHdlrState(void *p)
   : _chainPtr( std::make_shared<EventCalled::Chain_t>() ),
     _scopeContext( new EventCallContext(*this,0) )
{
  if ( EventCallContext::st_currentCtxt && EventCallContext::st_currentCtxt->_dfltAssignPoint ) 
  {
    _assignPoint = EventCallContext::st_currentCtxt->_dfltAssignPoint;
  }
  Debug("conttrace","%p: init-created: %s %s@%d",this,_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
}

//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr),
     _chainPtr( std::make_shared<EventCalled::Chain_t>() ),
     _scopeContext( new EventCallContext(*this,0) ) // add ctor on to stack!
{
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
  if ( _chainPtr.use_count() > 1 ) {
    return;
  }

  if ( _chainPtr->empty() ) {
    return;
  }

  if ( _chainPtr->size() == 1 && _chainPtr->back().no_log() ) {
    return;
  }

  EventCalled::printLog(_chainPtr->begin(),_chainPtr->end()," [State DTOR]",this);
}

void EventHdlrState::reset_top_frame(EventHdlr_t rec)
{
  EventCallContext::st_currentCtxt = _scopeContext.get();
  XXX return from call (if one matches) and then insert new one
  *this = rec;
}


int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  EventCallContext _ctxt{*this,event};
  EventCallContext::st_currentCtxt = &_ctxt; // reset upon dtor

  if ( _assignPoint->_kTSEventFunc ) {
    // direct C call.. 
    return (*_assignPoint->_kTSEventFunc)(ptr,event,data);
  } 

  // C++ wrapper ...
  return (*_assignPoint->_kWrapFunc_Gen())(ptr,event,data);
}

int
EventHdlrState::operator()(Continuation *self,int event, void *data)
{
  EventCallContext _ctxt{*this,event};
  EventCallContext::st_currentCtxt = &_ctxt; // reset upon dtor

  return (*_assignPoint->_kWrapHdlr_Gen())(self,event,data);
}

//////////////////////////////////////////
// current chain has a record to complete
//    then change to new change if needed..
//////////////////////////////////////////
void EventCallContext::pop_caller_record()
{
  ink_assert( _chain.size() );

  EventCalled &call = static_cast<EventCalled&>(*this);
  unsigned ind = &call - &_chain.front();
  auto &rec = *call._hdlrAssign;

  call.completed(*this, _chainPtr);

  if ( _chainPtr.use_count() <= 1 ) 
  {
    EventCalled::printLog(_chain.begin(),_chain.end()," [pop DTOR]",_state);
    return;
  } 

  call.trim_call(_chain); // snip some if possible
    
  // caller was simply from outside all call chains?
  if ( ind < _chain.size() ) 
  {
    auto msg = ( call._callingChainLen ? "pop" : "top has completed" );
    EventCalled::printLog(_chain.begin()+ind,_chain.end(),msg,_state);
  }

  if ( ! call._callingChainLen ) {
    // returned from *last* entry?
    Debug("conttrace","top-object #%u[%lu]: %s %s %d [evt#%05d] (refs=%ld)",ind, _chain.size(), 
         rec._kLabel,rec._kFile,rec._kLine, 
         call._event, _chainPtr.use_count());
  }

  if ( ind < _chain.size() ) {
    EventCalled::printLog(_chain.begin()+ind,_chain.end(),"pop",_state);
    return;
  }

}

EventCallContext::~EventCallContext()
{
  pop_caller_record();
  if ( st_currentCtxt == this ) {
    st_currentCtxt = _waiting; // don't leave a pointer behind
  }
}

void
EventCalled::completed(EventCallContext const &ctxt, ChainPtr_t const &chain)
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto allocTot = EventCallContext::st_allocCounterRef - ctxt._allocStamp;
  auto deallocTot = EventCallContext::st_deallocCounterRef - ctxt._deallocStamp;

  _allocDelta += allocTot;
  _deallocDelta += deallocTot;
}

const TSEventFunc cb_null_return() { return nullptr; }

void cb_free_stack_context(TSContDebug *p)
{
  auto r = reinterpret_cast<EventCallContext*>(p);
  r->~EventCallContext();
}

const char *cb_alloc_plugin_label(const char *path, const char *symbol)
{
  auto file = strrchr(path,'/');
  if ( ! file ) {
    file = path-1;
  }

  ++file;

  auto symbolTrail = strrchr(symbol,'&');
  if ( ! symbolTrail ) {
     symbolTrail = strrchr(symbol,')');
  }
  if ( ! symbolTrail ) {
    symbolTrail = symbol-1;
  }

  ++symbolTrail;

  return strdup( ( std::string(file) + "::@" + symbol ).c_str() );
}


