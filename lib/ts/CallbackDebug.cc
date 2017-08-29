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
#include "ts/CCallbackDebug.h"

#include "ts/jemallctl.h"
#include "ts/ink_assert.h"
#include "ts/Diags.h"
#include "ts/ink_stack_trace.h"

#include <sstream>
#include <chrono>
#include <string>
#include <algorithm>
#include <iostream>
#include <memory>
#include <cstring>

static_assert( offsetof(EventHdlrAssignRec, _kLabel) == offsetof(EventCHdlrAssignRec, _kLabel), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kFile) == offsetof(EventCHdlrAssignRec, _kFile), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kLine) == offsetof(EventCHdlrAssignRec, _kLine), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kTSEventFunc) == offsetof(EventCHdlrAssignRec, _kCallback), "offset layout mismatch");

#define TRACE_DEBUG_FLAG  "debug_conttrace"
#define TRACE_FLAG  "conttrace"
#define TRACE_FLAG_FIXED  "conttrace           "
#define TRACE_SNPRINTF_PREFIX  "{%#012lx} "
#define TRACE_SNPRINTF_DATA  ink_thread_self(),

std::atomic_uint EventChain::s_ident{1};

///////////////////////////////////////////////
// common interface impl                     //
///////////////////////////////////////////////
EventChain::iterator EventCalled::calling_iterator() const
{ 
  ink_release_assert( has_calling() ); 
  return _callingChain.lock()->begin() + _callingChainLen-1; 
}
EventChain::iterator EventCalled::called_iterator() const
{
  ink_release_assert( _calledChainLen && _calledChain ); 
  return _calledChain->begin() + _calledChainLen-1; 
}

const EventCalled &EventCalled::called() const 
{ 
  return ( _calledChainLen ? *called_iterator() : this[1] );  
}
const EventCalled &EventCalled::calling() const 
{
  return ( ! _callingChain.expired() ? *calling_iterator() : this[-1] );  
}
EventCalled &EventCalled::calling()
{
  return ( ! _callingChain.expired() ? *calling_iterator() : this[-1] );  
}

bool EventCalled::is_no_log() const
{
  if ( ! _hdlrAssign ) {
    return true; // don't log
  }

  // do log if memory actually was increased/decreased 
  if ( _allocDelta != _deallocDelta ) {
    return false; // DO log
  }

  // if flagged .. it's not important enough
  if ( _hdlrAssign->is_no_log() ) {
    return true;
  }

  // if no important event was delivered..
  return ! _event;
}


int EventChain::printLog(std::ostringstream &out, unsigned ibegin, unsigned iend, const char *omsg)
{
  auto obegin = begin() + ibegin;
  auto oend = begin() + std::min(size(),iend+0UL);
  // not even one entry to print?
  if ( obegin == oend ) {
    return 0;
  }

  auto begin = obegin;

  ptrdiff_t chainTotal = _allocTotal - _deallocTotal;
  ptrdiff_t logTotal = 0;
  float delayTotal = 0;

  // skip constructor/boring callers in print
  while ( begin != oend ) 
  {
    if ( ! begin->is_no_log() ) {
      break;
    }

    const EventHdlrAssignRec &rec = *begin->_hdlrAssign;
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) skipping attempt[%u...#%u]: [%05d] %s %s@%d",id(), obegin->_i, begin->_i, 
             begin->_event,  
             rec._kLabel, rec._kFile, rec._kLine);

    logTotal += begin->_allocDelta; // in case not-same
    logTotal -= begin->_deallocDelta; // in case not-same
    delayTotal += begin->_delay; // in case non-zero

    ++begin;
  }

  auto latest = oend;
  --latest;

  ptrdiff_t n = oend - begin;
  ptrdiff_t i = n-1;

  // start from below entry if possible
  if ( latest->_calledChainLen ) 
  {
    n += latest->_calledChain->printLog(out, latest->_calledChainLen-1, ~0U, omsg);
    if ( ! out.str().empty() && begin <= latest ) {
      out << std::endl << "<----"; // called-link entry is separate from earlier ones
    }
  }

  // not even one entry to print?
  if ( ! n ) {
    return 0;
  }

  double timeAccount = 0;

  char buff[256];
  auto iter = latest;

  //
  // loop from latest backwards to begin...
  //
  for( ; iter >= begin ; --i, --iter )
  {
    auto &call = *iter;
    ptrdiff_t memDelta = call._allocDelta;
    memDelta -= call._deallocDelta;

    if ( ! call._delay ) {
      if ( ! out.str().empty() && iter != begin ) {
        out << std::endl << " ..... ";
      }
      continue; // skip incomplete entries                                             /// CONTINUE 
    }

    float delay = call._delay - timeAccount;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    if ( call.has_calling() || iter == begin ) 
    {
      logTotal += memDelta;
      delayTotal += delay;
    }

    const EventHdlrAssignRec &rec = *call._hdlrAssign;

//    const char *e = ( latest == begin 1 ? "  " 
//                      : iter == begin ? "~~" 
//                                : ! i ? "!!" 
//                                      : "##" );

    const char *debug = TRACE_FLAG_FIXED;
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

    std::string callinout;

    if ( call.has_calling() )
    {
      auto extCalling = call.calling()._hdlrAssign->_kLabel;

      auto extCallingTrail = strrchr(extCalling,'&');
      if ( ! extCallingTrail ) {
         extCallingTrail = strrchr(extCalling,')');
      }
      if ( ! extCallingTrail ) {
        extCallingTrail = extCalling-1;
      }

      ++extCallingTrail;

      callinout += std::string() + "<-- " + extCallingTrail + " ";
    } 
    else if ( call._callingChainLen )  // and not "has_calling()"
    {
      callinout += std::string() + "<-- XXXXX ";
    }

    if ( call._calledChainLen ) 
    {
      auto extCalled = call.called()._hdlrAssign->_kLabel;

      auto extCalledTrail = strrchr(extCalled,'&');
      if ( ! extCalledTrail ) {
         extCalledTrail = strrchr(extCalled,')');
      }
      if ( ! extCalledTrail ) {
        extCalledTrail = extCalled-1;
      }

      ++extCalledTrail;

      callinout += std::string() + "--> " + extCalledTrail + " ";
    }

    auto callback = strrchr(rec._kLabel,'&');
    if ( ! callback ) {
       callback = strrchr(rec._kLabel,')');
    }
    if ( ! callback ) {
      callback = rec._kLabel-1;
    }

    std::string eventbuff;
    if ( call._event ) {
      eventbuff.resize(10);
      snprintf(const_cast<char*>(eventbuff.data()),eventbuff.size(),"%05d",call._event);
    } else {
      eventbuff = "     ";
    }

    ++callback;
    if ( latest == begin ) 
    {
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%d:%s[ mem %9ld (+=%+9ld) time ~%5.1f%s]                %s %s@%d %s%s",
               TRACE_SNPRINTF_DATA id(), call._i, eventbuff.c_str(), 
               memDelta, chainTotal, delay / div, units, callback, rec._kFile, rec._kLine, callinout.c_str(), omsg);

      out << std::endl << "(" << debug << ") ";
      out << buff;
      break;                                                                           ///// BREAK
    }

    snprintf(buff,sizeof(buff),
         TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%d:%s[ mem %9ld time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s%s", 
         TRACE_SNPRINTF_DATA id(), call._i, eventbuff.c_str(), 
         memDelta, delay / div, units, timeAccount / div, units,
         callback, rec._kFile, rec._kLine, callinout.c_str(), omsg );

    // only if a not-first call was outward
    if ( call._calledChainLen && iter != latest ) {
      out << std::endl << "---->"; // separate from last one
    }
    out << std::endl << "(" << debug << ") ";
    out << buff;
    // only if a not-last call was inward 
    if ( call._callingChainLen && iter != begin ) {
      out << std::endl << "<----";
    }

    timeAccount = call._delay;

    // account not add if called from other chain ...
    if ( call._callingChainLen ) {
    }
  }

  if ( ! logTotal || n == 1 ) {
    return n;
  }

  const char *units = ( delayTotal >= 10000 ? "ms" : "us" ); 
  float div = ( delayTotal >= 10000 ? 1000.0 : 1.0 ); 

  out << std::endl << "(" << TRACE_DEBUG_FLAG << ")      ";

  snprintf(buff,sizeof(buff),
           TRACE_SNPRINTF_PREFIX "   (C#%06x) [%lu]:_____[ mem %9ld ~~ %+9ld time ~%5.1f%s]                         %s",
           TRACE_SNPRINTF_DATA id(), size(), logTotal, chainTotal, delayTotal / div, units, omsg);
  out << buff;
  return n;
}

bool EventCalled::trim_call() const
{
  if ( ! _hdlrAssign ) {
    return false; // no constructor-calls
  }

  // go ahead only if a direct-repeat of a boring call
  if ( (this-1)->_hdlrAssign == _hdlrAssign 
       && (this-1)->_allocDelta == _allocDelta 
       && (this-1)->_deallocDelta == _deallocDelta ) {
    return true; 
  }

  // not boring ... but memory was spent: trim
  if ( _allocDelta != _deallocDelta ) {
    return false; // need no memory lost
  }

  // not boring ... but no memory spent: trim
  return true;
}

bool EventCalled::trim_call(EventChain &chain) 
{
  if ( chain.size() <= 1 ) {
    return false;
  }

  if ( chain.size() > 100 && ! chain.back().trim_call() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) cannot trim #%ld %s", chain.id(), chain.size()-1, chain.back()._hdlrAssign->_kLabel);
    return false;
  }

  if ( ! chain.back().trim_call() ) {
    return false;
  }

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) trim #%ld %s", chain.id(), chain.size()-1, chain.back()._hdlrAssign->_kLabel);
  return true;
}

void EventCallContext::push_incomplete_call(EventHdlr_t rec, int event) const
{
  auto &chain = *_chainPtr;
  if ( ! _waiting || _waiting == this ) 
  {
    chain.push_back( EventCalled(chain.size(), rec,event) );

    if ( ! rec.is_no_log() && ! _waiting ) {
      Debug(TRACE_DEBUG_FLAG,"starting-push[-->#%lu]: [%05d] %s %s@%d",chain.size(),
                 event, rec._kLabel, rec._kFile, rec._kLine);
    } else if ( _waiting ) {
      Debug(TRACE_DEBUG_FLAG,"push-call#%lu: %s %s@%d [%d]",chain.size(),rec._kLabel,rec._kFile,rec._kLine, event);
    }

    auto &self = active_event();
    ink_release_assert( &self == &chain.back() );
    return;
  }

  auto &ochain = *_waiting->_chainPtr;

  // don't hold in old calls...
  while ( EventCalled::trim_call(ochain) ) 
    { ochain.pop_back(); }

  // push record for calling frame
  ochain.push_back( EventCalled(ochain.size(), ochain.back(), *this) );
  // push record for called frame
  chain.push_back( EventCalled(chain.size(), *_waiting, rec, event) );

  auto &self = chain.back();
  auto &calling = ochain.back();

  const_cast<uint16_t&>(self._callingChainLen) = ochain.size();
  const_cast<uint16_t&>(calling._calledChainLen) = chain.size();

  ink_release_assert( &self == &active_event() );
  ink_release_assert( &calling.called() == &self );
  ink_release_assert( &calling == &self.calling() );
}

EventCalled::EventCalled(unsigned i, EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _i(i),
     _event(event)
{
  // default values
  ink_release_assert(_hdlrAssign);
}


EventCalled::EventCalled(unsigned i, const EventCallContext &octxt, EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _callingChain( octxt._chainPtr ),
     _i(i),
     _event(event),
     _callingChainLen( octxt._chainPtr->size() ) // should be correct (but update it)
{ 
  ink_release_assert(_hdlrAssign);
  Debug(TRACE_DEBUG_FLAG,"@#%d called into (C#%06x) #%d: %s %s@%d", _i, octxt.id(), _callingChainLen-1, assign._kLabel, assign._kFile, assign._kLine);

}

// for caller-only
EventCalled::EventCalled(unsigned i, const EventCalled &prev, const EventCallContext &nctxt)
   : _hdlrAssign( prev._hdlrAssign ), // don't leave null!
     _calledChain( nctxt._chainPtr ),
     _i(i),
     // event is zero
     _calledChainLen( nctxt._chainPtr->size()+1 ) // should be correct (but update it)
{ 
  ink_release_assert(_hdlrAssign);
  Debug(TRACE_DEBUG_FLAG,"@#%d calling from (C#%06x) #%d: %s %s@%d", _i, nctxt.id(), _calledChainLen-1, prev._hdlrAssign->_kLabel, prev._hdlrAssign->_kFile, prev._hdlrAssign->_kLine);
}

EventChain::~EventChain()
{
  std::ostringstream oss;
  printLog(oss,0,~0U," [chain DTOR]");
  oss.str().empty() || ({ DebugSpecific(true,TRACE_FLAG,"pop-dtor %s",oss.str().c_str()); true; });
  if ( front()._hdlrAssign->is_plugin_rec() ) {
    Debug(TRACE_DEBUG_FLAG,"deleting assign-rec %s %s@%d", front()._hdlrAssign->_kLabel, front()._hdlrAssign->_kFile, front()._hdlrAssign->_kLine);
    delete front()._hdlrAssign->_kLabel;
    delete front()._hdlrAssign;
  }
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(const EventHdlrState &state, EventCallContext *octxtp, const EventCalled::ChainPtr_t &chain, int event)
   : _statep(&state), // void* only
     _waiting(octxtp),
     _chainPtr(chain), // shared ownership (if state destructs)
     _chainInd(chain->size()) // one minus new size
{
  // new chain may be empty!

  // create entry using current rec
  push_incomplete_call(static_cast<EventHdlr_t>(state), event);

  ink_release_assert(chain->size()); // now non-zero

  if ( _chainPtr->size() > 100 ) {
    std::ostringstream oss;
    _chainPtr->printLog(oss,0,~0U," [reset-too-long]");
    DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
    ink_fatal("too long!");
  }

  // last point of stamps
  const_cast<ptrdiff_t &>(_allocStamp) = st_allocCounterRef;
  const_cast<ptrdiff_t &>(_deallocStamp) = st_deallocCounterRef;
  const_cast<time_point &>(_start) = steady_clock::now();
}

thread_local EventCallContext *EventCallContext::st_currentCtxt = nullptr;
thread_local uint64_t         &EventCallContext::st_allocCounterRef = *jemallctl::thread_allocatedp();
thread_local uint64_t         &EventCallContext::st_deallocCounterRef = *jemallctl::thread_deallocatedp();

void EventCallContext::set_ctor_initial_callback(EventHdlr_t rec)
{
  ink_release_assert( st_currentCtxt );
  st_currentCtxt->_dfltAssignPoint = &rec;
}

CALL_FRAME_RECORD(null::null, kHdlrAssignEmpty);
CALL_FRAME_RECORD(nodflt::nodflt, kHdlrAssignNoDflt);

namespace {
EventHdlr_t current_default_assign_point()
{
  if ( ! EventCallContext::st_currentCtxt ) {
    return kHdlrAssignEmpty;
  }
  if ( ! EventCallContext::st_currentCtxt->_dfltAssignPoint ) 
  {
    return kHdlrAssignNoDflt;
  }

  return *EventCallContext::st_currentCtxt->_dfltAssignPoint;
}
}

EventHdlrState::EventHdlrState(void *p)
   : _assignPoint(&(current_default_assign_point())),      // must be set before scopeContext ctor
     _scopeContext( new EventCallContext(*this, EventCallContext::st_currentCtxt, std::make_shared<EventChain>(), 0) )
{
  Debug(TRACE_FLAG_FIXED,"(C#%06x) init-created: %s %s@%d",id(),_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
}

//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr),
     _scopeContext( new EventCallContext(*this, EventCallContext::st_currentCtxt, std::make_shared<EventChain>(),0) ) // add ctor on to stack!
{
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
}

void EventHdlrState::reset_top_frame() 
{ 
  void *p = _scopeContext.get(); // store copy of old

  auto waiting = _scopeContext->_waiting;
  auto chainPtr = _scopeContext->_chainPtr;

  // call destructor (and complete return of callback too)
  _scopeContext = nullptr; // prevent overlap alloc-dealloc

  //////////////////////////////////////////////////////////
  // alloc and then record stamps (upon ctor).. with no dealloc
  _scopeContext.reset( new EventCallContext(*this, waiting, chainPtr, 0) ); 

  if ( ! EventCallContext::st_currentCtxt || EventCallContext::st_currentCtxt == p ) {
    EventCallContext::st_currentCtxt = _scopeContext.get();
  }

  // dtor of context
}

bool enter_new_state(EventHdlr_t nhdlr)
{
  bool origProfState = jemallctl::thread_prof_active();

  // from non-frame to frame-rec -> turn off
  if ( origProfState && nhdlr.is_frame_rec() ) {
    jemallctl::disable_thread_prof_active(); 
  } 
  // from frame to non-frame-rec -> turn on
  else if ( ! origProfState && ! nhdlr.is_frame_rec() )
  {
    jemallctl::enable_thread_prof_active();

    const char *debug = TRACE_FLAG_FIXED;
    std::string debugStr = nhdlr._kLabel;

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
       debug = debugStr.c_str();
    } while(false);

    jemallctl::set_thread_prof_name(debug);
  }

  return origProfState;
}

void reset_old_state(bool origProfState, const std::string &origProfName) 
{
  if ( ! origProfName.empty() ) {
    jemallctl::set_thread_prof_name(origProfName);
  }

  if ( origProfState && ! jemallctl::thread_prof_active() ) {
     jemallctl::enable_thread_prof_active();
  } else if ( ! origProfState && jemallctl::thread_prof_active() ) {
     jemallctl::disable_thread_prof_active();
  }
}

int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  auto upctxt = EventCallContext::st_currentCtxt;
  if ( upctxt == _scopeContext.get() ) {
    upctxt = _scopeContext->_waiting; // replace with ctxt above
  }

  // don't hold in old calls as we create a new one
  auto &chain = *_scopeContext->_chainPtr;
  while ( EventCalled::trim_call(chain) ) 
    { chain.pop_back(); }
  ////////// set context for memory check

  auto profState = enter_new_state(*_assignPoint);
  auto profName = ( profState ? jemallctl::thread_prof_name() : "" );

  EventCallContext _ctxt{*this, upctxt, _scopeContext->_chainPtr, event};
  EventCallContext::st_currentCtxt = &_ctxt; // reset upon dtor

  int r = 0;

  ////////// perform call

  if ( _assignPoint->_kTSEventFunc ) {
    // direct C call.. 
    r = (*_assignPoint->_kTSEventFunc)(ptr,event,data);
  } else {
    // C++ wrapper ...
    r = (*_assignPoint->_kWrapFunc_Gen())(ptr,event,data);
  }

  reset_old_state(profState, profName);

  ////////// restore
  return r;
}

int
EventHdlrState::operator()(Continuation *self,int event, void *data)
{
  auto upctxt = EventCallContext::st_currentCtxt;
  if ( upctxt == _scopeContext.get() ) {
    upctxt = _scopeContext->_waiting;
  }

  // don't hold in old calls as we create a new one
  auto &chain = *_scopeContext->_chainPtr;
  while ( EventCalled::trim_call(chain) ) 
    { chain.pop_back(); }

  auto profState = enter_new_state(*_assignPoint);
  auto profName = ( profState ? jemallctl::thread_prof_name() : "" );

  EventCallContext _ctxt{*this, upctxt, _scopeContext->_chainPtr, event};
  EventCallContext::st_currentCtxt = &_ctxt; // reset upon dtor

  auto r = (*_assignPoint->_kWrapHdlr_Gen())(self,event,data);
  reset_old_state(profState, profName);

  return r;
}

EventCallContext::~EventCallContext()
{
  completed(); // completes the current record...

  if ( st_currentCtxt == this ) {
    st_currentCtxt = _waiting; // don't leave a pointer behind
  }
}

void
EventCalled::completed(EventCallContext const &ctxt)
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  ink_release_assert( ! _delay );

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto allocTot = ptrdiff_t() + EventCallContext::st_allocCounterRef - ctxt._allocStamp;
  auto deallocTot = ptrdiff_t() + EventCallContext::st_deallocCounterRef - ctxt._deallocStamp;
  auto waiting = _allocDelta - _deallocDelta;

  auto callingInd = 0;

  // look for a calling record
  if ( has_calling() || _i )
  {
    auto &prev = calling(); // get prev. frame

    // adjust only if the calling record will use the counters above
    if ( ! prev._delay ) 
    {
      callingInd = ( this - &prev == 1 ? -1 : prev._i );
      prev._allocDelta -= allocTot; // adjust for higher stack point
      prev._deallocDelta -= deallocTot; // adjust for higher stack point
    }
  }

  _allocDelta += allocTot;
  _deallocDelta += deallocTot;

  auto &chain = *ctxt._chainPtr;

  chain._allocTotal += _allocDelta;
  chain._deallocTotal += _deallocDelta;

  if ( has_calling() && ! calling().is_frame_rec() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d (=>@#%d): event-complete [%+ld (adj:%+d) -> %+d] [tot:%ld] %s %s@%d",
       ctxt.id(), _i, callingInd, allocTot - deallocTot, waiting, _allocDelta - _deallocDelta, 
       chain._allocTotal - chain._deallocTotal,
       _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine);
    return; // wait on the printlog
  }

  if ( has_calling() && is_frame_rec() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d (=>@#%d): frame-complete [%+ld (adj:%+d) -> %+d] [tot:%ld] %s %s@%d",
       ctxt.id(), _i, callingInd, allocTot - deallocTot, waiting, _allocDelta - _deallocDelta, 
       chain._allocTotal - chain._deallocTotal,
       _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine);
    return;
  }

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d (=>@#%d): top-complete [%+ld (adj:%+d) -> %+d] [tot:%ld] %s %s@%d",
     ctxt.id(), _i, callingInd, allocTot - deallocTot, waiting, _allocDelta - _deallocDelta, 
     chain._allocTotal - chain._deallocTotal,
     _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine);

  std::ostringstream oss;
  // returned from *last* entry?
  chain.printLog(oss,_i,~0U,"[trace]");

  if ( oss.str().empty() ) {
    return;
  }

  char buff[256];
  snprintf(buff,sizeof(buff),
       "\n" TRACE_SNPRINTF_PREFIX " (C#%06x) @#%d[%lu] top-object: %s %s@%d [evt#%05d] (refs=%ld)",
       TRACE_SNPRINTF_DATA chain.id(), _i, chain.size(), _hdlrAssign->_kLabel,_hdlrAssign->_kFile,_hdlrAssign->_kLine, 
       _event, ctxt._chainPtr.use_count());
  oss << buff;

  DebugSpecific(true,TRACE_FLAG,"trace-out %s",oss.str().c_str());
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

  std::string out = std::string() + file + "::@" + symbol;
  Debug(TRACE_DEBUG_FLAG,"create plugin name: %s <--- %s + %s",out.c_str(),path,symbol);

  return strdup(out.c_str());
}

extern "C" {
const TSEventFunc cb_null_return() { return nullptr; }

void cb_set_ctor_initial_callback(EventCHdlrAssignRecPtr_t crec)
{
  EventCallContext::set_ctor_initial_callback(reinterpret_cast<EventHdlr_t>(*crec));
}

}
