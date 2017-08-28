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

///////////////////////////////////////////////
// common interface impl                     //
///////////////////////////////////////////////
EventCalled::Chain::iterator EventCalled::calling_iterator() const
{ 
  ink_release_assert( _callingChainLen && ! _callingChain.expired() ); 
  return _callingChain.lock()->begin() + _callingChainLen-1; 
}
EventCalled::Chain::iterator EventCalled::called_iterator() const
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
  return ( _callingChainLen ? *calling_iterator() : this[-1] );  
}


bool EventCalled::is_no_log_adj() const
{
  if ( ! _hdlrAssign ) {
    return true; // don't log
  }

  auto &calledEvt = called();

  if ( calledEvt._allocDelta != calledEvt._deallocDelta ) {
    const EventHdlrAssignRec &rec = *calledEvt._hdlrAssign;
    Debug(TRACE_DEBUG_FLAG,"check skip-mem adjustment: [%05d] %s %s@%d",
              _event, rec._kLabel, rec._kFile, rec._kLine);
  }

  // do log if memory actually was increased/decreased 
  if ( _allocDelta - calledEvt._allocDelta != _deallocDelta - calledEvt._deallocDelta ) {
    return false; // DO log
  }

  // if flagged .. it's not important enough
  if ( _hdlrAssign->is_no_log() ) {
    return true;
  }

  // if no important event was delivered..
  return ! _event;
}

bool EventCalled::is_no_log() const
{
  if ( ! _hdlrAssign ) {
    return true; // don't log
  }

  // if flagged .. it's not important enough
  if ( _hdlrAssign->is_no_log() ) {
    return true;
  }

  // if no important event was delivered..
  return ! _event;
}


int EventCalled::printLog(std::ostream &out, Chain::const_iterator const &obegin, Chain::const_iterator const &oend, const char *omsg, const void *ptr)
{
  // not even one entry to print?
  if ( obegin == oend ) {
    return 0;
  }

  auto begin = obegin;

  ptrdiff_t memTotal = 0;
  float delayTotal = 0;

/*
  const EventHdlrAssignRec &beginrec = *begin->_hdlrAssign;
  Debug(TRACE_DEBUG_FLAG,"log-start attempt[-->#%lu]: nolog?=%d%d [%05d] %s %s@%d",begin - obegin, 
                       ( begin + 1 < oend ? begin->is_no_log_adj() : begin->is_no_log() ), 
                       begin->is_no_log(), 
                       begin->_event,  beginrec._kLabel, beginrec._kFile, beginrec._kLine);
*/

  // skip constructor/boring callers in print
  while ( begin != oend ) 
  {
    if ( ! begin->is_no_log() ) {
      break;
    }

    // check if no memory
    if ( begin + 1 != oend && ! begin->is_no_log_adj() ) {
      break;
    }

    const EventHdlrAssignRec &rec = *begin->_hdlrAssign;
    Debug(TRACE_DEBUG_FLAG,"skipping attempt[-->#%lu]: [%05d] %s %s@%d",begin - obegin, 
             begin->_event,  rec._kLabel, rec._kFile, rec._kLine);

    memTotal += begin->_allocDelta;
    memTotal -= begin->_deallocDelta;
    delayTotal += begin->_delay;

    ++begin;
  }

  auto last = oend;
  --last;

  ptrdiff_t n = oend - begin;
  ptrdiff_t i = n-1;

  // start from below entry if possible
  if ( last->_calledChainLen ) 
  {
    n += printLog(out,last->called_iterator(), last->_calledChain->end(), omsg, ptr);
  }

  // not even one entry to print?
  if ( ! n ) {
    return 0;
  }

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

  char buff[256];
  auto iter = last;

  //
  // loop from last backwards to begin...
  //
  for( ; iter >= begin ; --i, --iter )
  {
    auto &call = *iter;
    ptrdiff_t memDelta = call._allocDelta;
    memDelta -= call._deallocDelta;

    if ( ! call._delay ) {
      continue;
    }

    float delay = call._delay - timeAccount;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    if ( call._callingChainLen || iter == begin ) 
    {
      memTotal += memDelta;
      delayTotal += delay;
    }

    const EventHdlrAssignRec &rec = *call._hdlrAssign;

//    const char *e = ( last == begin 1 ? "  " 
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

    if ( call._callingChainLen && call._callingChain.expired() ) 
    {
      callinout += std::string() + "<-- XXXXX ";
    }
    else if ( call._callingChainLen ) 
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

    ++callback;
    out << std::endl << "(" << debug << ") ";

    if ( last == begin ) 
    {
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "                 :%5s[ mem %9ld time ~%5.1f%s]                %s %s@%d %s%s%s\n ---- ",
               TRACE_SNPRINTF_DATA ( call._event ? std::to_string(call._event).c_str() : "" ), 
               memDelta, delay / div, units, callback, rec._kFile, rec._kLine, addon, callinout.c_str(), omsg);
      out << buff;
      return 1;
    }

    if ( ! memAccount ) {
      snprintf(buff,sizeof(buff),
           TRACE_SNPRINTF_PREFIX "              (%ld):%5s[ mem %9ld              time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s%s%s", 
           TRACE_SNPRINTF_DATA i, ( call._event ? std::to_string(call._event).c_str() : "" ), 
           memDelta, delay / div, units, timeAccount / div, units, 
           callback, rec._kFile, rec._kLine, addon, callinout.c_str(), omsg );
    } else {
      snprintf(buff,sizeof(buff),
           TRACE_SNPRINTF_PREFIX "              (%ld):%5s[ mem %9ld (+%9ld) time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s%s%s", 
           TRACE_SNPRINTF_DATA i,( call._event ? std::to_string(call._event).c_str() : "" ), 
           memDelta, memAccount, delay / div, units, timeAccount / div, units, 
           callback, rec._kFile, rec._kLine, addon, callinout.c_str(), omsg );
    }

    out << buff;

    memAccount = memDelta;
    timeAccount = call._delay;

    // account not add if called from other chain ...
    if ( call._callingChainLen ) {
      Debug(TRACE_DEBUG_FLAG,"no adjustment needed[-->#%lu]",i);
      memAccount = 0;
      timeAccount = 0;
    }
  }

  if ( ! memTotal || n == 1 ) {
    return n;
  }

  const char *units = ( delayTotal >= 10000 ? "ms" : "us" ); 
  float div = ( delayTotal >= 10000 ? 1000.0 : 1.0 ); 

  out << std::endl << "(" << TRACE_DEBUG_FLAG << ")     ";

  snprintf(buff,sizeof(buff),
           TRACE_SNPRINTF_PREFIX "            (n=%ld):_____[ mem %9ld time ~%5.1f%s]                         %s%s\n ----",
           TRACE_SNPRINTF_DATA n, memTotal, delayTotal / div, units, addon, omsg);
  out << buff;
  return n;
}

bool EventCalled::trim_call() const
{
//  if ( ! _assignPoint || this == &chain.front() ) {
  if ( ! _hdlrAssign ) {
    return false; // no constructor-calls
  }

  if ( _allocDelta != _deallocDelta ) {
    return false; // need no memory lost
  }

  if ( (this-1)->_hdlrAssign != _hdlrAssign && ! is_no_log() ) {
    return false; // go ahead only if a direct-repeat of a boring call
  }

  return true;
}

bool EventCalled::trim_call(EventCalled::Chain &chain) 
{ 
  if ( chain.size() <= 1 || ! chain.back().trim_call() ) {
    return false;
  }
  Debug(TRACE_DEBUG_FLAG,"trim #%ld %s", chain.size()-1, chain.back()._hdlrAssign->_kLabel);
  return true;
}

void EventCallContext::push_incomplete_call(EventHdlr_t rec, int event) const
{
  if ( ! _waiting || _waiting == this ) 
  {
    _chain.push_back( EventCalled(rec,event) );

    if ( ! rec.is_no_log() && ! _waiting ) {
      Debug(TRACE_DEBUG_FLAG,"starting-push[-->#%lu]: [%05d] %s %s@%d",_chain.size(),
                 event, rec._kLabel, rec._kFile, rec._kLine);
    } else if ( _waiting ) {
      Debug(TRACE_DEBUG_FLAG,"push-call#%lu: %s %s@%d [%d]",_chain.size(),rec._kLabel,rec._kFile,rec._kLine, event);
    }

    auto &self = active_event();
    ink_release_assert( &self == &_chain.back() );
    return;
  }

  // don't hold in old calls...
  while ( EventCalled::trim_call(_waiting->_chain) ) 
    { _waiting->_chain.pop_back(); }

  // push record for calling frame
  _waiting->_chain.push_back( EventCalled(_waiting->_chain.back(), *this) );
  // push record for called frame
  _chain.push_back( EventCalled(*_waiting,rec,event) );

  auto &self = active_event();
  auto &calling = _waiting->_chain.back();

  ink_release_assert( &self == &_chain.back() );
  ink_release_assert( self._callingChainLen == _waiting->_chain.size() );
  ink_release_assert( calling._calledChainLen == _chain.size() );
  ink_release_assert( &calling.called() == &self );
  ink_release_assert( &calling == &self.calling() );

//    if ( rec.is_no_log() ) {
//      return;
//    }
//
//    auto &back = next._callingChain->back();
//    Debug(TRACE_DEBUG_FLAG,"separate-object-push[#%lu->#%lu]: [%05d] %s %s@%d --> [%05d] %s %s@%d", _callingChainLen, _currentCallChain->size(),
//             back._event, back._assignPoint->_kLabel, back._assignPoint->_kFile, back._assignPoint->_kLine,
//             event, _assignPoint->_kLabel, _assignPoint->_kFile, _assignPoint->_kLine);
}

EventCalled::EventCalled(EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _event(event)
{ 
  ink_release_assert(_hdlrAssign);
}


EventCalled::EventCalled(const EventCallContext &octxt, EventHdlr_t assign, int event)
   : _hdlrAssign(&assign),
     _callingChain( octxt._chainPtr ),
     _event(event),
     _callingChainLen( octxt._chain.size() )
{ 
  ink_release_assert(_hdlrAssign);
  Debug(TRACE_DEBUG_FLAG,"called into #%ld: %s %s@%d", octxt._chain.size(), assign._kLabel, assign._kFile, assign._kLine);

}

// for caller-only
EventCalled::EventCalled(const EventCalled &prev, const EventCallContext &nctxt)
   : _hdlrAssign( prev._hdlrAssign ), // don't leave null!
     _calledChain( nctxt._chainPtr ),
     _calledChainLen( nctxt._chain.size()+1 ) // include new record not inserted yet!
{ 
  ink_release_assert(_hdlrAssign);
  Debug(TRACE_DEBUG_FLAG,"calling from #%ld: %s %s@%d", nctxt._chain.size(), prev._hdlrAssign->_kLabel, prev._hdlrAssign->_kFile, prev._hdlrAssign->_kLine);
}

EventCalled::Chain::~Chain()
{
  std::ostringstream oss;
  EventCalled::printLog(oss,begin(),begin()+1," [chain FIRST]",nullptr);
  EventCalled::printLog(oss,begin()+1,end()," [chain DTOR]",nullptr);
  oss.str().empty() || ({ DebugSpecific(true,TRACE_FLAG,"pop-dtor %s",oss.str().c_str()); true; });
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(const EventHdlrState &state, EventCallContext *octxtp, const EventCalled::ChainPtr_t &chain, int event)
   : _statep(&state), // void* only
     _waiting(octxtp),
     _chainPtr(chain), // shared ownership (if state destructs)
     _chain(*chain),
     _chainInd(chain->size()) // one minus new size
{
//  if ( ! _waiting ) {
//    ink_stack_trace_dump();
//  }

  // create entry using current rec
  push_incomplete_call(static_cast<EventHdlr_t>(state), event);

  if ( _chain.size() > 100 ) {
    std::ostringstream oss;
    EventCalled::printLog(oss,_chain.begin(),_chain.end()," [reset-too-long]",_statep);
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
     _scopeContext( new EventCallContext(*this, EventCallContext::st_currentCtxt, std::make_shared<EventCalled::Chain>(), 0) )
{
  Debug(TRACE_FLAG_FIXED,"%p: init-created: %s %s@%d",this,_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine);
}

//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr),
     _scopeContext( new EventCallContext(*this, EventCallContext::st_currentCtxt, std::make_shared<EventCalled::Chain>(),0) ) // add ctor on to stack!
{
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
}

void EventHdlrState::reset_top_frame() 
{ 
  void *p = _scopeContext.get(); // store copy of old

  _scopeContext->completed();  // end all prev. memory activity

  auto waiting = _scopeContext->_waiting;
  auto chainPtr = _scopeContext->_chainPtr;
  _scopeContext = nullptr; // prevent overlap alloc-dealloc

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
  auto &chain = _scopeContext->_chain;
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
  auto &chain = _scopeContext->_chain;
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

//////////////////////////////////////////
// current chain has a record to complete
//    then change to new change if needed..
//////////////////////////////////////////
void EventCallContext::pop_caller_record()
{
  ink_release_assert( _chain.size() );

  EventCalled &call = active_event();
  unsigned ind = &call - &_chain.front();
  auto &rec = *call._hdlrAssign;
  std::ostringstream oss;

  // caller was simply from outside all call chains?
  if ( ind >= _chain.size() ) {
    return;
  }

  if ( ! call._callingChainLen || call.calling().is_frame_rec() )
  {
    // returned from *last* entry?
    char buff[256];
    EventCalled::printLog(oss,_chain.begin()+ind,_chain.end(),"[trace]",_statep);
    if ( ! oss.str().empty() ) {
      snprintf(buff,sizeof(buff),
           "\n" TRACE_SNPRINTF_PREFIX " %p: top-object #%u/%lu: %s %s@%d [evt#%05d] (refs=%ld)",
           TRACE_SNPRINTF_DATA _statep, ind, _chain.size(), rec._kLabel,rec._kFile,rec._kLine, 
           call._event, _chainPtr.use_count());
      oss << buff;
    }
    oss.str().empty() || ({DebugSpecific(true,TRACE_FLAG,"trace-out %s",oss.str().c_str()); true; });
  }
}

EventCallContext::~EventCallContext()
{
  completed(); // completes the current record...

  pop_caller_record();

  if ( st_currentCtxt == this ) {
    st_currentCtxt = _waiting; // don't leave a pointer behind
  }
}

void
EventCalled::completed(EventCallContext const &ctxt)
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto allocTot = ptrdiff_t() + EventCallContext::st_allocCounterRef - ctxt._allocStamp;
  auto deallocTot = ptrdiff_t() + EventCallContext::st_deallocCounterRef - ctxt._deallocStamp;

  auto waiting = _allocDelta - _deallocDelta;

  _allocDelta += allocTot;
  _deallocDelta += deallocTot;

  Debug(TRACE_DEBUG_FLAG,"complete [%+ld + %+d -> %+d] @#%d: %s %s@%d", allocTot - deallocTot, waiting, _allocDelta - _deallocDelta, ctxt._chainInd, _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine);
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
