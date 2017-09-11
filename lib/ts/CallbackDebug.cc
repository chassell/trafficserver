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
#include <cctype>

static_assert( offsetof(EventHdlrAssignRec, _kLabel) == offsetof(EventCHdlrAssignRec, _kLabel), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kFile) == offsetof(EventCHdlrAssignRec, _kFile), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kLine) == offsetof(EventCHdlrAssignRec, _kLine), "offset layout mismatch");
static_assert( offsetof(EventHdlrAssignRec, _kTSEventFunc) == offsetof(EventCHdlrAssignRec, _kCallback), "offset layout mismatch");

#define TRACE_DEBUG_FLAG  "debug_conttrace"
#define TRACE_FLAG  "conttrace"
#define TRACE_FLAG_FIXED  "conttrace           "
#define TRACE_SNPRINTF_PREFIX  "{%#012lx} "
#define TRACE_SNPRINTF_DATA  ink_thread_self(),

#define EVENT_NONE 0
#define EVENT_IMMEDIATE 1
#define EVENT_INTERVAL 2
#define EVENT_ERROR 3
#define EVENT_CALL 4
#define EVENT_POLL 5

#define VC_EVENT_EVENTS_START 100
#define NET_EVENT_EVENTS_START 200
#define DISK_EVENT_EVENTS_START 300
#define CLUSTER_EVENT_EVENTS_START 400
#define HOSTDB_EVENT_EVENTS_START 500
#define DNS_EVENT_EVENTS_START 600
#define CONFIG_EVENT_EVENTS_START 800
#define LOG_EVENT_EVENTS_START 900
#define MULTI_CACHE_EVENT_EVENTS_START 1000
#define CACHE_EVENT_EVENTS_START 1100
#define CACHE_DIRECTORY_EVENT_EVENTS_START 1200
#define CACHE_DB_EVENT_EVENTS_START 1300
#define HTTP_NET_CONNECTION_EVENT_EVENTS_START 1400
#define HTTP_NET_VCONNECTION_EVENT_EVENTS_START 1500
#define GC_EVENT_EVENTS_START 1600
#define ICP_EVENT_EVENTS_START 1800
#define TRANSFORM_EVENTS_START 2000
#define STAT_PAGES_EVENTS_START 2100
#define HTTP_SESSION_EVENTS_START 2200
#define HTTP2_SESSION_EVENTS_START 2250
#define HTTP_TUNNEL_EVENTS_START 2300
#define HTTP_SCH_UPDATE_EVENTS_START 2400
#define NT_ASYNC_CONNECT_EVENT_EVENTS_START 3000
#define NT_ASYNC_IO_EVENT_EVENTS_START 3100
#define RAFT_EVENT_EVENTS_START 3200
#define SIMPLE_EVENT_EVENTS_START 3300
#define UPDATE_EVENT_EVENTS_START 3500
#define LOG_COLLATION_EVENT_EVENTS_START 3800
#define AIO_EVENT_EVENTS_START 3900
#define BLOCK_CACHE_EVENT_EVENTS_START 4000
#define UTILS_EVENT_EVENTS_START 5000
#define CONGESTION_EVENT_EVENTS_START 5100
#define INK_API_EVENT_EVENTS_START 60000
#define SRV_EVENT_EVENTS_START 62000
#define REMAP_EVENT_EVENTS_START 63000

std::atomic_uint EventChain::s_ident{1};

CALL_FRAME_RECORD(null::null, kHdlrAssignEmpty);
CALL_FRAME_RECORD(nodflt::nodflt, kHdlrAssignNoDflt);
CALL_FRAME_RECORD(init::init, kHdlrAssignInit);

namespace {

void debug_str(std::string &debugStr, const char *label) 
{
   debugStr = label;

   auto colon = debugStr.rfind("::");
   if ( colon != debugStr.npos ) { 
     debugStr.erase(colon); // to the end
   }

   auto amp = debugStr.rfind('&');
   if ( amp == debugStr.npos ) {
     debugStr = TRACE_FLAG_FIXED;
     return;
   }

   debugStr.erase(0,amp+1); // from the start
   debugStr.insert(0,"cont_");
   debugStr.resize(20,' ');
}

void callback_str(std::string &callback, const EventCalled &call, const char *label) 
{
  callback = label;
  if ( callback.find('&') != callback.npos ) {
     callback.erase(0,callback.find('&')+1);
  }
  if ( callback.find(')') != callback.npos ) {
     callback.erase(0,callback.find(')')+1);
  }
  if ( call.is_frame_rec() ) {
     callback.insert(0,1,'#');
  }
}

void callinout_str(std::string &callinout, const EventCalled &call) 
{
    if ( call.has_calling() )
    {
      auto extCalling = call.calling()._hdlrAssign->_kLabel;

      auto extCallingTrail = strrchr(extCalling,'&');
      if ( ! extCallingTrail ) {
         extCallingTrail = strrchr(extCalling,')');
      }
      if ( ! extCallingTrail || ! std::isalpha(extCallingTrail[1]) ) {
        extCallingTrail = extCalling-1; // pre-decr
      }

      ++extCallingTrail;

      ink_release_assert( *extCallingTrail );

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
}

void event_str(std::string &eventbuff, unsigned event)
{
    switch ( event ) 
    {
      case EVENT_NONE:
         eventbuff = ""; return;
      case EVENT_IMMEDIATE:
         eventbuff = "IMM"; return;
      case EVENT_INTERVAL:
         eventbuff = "REPT"; return;
      case EVENT_ERROR:
         eventbuff = "ERR"; return;
      case EVENT_CALL:
         eventbuff = "CALL"; return;
      case EVENT_POLL:
         eventbuff = "POLL"; return;
      default:
        break;
    }

    static constexpr std::pair<unsigned,const char*> lookup_events[] = {
      { VC_EVENT_EVENTS_START, "VC" },
      { NET_EVENT_EVENTS_START, "NET" },
      { DISK_EVENT_EVENTS_START, "DISK" },
      { CLUSTER_EVENT_EVENTS_START, "CLST" },
      { HOSTDB_EVENT_EVENTS_START, "HSTDB" },
      { DNS_EVENT_EVENTS_START, "DNS" },
      { CONFIG_EVENT_EVENTS_START, "CFG" },
      { LOG_EVENT_EVENTS_START, "LOG" },
      { MULTI_CACHE_EVENT_EVENTS_START, "MCACH" },
      { CACHE_EVENT_EVENTS_START, "CACHE" },
      { CACHE_DIRECTORY_EVENT_EVENTS_START, "CACHDIR" },
      { CACHE_DB_EVENT_EVENTS_START, "CACHDB" },
      { HTTP_NET_CONNECTION_EVENT_EVENTS_START, "NETCXN" },
      { HTTP_NET_VCONNECTION_EVENT_EVENTS_START, "NETVCXN" },
      { GC_EVENT_EVENTS_START, "GC" },
      { ICP_EVENT_EVENTS_START, "ICP" },
      { TRANSFORM_EVENTS_START, "XFRM" },
      { STAT_PAGES_EVENTS_START, "STAT" },
      { HTTP_SESSION_EVENTS_START, "HTTP" },
      { HTTP2_SESSION_EVENTS_START, "HTTP2" },
      { HTTP_TUNNEL_EVENTS_START, "HTTPTUN" },
      { HTTP_SCH_UPDATE_EVENTS_START, "HTTPSCH" },
      { NT_ASYNC_CONNECT_EVENT_EVENTS_START, "ACONN" },
      { NT_ASYNC_IO_EVENT_EVENTS_START, "AIO" },
      { RAFT_EVENT_EVENTS_START, "RAFT" },
      { SIMPLE_EVENT_EVENTS_START, "SIMPLE" },
      { UPDATE_EVENT_EVENTS_START, "UPDATE" },
      { LOG_COLLATION_EVENT_EVENTS_START, "LOGCOLL" },
      { AIO_EVENT_EVENTS_START, "AIO" },
      { BLOCK_CACHE_EVENT_EVENTS_START, "BLKCACH" },
      { UTILS_EVENT_EVENTS_START, "UTIL" },
      { CONGESTION_EVENT_EVENTS_START, "CONGST" },
      { INK_API_EVENT_EVENTS_START, "INK_API" },
      { SRV_EVENT_EVENTS_START, "DNSSRV" },
      { REMAP_EVENT_EVENTS_START, "REMAP" }
    };

    using key_t = decltype(lookup_events[0]);

    key_t key = {event,""};
    auto i = std::lower_bound(std::begin(lookup_events), std::end(lookup_events), key); 

    if ( i == std::end(lookup_events) ) {
      eventbuff.resize(10);
      snprintf(&eventbuff.front(),eventbuff.size(),"??.%05d",event);
      return;
    }

    ink_release_assert( event >= i->first );

    eventbuff = i->second;
    if ( event != i->first ) {
      auto blank = eventbuff.size();
      eventbuff.append("          ");
      snprintf(&eventbuff.front()+blank,eventbuff.size()-blank,".%02d",event - i->first);
    } 
}

} // anon namespace 

// specialize operator..
namespace std
{
template <>
inline bool operator< <unsigned, const char*> (const std::pair<unsigned,const char*> &a, const std::pair<unsigned,const char*> &b)
  { return a.first < b.first; }
}

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

bool EventCalled::is_no_log_mem() const
{
  if ( ! _hdlrAssign ) {
    return true; // don't log
  }

  // do log if memory actually was increased/decreased 
  return _allocDelta == _deallocDelta;
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
  return _event == EVENT_NONE || _event == EVENT_INTERVAL;
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
    if ( _allocTotal != _deallocTotal && ! begin->is_no_log() ) {
      break;
    }
    if ( _allocTotal == _deallocTotal && ! begin->is_no_log_mem() ) {
      break;
    }


//    const EventHdlrAssignRec &rec = *begin->_hdlrAssign;
//    Debug(TRACE_DEBUG_FLAG,"(C#%06x) log skipping [%u...#%u]: [%05d] %s %s@%d",id(), obegin->_i, begin->_i, 
//             begin->_event,  
//             rec._kLabel, rec._kFile, rec._kLine);

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

    float delay = call._delay;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    if ( call.has_calling() || iter == begin ) 
    {
      logTotal += memDelta;
      delayTotal += delay;
    }

    const EventHdlrAssignRec &rec = *call._hdlrAssign;

    std::string debugStr;
    std::string callinout;
    std::string callback;

    debug_str(debugStr, rec._kLabel);
    callinout_str(callinout,call);
    callback_str(callback, call, rec._kLabel);

    const char *debug = debugStr.c_str();
    if ( ! call._i && oid() ) 
    {
      out << std::endl << "(" << debug << ") ";
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "   (C#%06x)     CREATED   [ mem %9ld ] <---- (C#%06x) thread-jump                 under %s %s@%d %s",
               TRACE_SNPRINTF_DATA id(), memDelta, oid(),
               callback.c_str(), rec._kFile, rec._kLine, callinout.c_str());
      out << buff;
      continue;
    }

    if ( ! call._i ) 
    {
      out << std::endl << "(" << debug << ") ";
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "   (C#%06x)     CREATED   [ mem %9ld ]                             under %s %s@%d %s",
               TRACE_SNPRINTF_DATA id(), memDelta,
               callback.c_str(), rec._kFile, rec._kLine, callinout.c_str());

      out << buff;
      continue;
    }

    std::string eventbuff;
    event_str(eventbuff,call._event);

    snprintf(buff,sizeof(buff),
         TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%2d:[ mem %9ld (tot %+9ld) time ~%5.1f%s ] %10s %s %s@%d %s [%s]", 
         TRACE_SNPRINTF_DATA id(), call._i, 
         memDelta, chainTotal, delay / div, units,
         eventbuff.c_str(), callback.c_str(), rec._kFile, rec._kLine, callinout.c_str(), omsg );

    // only if a not-first call was outward
    if ( call._calledChainLen && iter != latest && iter != begin ) {
      out << std::endl << "---->"; // separate from last one
    }
    out << std::endl << "(" << debug << ") ";
    out << buff;
    // only if a not-last call was inward 
    if ( call._callingChainLen && iter != begin && iter != latest  ) {
      out << std::endl << "<----";
    }
  }

  if ( ! logTotal || n == 1 ) {
    return n;
  }

  const char *units = ( delayTotal >= 10000 ? "ms" : "us" ); 
  float div = ( delayTotal >= 10000 ? 1000.0 : 1.0 ); 

  out << std::endl << "(" << TRACE_DEBUG_FLAG << ")      ";

  snprintf(buff,sizeof(buff),
           TRACE_SNPRINTF_PREFIX "   (C#%06x) [%2lu]:[ sub %9ld ~~ tot %+9ld time ~%5.1f%s] %10s                        [%s]",
           TRACE_SNPRINTF_DATA id(), size(), logTotal, chainTotal, delayTotal / div, units, "__________", omsg);
  out << buff;
  return n;
}

bool EventCalled::trim_check() const
{
  // NOTE: back record of chain only

  if ( ! _hdlrAssign || ! _delay || ! _i ) {
    return false; // nothing active and leave ctor recs!
  }

  // can simply detach from down-calls ...
  // must pop higher calls 

  // recurse up to see if we can trim all these out
  //   TODO: could go nuts and recurse down parent chains!
  if ( has_calling() && ! calling().trim_check() ) {
    return false;
  }
  if ( has_calling() && _callingChain.lock()->size() != _callingChainLen ) {
    return false;
  }

  // not boring ... but memory was spent: trim
  if ( _allocDelta != _deallocDelta ) {
    return false; // need no memory lost
  }

  // not boring ... but no memory spent: trim
  return true;
}

bool EventChain::trim_check()
{
  if ( size() <= 1 ) {
    return false;
  }
//  }

  if ( ! back().trim_check() ) 
  {
    if ( size() > 1000 && back().has_calling() ) {
      auto &ending = back();
      auto &calling = back().calling();
      auto &cchain = *back()._callingChain.lock();
      auto &cchainBack = cchain.back();
      Debug(TRACE_DEBUG_FLAG,"(C#%06x) must trim #%ld %s <--- %s (?%d)", id(), size()-1, ending._hdlrAssign->_kLabel,
                                                                           calling._hdlrAssign->_kLabel, &cchainBack == &calling );
    }
    return false;
  }

  return true;
}

void EventCallContext::push_incomplete_call(EventHdlr_t rec, int event)
{
  ink_release_assert( _chainPtr ); 

  auto &chain = *_chainPtr;

  if ( ~_chainInd && _chainInd+1 == chain.size() ) {
    completed("push"); // just in case!?
  }

  // logs should show it is not accurate
  _chainInd = -1;

  // clear up any old calls on the new chain...
  //   NOTE: may trim *lower* chains
  while ( chain.trim_check() ) { 
    ink_release_assert( chain.trim_back() ); 
  }

  // reset in case
  _chainInd = chain.size();

  ink_release_assert( _chainInd == chain.size() ); 

  if ( _chainPtr->size() > 5000 ) {
    std::ostringstream oss;
    _chainPtr->printLog(oss,0,~0U,"reset-too-long");
    DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
    ink_fatal("too long!");
  }

  // only with *real* handoff-calls    
  // only with non-ctors where memory is tracked
  // only where both are owned in stack
  if ( _chainInd && _waiting 
                 && _waiting->_chainPtr != _chainPtr 
                 && ink_mutex_try_acquire(&_waiting->_chainPtr->_owner) )
  {
    if ( _waiting->_chainPtr->size() > 5000 ) {
      std::ostringstream oss;
      _waiting->_chainPtr->printLog(oss,0,~0U,"reset-too-long");
      DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
      ink_fatal("too long!");
    }

    // multi-chain calling was done
    auto &ochain = *_waiting->_chainPtr;

    // make certain was trimmed
    _waiting->completed("upush"); // just in case!?

    // logs should show it's in flux
    _chainInd = -1;
    _waiting->_chainInd = -1;

    // trim calls if there's nothing remarkable...
    //   NOTE: may trim *local* chain
    while ( ochain.trim_check() ) { 
      ink_release_assert( ochain.trim_back() ); 
    }

    // set correctly
    _chainInd = chain.size();
    _waiting->_chainInd = ochain.size();

    // prepared with the *last* element 

    auto jump = st_allocCounterRef - _waiting->_allocStamp;
    jump -= st_deallocCounterRef - _waiting->_deallocStamp;

    if ( _waiting->active_event()._delay ) {
      jump = 0; // stamp is not valid now
    }

    // push called-record for previous chain
    ochain.push_back( EventCalled(_waiting->_chainInd, ochain.back(), *this) );

    // push calling-record for this chain
    chain.push_back( EventCalled(_chainInd, *_waiting, rec, event) );

    auto &self = chain.back();
    auto &calling = ochain.back();

    const_cast<uint16_t&>(self._callingChainLen) = ochain.size();
    const_cast<uint16_t&>(calling._calledChainLen) = chain.size();

    ink_release_assert( &self == &chain.front() + self._i );
    ink_release_assert( &calling == &ochain.front() + calling._i );
    ink_release_assert( &self == &active_event() );
    ink_release_assert( &calling.called() == &self );
    ink_release_assert( &calling == &self.calling() );
    ink_release_assert( ! calling._allocDelta && ! calling._deallocDelta );
    ink_release_assert( ! self._allocDelta && ! self._deallocDelta );

    auto allocTot = int64_t() + EventCallContext::st_allocCounterRef - _allocStamp;
    auto deallocTot = int64_t() + EventCallContext::st_deallocCounterRef - _deallocStamp;

    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d (<==(C#%06x) @#%d): chain-push [fwd:%+ld init:%+ld] [otot:%ld ntot:%ld] %s %s@%d",
       id(), self._i, ochain.id(), calling._i, 
       jump, allocTot - deallocTot,
       ochain._allocTotal - ochain._deallocTotal,
       chain._allocTotal - chain._deallocTotal,
       rec._kLabel, rec._kFile, rec._kLine);

    // reset the chain-ind and the start point for everything
    // must reset these ... (ctor values are strange)
    const_cast<time_point &>(_start) = std::chrono::steady_clock::now();
    const_cast<uint64_t &>(_allocStamp) = st_allocCounterRef;
    const_cast<uint64_t &>(_deallocStamp) = st_deallocCounterRef;

    // NOTE: context resets memory counters when done w/ctor
    return;
  }

  // ctor first-records (no backref)
  // untracked-top records (no earlier frame)
  // unowned-call records (other chain is lost)
  auto recp = &rec;

  // not a real call?  then just mark it from its origins
  if ( _waiting && &rec == &kHdlrAssignEmpty ) {
    recp = _waiting->active_event()._hdlrAssign;
  } 

  chain.push_back( EventCalled(_chainInd, *recp, event) );

  ink_release_assert( _chainInd == chain.size()-1 ); 

  if ( ! _chainInd && _waiting ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) <--- (C#%06x) ctor-init [ under %s %s@%d ]",chain.id(), _waiting->id(),
                recp->_kLabel,recp->_kFile,recp->_kLine);
  } else if ( ! _chainInd ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) ctor-init [ under %s %s@%d ]",chain.id(),
                recp->_kLabel,recp->_kFile,recp->_kLine);
  } else if ( _waiting ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== @#%u in-chain-push [%05d] %s %s@%d",chain.id(), 
                _chainInd, _chainInd-1, event, recp->_kLabel,recp->_kFile,recp->_kLine);
  } else if ( ! rec.is_no_log() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== event-push [%05d] %s %s@%d",chain.id(), _chainInd,
               event, recp->_kLabel,recp->_kFile,recp->_kLine);
  } else {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== frame-push [%05d] %s %s@%d",chain.id(), _chainInd,
               event, recp->_kLabel,recp->_kFile,recp->_kLine);
  }

  // must reset these ... to avoid middle-points above
  const_cast<time_point &>(_start) = std::chrono::steady_clock::now();
  const_cast<uint64_t &>(_allocStamp) = st_allocCounterRef;
  const_cast<uint64_t &>(_deallocStamp) = st_deallocCounterRef;

  auto &self = active_event();
  ink_release_assert( &self == &chain.back() );
  // NOTE: context resets memory counters when done w/ctor
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
  Debug(TRACE_DEBUG_FLAG,"@#%d called from <==(C#%06x) @#%d-1: using new %s %s@%d", _i, octxt.id(), _callingChainLen, assign._kLabel, assign._kFile, assign._kLine);
}

// for caller-only
EventCalled::EventCalled(unsigned i, const EventCalled &prev, const EventCallContext &nctxt)
   : _hdlrAssign( prev._hdlrAssign ), // don't leave null!
     _calledChain( nctxt._chainPtr ),
     _i(i),
     // event is zero
     _calledChainLen( nctxt._chainPtr->size() ) // should be correct (but update it)
{ 
  ink_release_assert(_hdlrAssign);
  Debug(TRACE_DEBUG_FLAG,"@#%d calling into ==>(C#%06x) #%d-1: under prev %s %s@%d", _i, nctxt.id(), _calledChainLen, prev._hdlrAssign->_kLabel, prev._hdlrAssign->_kFile, prev._hdlrAssign->_kLine);
}

bool EventChain::trim_back()
{
  if ( ! ink_mutex_try_acquire(&_owner) ) {
    return false;
  }

  // is owned on this thread
  ink_mutex_release(&_owner);

  if ( empty() ) {
    return false;
  }

  if ( back().has_called() ) 
  {
    auto &called = back().called();
    auto &cchain = *back()._calledChain;
    const EventHdlrAssignRec &calledRec = *called._hdlrAssign;
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) trim downcall #%lu [%05d] %s %s@%d", cchain.id(), cchain.size()-1, called._event, calledRec._kLabel, calledRec._kFile, calledRec._kLine);
  }

  const EventHdlrAssignRec &rec = *back()._hdlrAssign;

  if ( ! back().trim_back_prep() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) failed to trim local #%lu [%05d] %s %s@%d", id(), size()-1, back()._event, rec._kLabel, rec._kFile, rec._kLine);
    return false; // no pop allowed
  }

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) trim local #%lu [%05d] %s %s@%d", id(), size()-1, back()._event, rec._kLabel, rec._kFile, rec._kLine);
  pop_back();

  return true; // popped
}

bool EventCalled::trim_back_prep()
{
  if ( has_called() ) 
  {
    auto &cchain = *_calledChain;
    const_cast<ChainWPtr_t&>(called()._callingChain).reset();

    // that last lower one is feasible?
    if ( cchain.size() == _calledChainLen ) {
      cchain.trim_back(); // try to trim *now*
    }
  }

  if ( ! has_calling() ) {
    // base case --> no calling refs left
    return true;
  }

  auto &callingRec = calling();
  auto &callingChain = *_callingChain.lock();

  if ( callingChain.size() != _callingChainLen ) {
    // base case --> cannot pop self
    return false;
  }

  const_cast<ChainWPtr_t&>(_callingChain).reset();
  const_cast<ChainPtr_t&>(callingRec._calledChain).reset(); 
  const_cast<uint16_t&>(callingRec._calledChainLen) = 0; 

  callingChain.trim_back(); // recurse up-only

  // base case --> no calling refs left
  return true;
}

EventChain::EventChain()
{
  reserve(10);
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

  auto r = pthread_mutex_init(&_owner, &attr);
  ink_release_assert( ! r );

  r = ink_mutex_acquire(&_owner);
  ink_release_assert( ! r );
}

EventChain::EventChain(uint32_t oid)
   : _oid(oid)
{
  reserve(10);
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

  auto r = pthread_mutex_init(&_owner, &attr);
  ink_release_assert( ! r );

  r = ink_mutex_acquire(&_owner);
  ink_release_assert( ! r );
}

EventChain::~EventChain()
{
  std::ostringstream oss;
  printLog(oss,0,~0U,"chain.dtor");
  oss.str().empty() || ({ DebugSpecific(true,TRACE_FLAG,"pop-dtor %s",oss.str().c_str()); true; });

  if ( front()._hdlrAssign->is_plugin_rec() ) {
    Debug(TRACE_DEBUG_FLAG,"deleting assign-rec %s %s@%d", front()._hdlrAssign->_kLabel, front()._hdlrAssign->_kFile, front()._hdlrAssign->_kLine);
    delete front()._hdlrAssign->_kLabel;
    delete front()._hdlrAssign;
  }

  while ( ! pthread_mutex_unlock(&_owner) ) {
    Warning("Leftover locks at chain dtor");
  }
  pthread_mutex_destroy(&_owner);
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(const EventHdlr_t &hdlr, const EventCalled::ChainPtr_t &chainPtr, int event)
   : _chainPtr(chainPtr) // _waiting(
{
  st_currentCtxt = this; // push down one immediately..

  // is there a post-return call we are returning from?
  // close earlier context before taking over
  if ( chainPtr && _waiting ) {
    _waiting->completed("wctxt.ctor"); // mark and release chain (if owned)
  } else if ( chainPtr ) {
    completed("ctxt.ctor"); // mark and release chain (if owned)
  }

  if ( chainPtr && ! ink_mutex_try_acquire(&_chainPtr->_owner) )
  {
    auto &ochain = *_chainPtr;
    auto nchainPtr = std::make_shared<EventChain>(ochain.id()); // NOTE: chain adds into alloc tally
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%lu --> (C#%06x): event-thread-restart [tot:%ld] %s %s@%d",
       ochain.id(), ochain.size()-1, nchainPtr->id(), ochain._allocTotal - ochain._deallocTotal,
       hdlr._kLabel, hdlr._kFile, hdlr._kLine);
    _chainPtr = nchainPtr;
    // ctor acquires lock until completed() is called
  } 
  else if ( ! chainPtr && _waiting ) 
  {
    // else lock was successfully acquired until completed()
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== (C#%06x) chain-ctor [%05d] %s %s@%d", ~0U, 0, _waiting->id(),
               event, hdlr._kLabel, hdlr._kFile, hdlr._kLine);
    // ctor acquires lock until completed() is called
    _chainPtr = std::make_shared<EventChain>(); // NOTE: chain adds into alloc tally
  } 
  else if ( ! chainPtr ) 
  {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== top-ctor [%05d] %s %s@%d", ~0U, 0,
               event, hdlr._kLabel, hdlr._kFile, hdlr._kLine);
    // ctor acquires lock until completed() is called
    _chainPtr = std::make_shared<EventChain>(); // NOTE: chain adds into alloc tally
  }

  // chain is owned by this thread...

  // create entry using current rec
  push_incomplete_call(hdlr, event);

  if ( ! chainPtr ) {
    st_currentCtxt = _waiting; // reset because no real call has started yet
    const_cast<EventCallContext*&>(_waiting) = nullptr; // do *not* use ctor-origin after now
  }

  ink_release_assert(_chainPtr->size()); // now non-zero

  // disclude any init bytes above 
//  active_event()._allocDelta -= st_allocCounterRef - _allocStamp;
////  active_event()._deallocDelta -= st_deallocCounterRef - _deallocStamp;
}

EventCallContext::~EventCallContext()
{
  completed(nullptr);

  if ( st_currentCtxt == this ) {
    st_currentCtxt = _waiting;
  }

  if ( st_currentCtxt == this ) {
    Warning("No replacement ctor upon destruction");
    st_currentCtxt = nullptr;
  }
}

thread_local EventCallContext *EventCallContext::st_currentCtxt = nullptr;
thread_local EventHdlrP_t     EventCallContext::st_dfltAssignPoint = nullptr;
thread_local uint64_t         &EventCallContext::st_allocCounterRef = *jemallctl::thread_allocatedp();
thread_local uint64_t         &EventCallContext::st_deallocCounterRef = *jemallctl::thread_deallocatedp();

void EventCallContext::clear_ctor_initial_callback() {
  st_dfltAssignPoint = nullptr;
  Debug(TRACE_DEBUG_FLAG,"pre-setting default");
}

void EventCallContext::set_ctor_initial_callback(EventHdlr_t rec) {
  st_dfltAssignPoint = &rec;
  Debug(TRACE_DEBUG_FLAG,"pre-setting %s %s@%d", rec._kLabel, rec._kFile, rec._kLine );
}

namespace {
EventHdlr_t current_default_assign_point()
{
  auto rec = EventCallContext::st_dfltAssignPoint;
  if ( rec && rec != &kHdlrAssignNoDflt && rec != &kHdlrAssignEmpty ) {
    Debug(TRACE_DEBUG_FLAG,"pre-set for use %s %s@%d", rec->_kLabel, rec->_kFile, rec->_kLine );
  }

  return ( rec ? *rec : kHdlrAssignNoDflt );
}
} // anon namespace

EventHdlrState::EventHdlrState(void *p, uint64_t allocStamp, uint64_t deallocStamp)
   : _assignPoint(&kHdlrAssignEmpty),      // must be set before scopeContext ctor
     _scopeContext(*this)
{
  // get the earliest point before construction started
  const_cast<uint64_t &>(_scopeContext._allocStamp) = allocStamp;
  const_cast<uint64_t &>(_scopeContext._deallocStamp) = deallocStamp;

  _scopeContext.completed("cxt.ctor"); // mark as done and release chain

  // now change to actual setting for next call 
  *this = current_default_assign_point();

  // must return an error...
  ink_release_assert( pthread_mutex_unlock(&_scopeContext._chainPtr->_owner) );
}

//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr),
     _scopeContext(*this) // add ctor on to stack!
{
  _scopeContext.completed("cxt.ctor"); // mark as done and release chain

  current_default_assign_point();
  EventCallContext::clear_ctor_initial_callback(); // this must be stack based and not in an object
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
}

void EventCallContext::reset_top_frame(EventHdlr_t hdlr) 
{
  auto &ochain = *_chainPtr; // save chain (if usable)

  ink_release_assert( st_currentCtxt == this || ! st_currentCtxt ); // must reset top here

  EventCallContext::st_currentCtxt = this; // must reset top here

  // lock for the next pushed entry (push_incomplete_call)
  if ( ink_mutex_try_acquire(&_chainPtr->_owner) ) 
  {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) resetting %s %s@%d",
       id(), hdlr._kLabel, hdlr._kFile, hdlr._kLine );

    completed("reset"); // mark and release one lock 
  } else {
    auto nchainPtr = std::make_shared<EventChain>(ochain.id()); // NOTE: chain adds into alloc tally
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%lu --> (C#%06x): event-thread-restart [tot:%ld] %s %s@%d",
       ochain.id(), ochain.size()-1, nchainPtr->id(), ochain._allocTotal - ochain._deallocTotal,
       hdlr._kLabel, hdlr._kFile, hdlr._kLine);

    // keep lock for the next pushed entry (push_incomplete_call)
    _chainPtr = nchainPtr; // NOTE: chain adds into alloc tally
  }

  // create entry using current rec (and reset storage stamp)
  push_incomplete_call(hdlr, 0);

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) resetting done %s %s@%d",
     id(), hdlr._kLabel, hdlr._kFile, hdlr._kLine );
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

void EventCallContext::completed(const char *omsg) 
{
  auto &mutex = _chainPtr->_owner;
  if ( ! ink_mutex_try_acquire(&mutex) ) {
    return;
  }

  auto msg = ( omsg ? : "ctxt.dtor" );

  auto &active = active_event();
  // fix currently endpoint if needed
  _chainInd = active._i;
  active.completed(*this,msg);  // complete this record (and adjust caller record)

  ink_mutex_release(&_chainPtr->_owner); // balance from top

  if ( ! omsg ) {
    _chainPtr.reset(); // remove from storage account
  }

  // can't reset accounting for next?
  if ( ! _waiting || ! _waiting->_chainPtr ) {
    return;
  }

  if ( ink_mutex_try_acquire(&_waiting->_chainPtr->_owner) ) {
    auto &upper = *_waiting;

    // pass accounting officially to upper context
    const_cast<time_point &>(upper._start) = std::chrono::steady_clock::now();
    const_cast<uint64_t &>(upper._allocStamp) = st_allocCounterRef;
    const_cast<uint64_t &>(upper._deallocStamp) = st_deallocCounterRef;
    ink_mutex_release(&upper._chainPtr->_owner); // balance
  }
}

void
EventCalled::completed(EventCallContext const &ctxt, const char *msg)
{
  if ( _delay ) {
    return; // don't attempt release (can't be locked either)
  }

  auto &chain = *ctxt._chainPtr;

  auto duration = std::chrono::steady_clock::now() - ctxt._start;
  auto allocTot = int64_t() + EventCallContext::st_allocCounterRef - ctxt._allocStamp;
  auto deallocTot = int64_t() + EventCallContext::st_deallocCounterRef - ctxt._deallocStamp;

  ink_release_assert( ! _delay );

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto prevDiff = _allocDelta - _deallocDelta;
  auto callingInd = 0;
  auto callingID = 0;
  auto calledID = 0;

  if ( has_calling() ) {
    callingID = _callingChain.lock()->id();
  }
  if ( has_called() ) {
    calledID = _calledChain->id();
  }

  _allocDelta += allocTot;
  _deallocDelta += deallocTot;

  if ( _i ) {
    chain._allocTotal += _allocDelta;
    chain._deallocTotal += _deallocDelta;
  }

  const char *title = "top-call";

  do {
    if ( has_calling() && ! calling().is_frame_rec() ) {
      title = "sub-event"; break; // no log yet
    }

    if ( has_calling() && is_frame_rec() ) {
      title = "sub-frame"; break; // no log yet
    }

    if ( has_calling() ) {
      title = "top-event";
    } else if ( has_called() && is_frame_rec() ) {
      title = "post-return-frame";
    } else if ( has_called() ) {
      title = "post-return-event";
    }

    Debug(TRACE_DEBUG_FLAG,"[ (C#%06x) @#%d ] <==> (C#%06x) @#%d called: %s complete [post:%+ld pre:%+ld -> %+ld] [tot:%ld] %s %s@%d [%s]",
       ctxt.id(), _i, calledID, _calledChainLen-1, title, 
       allocTot - deallocTot, prevDiff, _allocDelta - _deallocDelta, 
       chain._allocTotal - chain._deallocTotal,
       _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine, msg);

    std::ostringstream oss;
    chain.printLog(oss,_i,~0U,msg);

    if ( ! oss.str().empty() ) 
    {
      // break from *last* entry?
      /*
      char buff[256];
      snprintf(buff,sizeof(buff),
           "\n" TRACE_SNPRINTF_PREFIX " (C#%06x) @#%d[%lu] top-object: %s %s@%d [evt#%05d] (refs=%ld)",
           TRACE_SNPRINTF_DATA chain.id(), _i, chain.size(), _hdlrAssign->_kLabel,_hdlrAssign->_kFile,_hdlrAssign->_kLine, 
           _event, ctxt._chainPtr.use_count());
      oss << buff;

      */
      DebugSpecific(true,TRACE_FLAG,"trace-out [%s] %s",msg,oss.str().c_str());
    }

    ink_mutex_release(&chain._owner); // now free to go to other threads
    return;

  } while(false);

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d <===> [ (C#%06x) @#%d called ]: %s completed [post:%+ld pre:%+ld -> %+ld] [tot:%ld] %s %s@%d [%s]",
     callingID, callingInd, ctxt.id(), _i, title,
     allocTot - deallocTot, prevDiff, 
     _allocDelta - _deallocDelta, chain._allocTotal - chain._deallocTotal,
     _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine, msg);

  // clear up any old calls on this chain...
  while ( chain.trim_check() ) { 
    ink_release_assert( chain.trim_back() ); 
  }

  ink_mutex_release(&chain._owner); // now free to go to other threads

  // on single shot, release chain's lock too
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
