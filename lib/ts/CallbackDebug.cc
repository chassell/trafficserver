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
#include "ts/apidefs.h"

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
#define EVENT_RESET_FAKE 0xffff

#define TRIM_EVENTS ((1<<EVENT_NONE)|(1<<EVENT_IMMEDIATE)|(1<<EVENT_INTERVAL)|(1<<EVENT_POLL))

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

NAMED_CALL_FRAME(kHdlrAssignEmptyV,null::null);
NAMED_CALL_FRAME(kHdlrAssignNoDfltV,nodflt::nodflt);

const EventHdlrAssignRec &kHdlrAssignEmpty = kHdlrAssignEmptyV;
const EventHdlrAssignRec &kHdlrAssignNoDflt = kHdlrAssignNoDfltV;

namespace {

EventHdlr_t current_default_assign_point()
{
  auto recp = EventCallContext::st_dfltAssignPoint;
  if ( recp && recp != &kHdlrAssignNoDflt && recp != &kHdlrAssignEmpty ) {
    Debug(TRACE_DEBUG_FLAG,"pre-set for use %s %s@%d", recp->_kLabel, recp->_kFile, recp->_kLine );
  }

  return ( recp ? *recp : kHdlrAssignNoDflt );
}

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

void callback_str(std::string &callback, bool isFrameRec, const char *label) 
{
  callback = label;
  if ( callback.find('&') != callback.npos ) {
     callback.erase(0,callback.find('&')+1);
  }
  if ( callback.find(')') != callback.npos ) {
     callback.erase(0,callback.find(')')+1);
  }
  if ( isFrameRec && callback.front() != '#' ) {
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
    else if ( ! call._i ) 
    {
      auto &rec = current_default_assign_point();
      if ( &rec != &kHdlrAssignNoDflt && &rec != &kHdlrAssignEmpty ) 
      {
        auto extCalled = rec._kLabel;
        auto extCalledTrail = strrchr(extCalled,'&');
        if ( ! extCalledTrail ) {
           extCalledTrail = strrchr(extCalled,')');
        }
        if ( ! extCalledTrail || ! std::isalpha(extCalledTrail[1]) ) {
          extCalledTrail = extCalled-1; // pre-decr
        }

        ++extCalledTrail;

        ink_release_assert( *extCalledTrail );

        callinout += std::string() + "--> " + extCalledTrail + " ";
      }
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
         eventbuff = "INTVL"; return;
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

	  {	CACHE_EVENT_EVENTS_START + 0, "CACH.LKUP" }, // CACHE_EVENT_LOOKUP
	  {	CACHE_EVENT_EVENTS_START + 1, "CACH.NOLKUP" }, // CACHE_EVENT_LOOKUP_FAILED
	  {	CACHE_EVENT_EVENTS_START + 2, "CACH.RD" }, // CACHE_EVENT_OPEN_READ
	  {	CACHE_EVENT_EVENTS_START + 3, "CACH.NORD" }, // CACHE_EVENT_OPEN_READ_FAILED
	  {	CACHE_EVENT_EVENTS_START + 8, "CACH.WR" }, // CACHE_EVENT_OPEN_WRITE
	  {	CACHE_EVENT_EVENTS_START + 9, "CACH.NOWR" }, // CACHE_EVENT_OPEN_WRITE_FAILED
	  {	CACHE_EVENT_EVENTS_START + 12, "CACH.RM" }, // CACHE_EVENT_REMOVE
	  {	CACHE_EVENT_EVENTS_START + 13, "CACH.NORM" }, // CACHE_EVENT_REMOVE_FAILED
	  {	CACHE_EVENT_EVENTS_START + 20, "CACH.SCN" }, // CACHE_EVENT_SCAN
	  {	CACHE_EVENT_EVENTS_START + 21, "CACH.NOSCN" }, // CACHE_EVENT_SCAN_FAILED
	  {	CACHE_EVENT_EVENTS_START + 22, "CACH.SCNOBJ" }, // CACHE_EVENT_SCAN_OBJECT
	  {	CACHE_EVENT_EVENTS_START + 23, "CACH.BLKSCN" }, // CACHE_EVENT_SCAN_OPERATION_BLOCKED
	  {	CACHE_EVENT_EVENTS_START + 24, "CACH.NOSCN" }, // CACHE_EVENT_SCAN_OPERATION_FAILED
	  {	CACHE_EVENT_EVENTS_START + 25, "CACH.SCNEND" }, // CACHE_EVENT_SCAN_DONE
	  {	CACHE_EVENT_EVENTS_START + 50, "CACH.RSP" }, // CACHE_EVENT_RESPONSE

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

	  { TS_EVENT_HTTP_CONTINUE, "TSTXN.CONT" },
	  { TS_EVENT_HTTP_ERROR, "TSTXN.ERR" },
	  { TS_EVENT_HTTP_READ_REQUEST_HDR, "TSTXN.RDREQ" },
	  { TS_EVENT_HTTP_OS_DNS, "TSTXN.DNS" },
	  { TS_EVENT_HTTP_SEND_REQUEST_HDR, "TSTXN.WRREQ" },
	  { TS_EVENT_HTTP_READ_CACHE_HDR, "TSTXN.RDCACH" },
	  { TS_EVENT_HTTP_READ_RESPONSE_HDR, "TSTXN.RDRSP" },
	  { TS_EVENT_HTTP_SEND_RESPONSE_HDR, "TSTXN.WRRSP" },
	  { TS_EVENT_HTTP_REQUEST_TRANSFORM, "TSTXN.XFREQ"},
	  { TS_EVENT_HTTP_RESPONSE_TRANSFORM, "TSTXN.XFRSP"},
	  { TS_EVENT_HTTP_SELECT_ALT, "TSTXN.ALT" },
	  { TS_EVENT_HTTP_TXN_START, "TSTXN.START" },
	  { TS_EVENT_HTTP_TXN_CLOSE, "TSTXN.CLOSE" },
	  { TS_EVENT_HTTP_SSN_START, "TSSSN.START" },
	  { TS_EVENT_HTTP_SSN_CLOSE, "TSSSN.CLOSE" },
	  { TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE, "TSCACH.LKUP" },
	  { TS_EVENT_HTTP_PRE_REMAP, "TSREMAP.PRE" },
	  { TS_EVENT_HTTP_POST_REMAP, "TSREMAP.POST" },
	  { TS_EVENT_LIFECYCLE_PORTS_INITIALIZED, "TSLCYCLE.PINIT" },
	  { TS_EVENT_LIFECYCLE_PORTS_READY, "TSLCYCLE.PRDY" },
	  { TS_EVENT_LIFECYCLE_CACHE_READY, "TSLCYCLE.CRDY" },
	  { TS_EVENT_LIFECYCLE_SERVER_SSL_CTX_INITIALIZED, "TSLCYCLE.SSLSVR" },
	  { TS_EVENT_LIFECYCLE_CLIENT_SSL_CTX_INITIALIZED, "TSLCYCLE.SSLCLT" },
	  { TS_EVENT_VCONN_PRE_ACCEPT, "TSVCONN.ACC" },
	  { TS_EVENT_MGMT_UPDATE, "TSMGMT.UPD" },

      { SRV_EVENT_EVENTS_START, "DNSSRV" },
      { REMAP_EVENT_EVENTS_START, "REMAP" },
      { EVENT_RESET_FAKE, "_____" }
    };

    using key_t = decltype(lookup_events[0]);

    // always one past a full-match 
    key_t key = {event+1,""};
    auto i = std::lower_bound(std::begin(lookup_events), std::end(lookup_events), key); 

    --i; // one *before* the closest-but-larger

    ink_release_assert( event >= i[0].first && event < i[1].first );

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
inline constexpr bool operator< <unsigned, const char*> (const std::pair<unsigned,const char*> &a, const std::pair<unsigned,const char*> &b)
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

  if ( !((1<<_event) & TRIM_EVENTS) && _event < EVENT_RESET_FAKE ) {
    return false; // interesting event!
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
  return ((1<<_event) & TRIM_EVENTS) || _event == EVENT_RESET_FAKE;
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

    auto &rec = *call._hdlrAssign;

    std::string debugStr;
    std::string callinout;
    std::string callback;

    debug_str(debugStr, rec._kLabel);
    callinout_str(callinout,call);
    callback_str(callback, call.is_frame_rec(), rec._kLabel);

    const char *debug = debugStr.c_str();
    if ( ! call._i && oid() ) 
    {
      out << std::endl << "(" << debug << ") ";
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "   (C#%06x) 00 ----  CREATED   [ mem %9ld ] <---- (C#%06x) thread-jump                 under %s %s@%d %s",
               TRACE_SNPRINTF_DATA id(), memDelta, oid(),
               callback.c_str(), rec._kFile, rec._kLine, callinout.c_str());
      out << buff;
      continue;
    }

    if ( ! call._i ) 
    {
      out << std::endl << "(" << debug << ") ";
      snprintf(buff,sizeof(buff),
               TRACE_SNPRINTF_PREFIX "   (C#%06x) 00 ----  CREATED   [ mem %9ld ]                             under %s %s@%d %s",
               TRACE_SNPRINTF_DATA id(), memDelta,
               callback.c_str(), rec._kFile, rec._kLine, callinout.c_str());

      out << buff;
      continue;
    }

    std::string eventbuff;
    event_str(eventbuff,call._event);

    snprintf(buff,sizeof(buff),
         TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%02d/%02d:[ mem %9ld (tot %+9ld) time ~%5.1f%s ] %10s %s %s@%d %s [%s]", 
         TRACE_SNPRINTF_DATA id(), call._i, _cnt-1,
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

//  const char *units = ( delayTotal >= 10000 ? "ms" : "us" ); 
//  float div = ( delayTotal >= 10000 ? 1000.0 : 1.0 ); 
//
//  out << std::endl << "(" << TRACE_DEBUG_FLAG << ")      ";
//  snprintf(buff,sizeof(buff),
//           TRACE_SNPRINTF_PREFIX "   (C#%06x) [%2lu]:[ sub %9ld ~~ tot %+9ld time ~%5.1f%s] %10s                        [%s]",
//           TRACE_SNPRINTF_DATA id(), size(), logTotal, chainTotal, delayTotal / div, units, "__________", omsg);
//  out << buff;
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

void EventCallContext::push_call_entry(EventHdlr_t rec, int event)
{
  ink_release_assert( _chainPtr ); 

  auto &chain = *_chainPtr;

  // logs should show it is not accurate
  _chainInd = -1;

  // clear up any old calls on the new chain...
  //   NOTE: may trim *lower* chains
  while ( chain.trim_check() ) { 
    ink_release_assert( chain.trim_back() ); 
  }

  if ( chain.size() > 5000 ) {
    std::ostringstream oss;
    chain.printLog(oss,0,~0U,"reset-too-long");
    DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
    ink_fatal("too long!");
  }

  chain.push_back( EventCalled(chain._cnt++, rec, event) );
  // reset in case
  _chainInd = chain.size()-1;

  ink_release_assert( is_incomplete() );

  auto jump = int64_t() + st_allocCounterRef - _allocStamp;
  jump -= int64_t() + st_deallocCounterRef - _deallocStamp;

  // jump-chain failed lock?
  if ( _waiting && oid() == _waiting->id() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== @#%u thread-jump-push [ignored:%ld] [%05d]  %s %s@%d",chain.id(), 
          active_event()._i, _waiting->active_event()._i, jump,
          event, rec._kLabel,rec._kFile,rec._kLine);
  }
  else if ( _waiting && _waiting->is_incomplete() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== @#%u reused-calling-push [ignored:%ld] [%05d]  %s %s@%d",chain.id(), 
          active_event()._i, _waiting->active_event()._i, jump,
          event, rec._kLabel,rec._kFile,rec._kLine);
  }
  else if ( _waiting ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== @#%u waiting-rec-lost [ignored:%ld] [%05d]  %s %s@%d",chain.id(), 
          active_event()._i, _waiting->active_event()._i, jump,
          event, rec._kLabel,rec._kFile,rec._kLine);
  // not trivial new entry 
  } else if ( ! rec.is_no_log() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== sub-event-push [ignored:%ld] [%05d] %s %s@%d",chain.id(), _chainInd,
         jump, event, rec._kLabel,rec._kFile,rec._kLine);
  // trivial new entry 
  } else {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== sub-frame-push [ignored:%ld] [%05d] %s %s@%d",chain.id(), _chainInd,
         jump, event, rec._kLabel,rec._kFile,rec._kLine);
  }

  // reset to begin correct accounting
  const_cast<time_point &>(_start) = std::chrono::steady_clock::now();
  const_cast<uint64_t &>(_allocStamp) = st_allocCounterRef;
  const_cast<uint64_t &>(_deallocStamp) = st_deallocCounterRef;
  // NOTE: context resets memory counters when done w/ctor
}


void EventCallContext::push_call_chain_pair(EventHdlr_t rec, int event)
{
  ink_release_assert( _chainPtr && _waiting && _waiting->is_incomplete() );

  auto &chain = *_chainPtr;
  auto &ochain = *_waiting->_chainPtr;

  _chainInd = -1; // until log-cases done

  // ctor first-records (no backref)
  // untracked-top records (no earlier frame)
  // unowned-call records (other chain is lost)
  auto recp = &rec;
  auto orecp = &rec;

  // remember first-origins if making a new call
  if ( _waiting->has_active() ) {
    orecp = _waiting->active_hdlrp();
  } 

  if ( chain.size() > 5000 ) 
  {
    std::ostringstream oss;
    chain.printLog(oss,0,~0U,"reset-too-long");
    DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
    ink_fatal("too long!");
  }

  if ( ochain.size() > 5000 ) 
  {
    std::ostringstream oss;
    ochain.printLog(oss,0,~0U,"reset-too-long");
    DebugSpecific(true,TRACE_FLAG,"chain-big: %s",oss.str().c_str());
    ink_fatal("too long!");
  }

  // remove a lock from _waiting
  _waiting->complete_call("upush") && _waiting->restart_upper_stamp("upush");
  // complete any post-call or call-in

  // logs should show its done
  _waiting->_chainInd = -1;

  // do first to finish any upward checks
  while ( ochain.trim_check() ) { 
    ink_release_assert( ochain.trim_back() ); 
  }
  while ( chain.trim_check() ) { 
    ink_release_assert( chain.trim_back() ); 
  }

  EventCalled *selfp = nullptr;
  EventCalled *callingp = nullptr;

  // same chain records
  if ( &ochain == &chain || orecp->is_frame_rec() )
  {
    // prep higher context for post-call
    ochain.push_back( EventCalled(ochain._cnt++, *orecp, event) );
    _waiting->_chainInd = &ochain.back() - &ochain.front();
  // Debug(TRACE_DEBUG_FLAG,"@#%d called from <==(C#%06x) @#%d-1: using new %s %s@%d", _i, octxt.id(), _callingChainLen, assign._kLabel, assign._kFile, assign._kLine);

//    if ( orecp->is_frame_rec() )
//    {
//      // maybe use the frame as reference???  no.
//      chain.push_back( EventCalled(chain._cnt++, *_waiting, rec, event) );
//    } else {
      // push self for this context
      chain.push_back( EventCalled(chain._cnt++, *recp, event) );
    // Debug(TRACE_DEBUG_FLAG,"@#%d calling into ==>(C#%06x) #%d-1: under prev %s %s@%d", _i, nctxt.id(), _calledChainLen, prevHdlr._kLabel, prevHdlr._kFile, prevHdlr._kLine);
//    }
    _chainInd = &chain.back() - &chain.front();

    callingp = &_waiting->active_event();
    selfp = &active_event();
  }
  // different-chains records
  else 
  {
    // use waiting-active as calling rec
    ochain.push_back( EventCalled(ochain._cnt++, *orecp, *this, event) );
    _waiting->_chainInd = &ochain.back() - &ochain.front();

    // use current context as called rec
    chain.push_back( EventCalled(chain._cnt++, *_waiting, rec, event) );
    _chainInd = &chain.back() - &chain.front();

    callingp = &_waiting->active_event();
    selfp = &active_event();

    const_cast<uint16_t&>(selfp->_callingChainLen) = ochain.size();
    const_cast<uint16_t&>(callingp->_calledChainLen) = chain.size();

    ink_release_assert( &callingp->called() == selfp );
    ink_release_assert( callingp == &selfp->calling() );
  }

  ink_release_assert( callingp == &_waiting->active_event() );
  ink_release_assert( selfp == &active_event() );

  auto &self = *selfp;
  auto &calling = *callingp;

  auto allocTot = int64_t() + EventCallContext::st_allocCounterRef - _allocStamp;
  auto deallocTot = int64_t() + EventCallContext::st_deallocCounterRef - _deallocStamp;

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d ==> (C#%06x) @#%d : chain-push-two [ignore:%+ld] [otot:%ld ntot:%ld] %s %s@%d",
     ochain.id(), calling._i, 
     id(), self._i,
     allocTot - deallocTot,
     ochain._allocTotal - ochain._deallocTotal,
     chain._allocTotal - chain._deallocTotal,
     rec._kLabel, rec._kFile, rec._kLine);

  // reset the chain-ind and the start point for everything
  // must reset these ... (ctor values are strange)
  const_cast<time_point &>(_start) = std::chrono::steady_clock::now();
  const_cast<uint64_t &>(_allocStamp) = st_allocCounterRef;
  const_cast<uint64_t &>(_deallocStamp) = st_deallocCounterRef;
}

void EventCallContext::push_initial_call(EventHdlr_t under)
{
  auto &chain = *_chainPtr;

  _chainInd = 0;
  chain.push_back( EventCalled(0, under, EVENT_NONE) );

  if ( _waiting ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) <--- (C#%06x) ctor-init [ under %s %s@%d ]",chain.id(), _waiting->id(),
                under._kLabel,under._kFile,under._kLine);
  } else {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) ctor-init [ under %s %s@%d ]",chain.id(),
                under._kLabel,under._kFile,under._kLine);
  } 

  // must reset these ... to avoid middle-points above
  const_cast<time_point &>(_start) = std::chrono::steady_clock::now();
  const_cast<uint64_t &>(_allocStamp) = st_allocCounterRef;
  const_cast<uint64_t &>(_deallocStamp) = st_deallocCounterRef;

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
}

// for caller-only
EventCalled::EventCalled(unsigned i, EventHdlr_t prevHdlr, const EventCallContext &nctxt, int event)
   : _hdlrAssign( &prevHdlr ), // don't leave null!
     _calledChain( nctxt._chainPtr ),
     _i(i),
     _event(event),
     _calledChainLen( nctxt._chainPtr->size() ) // should be correct (but update it)
{ 
  ink_release_assert(_hdlrAssign);
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
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) trim downcall @#%d n=%lu [%05d] %s %s@%d", cchain.id(), cchain.back()._i, cchain.size(), called._event, calledRec._kLabel, calledRec._kFile, calledRec._kLine);
  }

  auto &rec = *back()._hdlrAssign;

  if ( ! back().trim_back_prep() ) {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) failed to trim local @#%d [%05d] %s %s@%d", id(), back()._i, back()._event, rec._kLabel, rec._kFile, rec._kLine);
    return false; // no pop allowed
  }

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) trim local @#%d [%05d] %s %s@%d", id(), back()._i, back()._event, rec._kLabel, rec._kFile, rec._kLine);
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

EventChain::EventChain(uint32_t oid, uint16_t cnt)
   : _oid(oid), _cnt(cnt)
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
  oss.str().empty() || ({ DebugSpecific(true,TRACE_FLAG,"chain-dtor %s",oss.str().c_str()); true; });

  if ( front()._hdlrAssign->is_plugin_rec() ) {
    Debug(TRACE_FLAG,"NOT deleting assign-rec %s %s@%d", front()._hdlrAssign->_kLabel, front()._hdlrAssign->_kFile, front()._hdlrAssign->_kLine);
//    delete front()._hdlrAssign->_kLabel;
//    delete front()._hdlrAssign;
  }

  while ( ! pthread_mutex_unlock(&_owner) ) {
    Warning("Leftover locks at chain dtor");
  }
  pthread_mutex_destroy(&_owner);
}

/////////////////////////////////////////////////////////////////
// create new callback-entry on chain associated with HdlrState
/////////////////////////////////////////////////////////////////
EventCallContext::EventCallContext(EventHdlr_t hdlr, const EventCalled::ChainPtr_t &chainPtr, int event)
   : _chainPtr(chainPtr) // _waiting(
{
  st_currentCtxt = this; // push down one immediately..

  ink_release_assert(chainPtr);

  // must branch?  [NOTE: alloc *before* completed entries below]
  if ( ! ink_mutex_try_acquire(&_chainPtr->_owner) )
  {
    int64_t memAlloc = 0;
    int64_t memDealloc = 0;
    EventHdlrP_t ohdlr = nullptr;
    auto oid = 0;
    auto ocnt = 0;

    {
      auto ochainPtr = _chainPtr;
      auto &ochain = *_chainPtr;
      oid = ochain.id();
      ocnt = ochain._cnt+1;
      memAlloc = ochain._allocTotal;
      memDealloc = ochain._deallocTotal;
      ohdlr = ochain[0]._hdlrAssign;
    }

    auto nchainPtr = std::make_shared<EventChain>(oid,ocnt); // NOTE: chain adds into alloc tally
    auto &nchain = *nchainPtr;
    _chainPtr = nchainPtr;

    nchain._allocTotal = memAlloc;
    nchain._deallocTotal = memDealloc;

    {
      std::string eventbuff, callback;

      event_str(eventbuff,event);
      callback_str(callback, false, hdlr._kLabel); // wish me luck!

      DebugSpecific(true,TRACE_FLAG,"thread-jump\n(conttrace           ) " 
                 TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%02d   :[ tot mem %9ld ] %10s %s %s@%d [%s]", 
                 TRACE_SNPRINTF_DATA nchain.oid(), nchain._cnt-1, memAlloc - memDealloc,
                 eventbuff.c_str(), callback.c_str(), hdlr._kFile, hdlr._kLine, "ctxt.jump" );
    }

    push_initial_call(*ohdlr);
    complete_call("ctxt.jump");
    // ctor acquires lock until completed() is called
  }

  if ( _waiting && _waiting->is_incomplete() 
                && ink_mutex_try_acquire(&_waiting->_chainPtr->_owner) ) 
  {
    // upper chain has 2 incomplete-locks
    push_call_chain_pair(hdlr, event); // start new calling-called records
    // local chain has incomplete-lock
    // upper chain has one incomplete-lock
  } else {
    push_call_entry(hdlr, event); // new call record on this chain only
    // local chain [only] has incomplete-lock
    // no upper entry is incomplete
  }

}


EventCallContext::EventCallContext()
   : _chainPtr(std::make_shared<EventChain>(0,1)),  // init oid==0, cnt==1
     _chainInd(0) 
   // : init of _waiting(..)
{
  // ctor acquires lock until completed() is called

  if ( _waiting ) {
    auto &rec = _waiting->active_hdlr();
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%u <=== (C#%06x) chain-ctor %s %s@%d", id(), 0, _waiting->id(),
               rec._kLabel, rec._kFile, rec._kLine);
    push_initial_call(rec);
  } else {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) <=== top-ctor", id());
    push_initial_call(kHdlrAssignEmpty);
  }

  // create entry using current rec
  const_cast<EventCallContext*&>(_waiting) = nullptr; // do *not* use ctor-origin after now

  complete_call("ctxt.ctor");

  ink_release_assert(! _chainPtr->empty()); // now non-zero
}


EventCallContext::~EventCallContext()
{
  complete_call(nullptr) && restart_upper_stamp(nullptr);

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
thread_local uint64_t         EventHdlrState::st_preAllocCounter = 0LL;
thread_local void            *EventHdlrState::st_preCtorObject = nullptr;
thread_local void            *EventHdlrState::st_preCtorObjectEnd = nullptr;

void EventCallContext::clear_ctor_initial_callback() {
  if ( ! st_dfltAssignPoint ) {
    return;
  }

  st_dfltAssignPoint = nullptr;
  Debug(TRACE_DEBUG_FLAG,"pre-setting default");
}

void EventCallContext::set_ctor_initial_callback(EventHdlr_t rec) 
{
  if ( st_dfltAssignPoint == &rec ) {
    return;
  }

  st_dfltAssignPoint = &rec;
  Debug(TRACE_DEBUG_FLAG,"pre-setting %s %s@%d", rec._kLabel, rec._kFile, rec._kLine );
}

EventHdlrState::EventHdlrState(void *p)
   : _assignPoint(&kHdlrAssignEmpty), // no-op at first
     _scopeContext()  // add ctor-record
{
  // many ctors may use this as default.. in an object

  *this = current_default_assign_point();

  // claim bytes grabbed so far ...
  remove_ctor_delta();

  // must return an error...
  auto r = pthread_mutex_unlock(&_scopeContext._chainPtr->_owner);
  ink_release_assert( r ); // assert it did
}

//
// a HdlrState that merely "owns" the top of other calls
//
EventHdlrState::EventHdlrState(EventHdlr_t hdlr)
   : _assignPoint(&hdlr),
     _scopeContext() // add ctor-record
{
  // must be simple/stack created .. so no other ctors left
  EventCallContext::clear_ctor_initial_callback(); 

  // prep for a restart of upper frame...
  const_cast<EventCallContext*&>(_scopeContext._waiting) = EventCallContext::st_currentCtxt; 
  EventCallContext::st_currentCtxt = &_scopeContext;

  // start first call-down *now* [complete upon dtor, if needed]
  _scopeContext.reset_frame(hdlr);
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
  if ( _scopeContext._chainPtr ) { // why test?
    _scopeContext.complete_call("hdlr.dtor");
  }

  if ( EventCallContext::st_currentCtxt != &_scopeContext ) {
    return;
  }

  // account for freeing the object to higher calls
  auto &ctor = _scopeContext._chainPtr->front();
  auto delta = ctor._allocDelta - ctor._deallocDelta;
  EventCallContext::st_currentCtxt = _scopeContext._waiting;
  EventCallContext::remove_mem_delta( id(), -delta ); // 
  EventCallContext::st_currentCtxt = &_scopeContext;
}

void EventCallContext::reset_frame(EventHdlr_t hdlr) 
{
  ink_release_assert( st_currentCtxt == this || ! st_currentCtxt ); // must reset top here

  EventCallContext::st_currentCtxt = this; // must reset top here

  // lock for the next pushed entry (push_call_chain_pair)
  if ( ink_mutex_try_acquire(&_chainPtr->_owner) ) 
  {
    Debug(TRACE_DEBUG_FLAG,"(C#%06x) resetting %s %s@%d",
       id(), hdlr._kLabel, hdlr._kFile, hdlr._kLine );

    complete_call("reset") && restart_upper_stamp("reset");
  } else {
    auto &ochain = *_chainPtr; // save chain (if usable)
    auto nchainPtr = std::make_shared<EventChain>(ochain.id(),ochain._cnt+1); // NOTE: chain adds into alloc tally
    Debug(TRACE_FLAG,"(C#%06x) @#%d --> (C#%06x): event-thread-restart [tot:%ld] %s %s@%d",
       ochain.id(), ochain.back()._i, nchainPtr->id(), ochain._allocTotal - ochain._deallocTotal,
       hdlr._kLabel, hdlr._kFile, hdlr._kLine);

    // keep lock for the next pushed entry (push_call_chain_pair)
    _chainPtr = nchainPtr; // NOTE: chain adds into alloc tally

    push_initial_call(*ochain[0]._hdlrAssign);
  }

  // chain is owned by ctor

  // create entry using current rec (and reset storage stamp)
  push_call_entry(hdlr, EVENT_RESET_FAKE);

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

bool EventCallContext::complete_call(const char *omsg) 
{
  // no need?
  if ( ! is_incomplete() ) {
    return false;
  }

  auto &mutex = _chainPtr->_owner;

  // cannot own this chain?
  if ( ! ink_mutex_try_acquire(&mutex) ) {
    return false;
  }

  // reset accounting for upper if it matters
  // NOTE: has no effect on _waiting
  active_event().completed(*this,( omsg ? : "ctxt.dtor" ));  // complete this record (and adjust caller record)

  ink_mutex_release(&mutex); // balance from top
  return true;
}


bool EventCallContext::restart_upper_stamp(const char *omsg) 
{
  if ( ! _waiting || ! _waiting->is_incomplete() ) {
    return false; // won't reset upper context
  }

  auto &upper = *_waiting;

  if ( ! ink_mutex_try_acquire(&upper._chainPtr->_owner) ) {
    return false; // can't own
  }

  // reset accounting for active upper

  if ( ! omsg ) {
    _chainPtr.reset(); // remove from storage account
  }

  // pass accounting officially to upper context
  const_cast<time_point &>(upper._start) = std::chrono::steady_clock::now();
  const_cast<uint64_t &>(upper._allocStamp) = st_allocCounterRef;
  const_cast<uint64_t &>(upper._deallocStamp) = st_deallocCounterRef;

  ink_mutex_release(&upper._chainPtr->_owner); // balance for upper
  return true;
}


bool EventCallContext::remove_mem_delta(int id, int32_t adj)
{
  char buff[20];
  snprintf(buff,sizeof(buff),"(C#%06x)",id);
  return remove_mem_delta(buff,adj);
}

bool EventCallContext::remove_mem_delta(const char* idstr, int32_t adj)
{
  if ( ! EventCallContext::st_currentCtxt ) {
    DebugSpecific(true,TRACE_FLAG,"remove-mem-delta\n(conttrace           ) " TRACE_SNPRINTF_PREFIX "   %s              [ mem %d ]     NO-ADJUST       topmost", 
       TRACE_SNPRINTF_DATA idstr, -adj );
    return false;
  }

  auto &upper = *EventCallContext::st_currentCtxt;

  if ( ! upper.has_active() ) {
    DebugSpecific(true,TRACE_FLAG,"remove-mem-delta\n(conttrace           ) " TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%02d <-- %s  [ mem %d ]     NO-ADJUST       no active",
       TRACE_SNPRINTF_DATA upper.id(), upper.active_event()._i, idstr, -adj );
    return false;
  }
  auto &active = upper.active_event();
  auto &hdlr = *active._hdlrAssign;

  if ( ! upper.is_incomplete() ) {
    DebugSpecific(true,TRACE_FLAG,"remove-mem-delta\n(conttrace           ) " TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%02d <-- %s [ mem %d ]     NO-ADJUST       under %s %s@%d",
       TRACE_SNPRINTF_DATA  upper.id(), upper.active_event()._i, idstr, -adj, hdlr._kLabel, hdlr._kFile, hdlr._kLine );
    return false;
  }

  return upper.reverse_mem_delta(idstr,adj);
}

bool EventCallContext::reverse_mem_delta(const char* idstr, int32_t adj)
{
  auto &active = active_event();
  auto &hdlr = *active._hdlrAssign;

  active._allocDelta -= ( adj > 0 ? adj : 0 ); // back it up
  active._deallocDelta -= ( adj < 0 ? -adj : 0 ); // back it up

  DebugSpecific(true,TRACE_FLAG,"remove-mem-delta\n(conttrace           ) " TRACE_SNPRINTF_PREFIX "   (C#%06x) @#%02d <-- %s [ mem %d ]     ADJUST       under %s %s@%d",
     TRACE_SNPRINTF_DATA id(), active_event()._i, idstr, -adj, hdlr._kLabel, hdlr._kFile, hdlr._kLine );

  // if incomplete... compensate for the deallocation-so-far (later-ctor stuff is unkn)
  return true;
}

void EventHdlrState::remove_ctor_delta()
{
  if ( this < st_preCtorObject ) {
    return; // new was unrelated to this object
  }
  if ( this >= st_preCtorObjectEnd ) {
    return; // new was unrelated to this object
  }

  // not on stack and not an old alloc

  // inside this alloc
  auto objsize = static_cast<char*>(st_preCtorObjectEnd) - static_cast<char*>(st_preCtorObject);

  // remove object's memory from caller 
  EventCallContext::remove_mem_delta(_scopeContext.id(), objsize);

  // add object's memory to ctor context (already completed)
  _scopeContext.reverse_mem_delta(-objsize);

  {
    std::ostringstream oss;
    _scopeContext._chainPtr->printLog(oss,0,1,"hdlr-adjust.ctor");
    oss.str().empty() || ({ DebugSpecific(true,TRACE_FLAG,"hdlr-adjust.ctor %s",oss.str().c_str()); true; });
  }

  // **don't** allow others to claim the alloc
  st_preCtorObjectEnd = st_preCtorObject; 
}

void
EventCalled::completed(EventCallContext const &ctxt, const char *msg)
{
  if ( _delay ) {
    return; // don't attempt release (can't be locked either)
  }

  auto &chain = *ctxt._chainPtr;
  auto callInd = this - &chain.front();

  auto duration = std::chrono::steady_clock::now() - ctxt._start;
  auto allocTot = int64_t() + EventCallContext::st_allocCounterRef - ctxt._allocStamp;
  auto deallocTot = int64_t() + EventCallContext::st_deallocCounterRef - ctxt._deallocStamp;

  ink_release_assert( ! _delay );

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto prevDiff = _allocDelta - _deallocDelta;
  auto callingID = 0;
  auto calledID = 0;
  auto callingInd = 0;
  auto calledInd = 0;

  if ( has_calling() ) {
    auto &ochain = *_callingChain.lock();
    callingID = ochain.id();
    callingInd = calling()._i;
  }
  if ( has_called() ) {
    calledID = _calledChain->id();
    calledInd = called()._i;
  }

  _allocDelta += allocTot;
  _deallocDelta += deallocTot;

  const char *title = "anon-call";

  if ( _i ) {
    chain._allocTotal += _allocDelta;
    chain._deallocTotal += _deallocDelta;
  } 

  do {
      if ( callingID ) {
        break; // consider calling frame
      }
      if ( _i && ! calledID && ! this[-1]._delay ) {
        callingInd = this[-1]._i;
        callingID = chain.id(); // note self..
        title = "in-chain-call";
        break; // consider calling frame
      }

      if ( calledID && is_frame_rec() ) {
        title = "frame-post-call"; // print log
      } else if ( calledID ) {
        title = "event-post-call"; // print log
      }

      // current or lower 
      if ( _i && ! calledID && this != &chain.back() ) {
        calledID = chain.id(); // note self..
        calledInd = this[1]._i;
        title = "top-call"; // print log
      }

      if ( ! _i ) {
        title = "ctor-call"; // print one-line log
      }

      if ( calledID ) {
        Debug(TRACE_DEBUG_FLAG,"[ (C#%06x) @#%d ] <==> (C#%06x) @#%d called: %s complete [post:%+ld pre:%+ld -> %+ld] [tot:%ld] [%05d] %s %s@%d [%s]",
           chain.id(), _i, calledID, calledInd, title, 
           allocTot - deallocTot, prevDiff, _allocDelta - _deallocDelta, 
           chain._allocTotal - chain._deallocTotal, 
           _event, _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine, msg);
      }
      else
      {
        Debug(TRACE_DEBUG_FLAG,"[ (C#%06x) @#%d ]: %s complete [post:%+ld pre:%+ld -> %+ld] [tot:%ld] [%05d] %s %s@%d [%s]",
           chain.id(), _i, title, 
           allocTot - deallocTot, prevDiff, _allocDelta - _deallocDelta, 
           chain._allocTotal - chain._deallocTotal, _event,
           _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine, msg);
      }

      std::ostringstream oss;
      chain.printLog(oss,callInd,~0U,msg);

      if ( ! oss.str().empty() ) {
        DebugSpecific(true,TRACE_FLAG,"%s calls [%s] %s", title, msg, oss.str().c_str());
      }

      ink_mutex_release(&chain._owner); // now free to go to other threads
      return;

  } while(false);

  // calling context is interesting
  std::ostringstream oss;

  if ( is_frame_rec() ) {
    title = "enter-frame"; // no log yet
  } else if ( ! calling().is_frame_rec() ) {
    title = "sub-event"; 
    // need to log in case a new call is made
    chain.printLog(oss,callInd,~0U,msg); 
  } else { 
    title = "top-event"; // print log
    chain.printLog(oss,callInd,~0U,msg);
  }

  Debug(TRACE_DEBUG_FLAG,"(C#%06x) @#%d <===> [ (C#%06x) @#%d called ]: %s completed [post:%+ld pre:%+ld -> %+ld] [tot:%ld] [%05d] %s %s@%d [%s]",
     callingID, callingInd, chain.id(), _i, title,
     allocTot - deallocTot, prevDiff, 
     _allocDelta - _deallocDelta, chain._allocTotal - chain._deallocTotal,
     _event, _hdlrAssign->_kLabel, _hdlrAssign->_kFile, _hdlrAssign->_kLine, msg);

  if ( ! oss.str().empty() ) {
    DebugSpecific(true,TRACE_FLAG,"%s calls [%s] %s", title, msg, oss.str().c_str());
  }

  // clear up any old calls on this chain now that it's done
  while ( chain.trim_check() ) { 
    ink_release_assert( chain.trim_back() ); 
  }

  ink_mutex_release(&chain._owner); // now free to go to other threads

  // on single shot, release chain's lock too
}

extern "C" {

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

void cb_remove_mem_delta(const char *msg, unsigned n) 
   { EventCallContext::remove_mem_delta(msg, n); }

void cb_allocate_hook(void *p, unsigned n) 
   { EventHdlrState::allocate_hook(p,n); }

const TSEventFunc cb_null_return() { return nullptr; }

void cb_set_ctor_initial_callback(EventCHdlrAssignRecPtr_t crec)
{
  EventCallContext::set_ctor_initial_callback(reinterpret_cast<EventHdlr_t>(*crec));
}

void cb_enable_thread_prof_active()
{
  jemallctl::enable_thread_prof_active(); 
}

int64_t cb_get_thread_alloc_surplus()
{
  return EventCallContext::st_allocCounterRef - EventCallContext::st_deallocCounterRef;
}

jemallctl::SetObjFxn<const char*>  g_prof_dump{"prof.dump"};

void cb_disable_thread_prof_active()
{
  jemallctl::disable_thread_prof_active(); 
}

void cb_flush_thread_prof_active()
{
  g_prof_dump(nullptr);
}

}
