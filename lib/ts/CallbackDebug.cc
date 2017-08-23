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
  if ( ! _assignPoint ) {
    return true; // don't log
  }

  // do log if memory changed
  if ( _allocDelta || _deallocDelta ) {
    return false; // DO log
  }

  // if flagged .. it's not important enough
  if (  _assignPoint->no_log() ) {
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

    ptrdiff_t memDelta = call._allocDelta;
    memDelta -= call._deallocDelta;

    float delay = call._delay - timeAccount;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    if ( iter->_extCallerChain ) 
    {
      memTotal += memDelta;
      delayTotal += delay;
    }

    const EventHdlrAssignRec &rec = *call._assignPoint;

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

    if ( call._extCallerChain ) 
    {
      auto extCaller = (*call._extCallerChain)[call._extCallerChainLen-1]._assignPoint->_kLabel;

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
    if ( call._extCallerChain ) {
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
  if ( ! _assignPoint ) {
    return; // no constructor-calls
  }

  if ( _allocDelta != _deallocDelta ) {
    return; // need no memory lost
  }

  auto i = (this - &chain.front());

  if ( (this-1)->_assignPoint != _assignPoint && ! no_log() ) {
    return; // go ahead only if a direct-repeat of a boring call
  }

//  if ( no_log() ) {
//   Debug("conttrace","trim #%ld %s", i, _assignPoint->_kLabel);
//  }

  chain.erase( chain.begin() + i);
}


EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
  if ( _eventChainPtr.use_count() > 1 ) {
    return;
  }

  if ( _eventChainPtr->empty() ) {
    return;
  }

  if ( _eventChainPtr->size() == 1 && _eventChainPtr->back().no_log() ) {
    return;
  }

  EventCalled::printLog(_eventChainPtr->begin(),_eventChainPtr->end()," [State DTOR]",this);
}

void EventCallContext::push_incomplete_call(EventCalled::ChainPtr_t const &calleeChain, int event) const
{
  calleeChain->push_back( EventCalled(*_assignPoint,event) );

  if ( ! _currentCallChain ) 
  {
    _currentCallChain = calleeChain;

    if ( _state && ! no_log() ) {
      Debug("conttrace","starting-push[-->#%lu]: [%05d] %s %s@%d",_currentCallChain->size(),
                 event, _assignPoint->_kLabel, _assignPoint->_kFile, _assignPoint->_kLine);
    }

    return;
  }

  // no change needed
  if ( _currentCallChain == calleeChain ) 
  {
    if ( ! calleeChain->empty() ) {
      Debug("conttrace","push-call#%lu: %s %s@%d [%d]",calleeChain->size(),_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
    }

    return;
  }

  // new object-call-chain is needed

  auto &next = calleeChain->back();

  // record it for old record
  next._extCallerChain = _currentCallChain;
  next._extCallerChainLen = _currentCallChain->size();

  // use new chain
  _currentCallChain = calleeChain;

//    if ( no_log() ) {
//      return;
//    }
//
//    auto &back = next._extCallerChain->back();
//    Debug("conttrace","separate-object-push[#%lu->#%lu]: [%05d] %s %s@%d --> [%05d] %s %s@%d", _extCallerChainLen, _currentCallChain->size(),
//             back._event, back._assignPoint->_kLabel, back._assignPoint->_kFile, back._assignPoint->_kLine,
//             event, _assignPoint->_kLabel, _assignPoint->_kFile, _assignPoint->_kLine);
}

EventCalled::EventCalled(EventHdlr_t point, int event)
   : _assignPoint(&point), // changes to assign-point
     _event(event)
{ }

//////////////////////////////////////////
// current chain has a record to complete
//    then change to new change if needed..
//////////////////////////////////////////
EventCalled *
EventCallContext::pop_caller_record()
{
  auto currPtr = _currentCallChain; // save a copy
  auto &curr = *currPtr;
  auto len = curr.size();

  ink_assert( currPtr->size() );

  EventCalled *callerRec = nullptr;

  auto i = curr.end() - 1;
  auto lastReturnLen = i->_intReturnedChainLen;

  if ( lastReturnLen && lastReturnLen < len ) 
  {
    Debug("conttrace","pop-call not found: #%lu: (#%d>>) @%d<<,  ", len-1,
                curr.back()._extCallerChainLen, 
                curr.back()._intReturnedChainLen);
    return nullptr;                                           //// RETURN (confused)
  }

  // caller/call -> return from prev. call
  if ( lastReturnLen == len )
  {
    // scan to shallowest just-completed call
    auto rev = std::find_if(curr.rbegin(), curr.rend(), 
                             [len](EventCalled &c){ return c._intReturnedChainLen != len; });
    // rev-distance from rend() == ind+1 of first match (or 0 if none)
    // rev-distance from rend() == ind of last non-match
    i = curr.begin() + ( curr.rend() - rev );
  }

  i->completed(*this, currPtr);

  // non-zero and not current??
  size_t ith = i - curr.begin();
  auto &ap = *i->_assignPoint;

  // caller was simply from outside all call chains?
  if ( ! i->_extCallerChainLen ) 
  {
    i->_intReturnedChainLen = currPtr->size(); // save return-point call chain length

    // next context is empty
    _currentCallChain.reset();

    if ( currPtr.use_count() <= 1 ) { // orig and my copy
      EventCalled::printLog(curr.begin(),curr.end()," [top DTOR]",_state);
      currPtr.reset();
      return nullptr;

    } 
    
    i->trim_call(curr); // snip some if needed

    if ( curr.size() <= ith ) {
      return nullptr; // caller record was trimmed 
    }

    i = curr.begin()+ith; // in case it was deleted

    // size is larger than the index we were at...

    EventCalled::printLog(curr.begin()+ith,curr.end(),"top has completed",_state);

    // returned from *last* entry?
    Debug("conttrace","top-object #%ld[%lu]: %s %s %d [evt#%05d] (refs=%ld)",ith, curr.size(), 
         ap._kLabel,ap._kFile,ap._kLine, 
         i->_event, currPtr.use_count());

    return nullptr; // no caller record to examine
  }

  {
    auto callerChainPtr = ( i->_extCallerChain ? i->_extCallerChain  // chain-external caller made call
                                               : currPtr );          // chain-internal caller made call
    callerRec = &(*callerChainPtr)[i->_extCallerChainLen-1];
    callerRec->_intReturnedChainLen = callerChainPtr->size(); // save return-point call chain length

    // pop back to earlier context (maybe null)
    _currentCallChain.swap(callerChainPtr);
  }

  /*
  auto &ap2 = *callerRec->_assignPoint;
  Debug("conttrace","pop-object prev-top #%ld[%lu]: %s %s %d [evt#%05d] (refs=%ld)",ith, curr.size(), 
           ap._kLabel,ap._kFile,ap._kLine, 
           i->_event, currPtr.use_count());
  Debug("conttrace","over-object new-top  #%d[%lu]: %s %s %d [evt#%05d] (refs=%ld)",
           i->_extCallerChainLen-1, _currentCallChain->size(), 
           ap2._kLabel,ap2._kFile,ap2._kLine, 
           callerRec->_event, _currentCallChain.use_count());
  */

  // To Be assured...
  i->_intReturnedChainLen = len;

  if ( currPtr.use_count() <= 1 ) // orig and my copy
  { 
     EventCalled::printLog(curr.begin(),curr.end()," [Chain DTOR]",_state);
     currPtr.reset();  // fred
     return callerRec;
  }

  i->trim_call(curr); // snip any boring calls from end...
  if ( curr.size() <= ith ) {
    return callerRec;
  }

  EventCalled::printLog(i,curr.end(),"pop",_state);
  return callerRec;
}

EventCallContext::~EventCallContext()
{
  if ( ! _currentCallChain ) {
     Debug("conttrace","top level pop without handler-chain");
     return;
  }

  // use back-refs to return the actual caller that's now complete
  pop_caller_record();
}

void
EventCalled::completed(EventCallContext const &ctxt, ChainPtr_t const &chain)
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  auto allocTot = ctxt._allocCounterRef - ctxt._allocStamp;
  auto deallocTot = ctxt._deallocCounterRef - ctxt._deallocStamp;

  // NOTE: uses this_thread() to get ptrs fast
  _allocDelta += allocTot;
  _deallocDelta += deallocTot;

  if ( _extCallerChain ) 
  {
    auto &callerRec = (*_extCallerChain)[_extCallerChainLen-1];
    callerRec._allocDelta -= allocTot;
    callerRec._deallocDelta -= deallocTot;
  }

  _intReturnedChainLen = chain->size(); // save return-point call chain length
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