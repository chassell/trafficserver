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

void EventCalled::printLog(Chain_t::const_iterator const &begin, Chain_t::const_iterator const &end, const char *omsg)
{
  ptrdiff_t len = end - begin;
  ptrdiff_t i = len-1;

  ptrdiff_t memAccount = 0;
  double timeAccount = 0;

  for( auto iter = end-1 ; iter > begin ; --i, --iter )
  {
    auto &call = *iter;

    ptrdiff_t memDelta = call._allocDelta;
    memDelta -= call._deallocDelta;

    float delay = call._delay - timeAccount;
    const char *units = ( delay >= 10000 ? "ms" : "us" ); 
    float div = ( delay >= 10000 ? 1000.0 : 1.0 ); 

    const EventHdlrAssignRec &rec = *call._assignPoint;

//    const char *e = ( len == 1 ? "  " 
//                  : i == len-1 ? "~~" 
//                        : ! i ? "!!" 
//                              : "##" );

    const char *debug = "conttrace";
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

    std::string buff;
    const char *msg = omsg;

    if ( call._extCallerChain ) {
      ink_assert( call._extCallerChainLen <= call._extCallerChain->size() );
      auto extCaller = (*call._extCallerChain)[call._extCallerChainLen-1]._assignPoint->_kLabel;
      buff.resize(strlen(msg) + strlen(extCaller) + 10, '\0');
      snprintf(const_cast<char*>(buff.data()),buff.size(),"<-- %s %s",extCaller,msg);
      msg = buff.data();
    }

    std::string callback = rec._kLabel;
    auto cutoff = callback.rfind('&');
    if ( cutoff == callback.npos ) {
      cutoff = callback.rfind(')');
    }
    if ( cutoff == callback.npos ) {
      cutoff = 0;
    }

    callback.erase(0,cutoff);

    if ( len == 1 ) 
    {
      Debug(debug,"                 :%05u[ mem %9ld time ~%5.1f%s]                %s %s@%d %s",
               call._event, memDelta, delay / div, units, callback.c_str(), rec._kFile, rec._kLine, msg);
      return;
    }

    Debug(debug,"  (%ld)             %05u[ mem %9ld (+%9ld) time ~%5.1f%s (%5.1f%s) ] %s %s@%d %s",
           i, call._event, memDelta - memAccount, memAccount, delay / div, units, timeAccount / div, units, 
           callback.c_str(), rec._kFile, rec._kLine, msg);

    memAccount = memDelta;
    timeAccount = call._delay;

    // account not add if called from other chain ...
    if ( call._extCallerChain ) {
      memAccount = 0;
      timeAccount = 0;
    }
  }
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

  if ( (this-1)->_assignPoint != _assignPoint 
       && _assignPoint->_kEqualHdlr_Gen() ) {
    return; // need a flag set ... or a direct repeat is here
  }

  if ( ! _assignPoint->_kEqualHdlr_Gen() ) {
 //   Debug("conttrace","trim #%ld %s", i, _assignPoint->_kLabel);
  }

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

  if ( _eventChainPtr->size() > 1 || _eventChainPtr->back()._assignPoint->_kEqualHdlr_Gen() ) {
    EventCalled::printLog(_eventChainPtr->begin(),_eventChainPtr->end(),"state DTOR");
  }
}

void EventCallContext::push_incomplete_call(EventCalled::ChainPtr_t const &calleeChain, int event) const
{
  calleeChain->push_back( EventCalled(_assignPoint,event) );

  // no change needed
  if ( _currentCallChain == calleeChain ) {

    if ( calleeChain->empty() ) {
      return;
    }

    Debug("conttrace","push-call#%lu: %s %s@%d [%d]",calleeChain->size(),_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
    return;
  }

  auto &next = calleeChain->back();

  next._extCallerChain = _currentCallChain;
  _currentCallChain = calleeChain;

  if ( next._extCallerChain ) 
  {
    next._extCallerChainLen = next._extCallerChain->size();

    if ( ! next._assignPoint->_kEqualHdlr_Gen() ) {
      return;
    }

//    auto &back = next._extCallerChain->back();
//    Debug("conttrace","separate-object-push[#%lu->#%lu]: [%05d] %s %s@%d --> [%05d] %s %s@%d", _extCallerChainLen, _currentCallChain->size(),
//             back._event, back._assignPoint->_kLabel, back._assignPoint->_kFile, back._assignPoint->_kLine,
//             next._event, next._assignPoint->_kLabel, next._assignPoint->_kFile, next._assignPoint->_kLine);
    return;
  } 

  if ( ! _state || ! next._assignPoint->_kEqualHdlr_Gen() ) {
    return;
  }

  Debug("conttrace","starting-push[-->#%lu]: [%05d] %s %s@%d",_currentCallChain->size(),
               next._event, next._assignPoint->_kLabel, next._assignPoint->_kFile, next._assignPoint->_kLine);
}

EventCalled::EventCalled(EventHdlr_t point, int event)
   : _assignPoint(point), // changes to assign-point
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
      EventCalled::printLog(curr.begin(),curr.end(),"top dtor");
      currPtr.reset();
      return nullptr;

    } 
    
    i->trim_call(curr); // snip some if needed

    if ( curr.size() <= ith ) {
      return nullptr; // caller record was trimmed 
    }

    i = curr.begin()+ith; // in case it was deleted

    // size is larger than the index we were at...

    EventCalled::printLog(curr.begin()+ith,curr.end(),"top has completed");

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
     EventCalled::printLog(curr.begin(),curr.end(),"Chain DTOR");
     currPtr.reset();  // freed
     return callerRec;
  }

  i->trim_call(curr); // snip any boring calls from end...
  if ( curr.size() <= ith ) {
    return callerRec;
  }

  EventCalled::printLog(i,curr.end(),"pop");
  return callerRec;
}

EventCallContext::~EventCallContext()
{
  if ( ! _currentCallChain ) {
     Debug("conttrace","top level pop without handler-chain");
     return nullptr;
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
