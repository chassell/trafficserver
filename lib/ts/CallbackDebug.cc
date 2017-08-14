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

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

void EventCalled::printLog(Chain_t::const_iterator const &begin, Chain_t::const_iterator const &end, const char *msg)
{
  ptrdiff_t len = end - begin;
  ptrdiff_t i = 0;

  for( auto iter = begin ; iter != end ; ++i, ++iter )
  {
    auto &call = *iter;

    const char *units = ( call._delay >= 10000 ? "ms" : "us" ); 
    float div = ( call._delay >= 10000 ? 1000.0 : 1.0 ); 

    const EventHdlrAssignRec &rec = *call._assignPoint;

    const char *e = ( len == 1 ? "  " 
                  : i == len-1 ? "~~" 
                        : ! i ? "!!" 
                              : "##" );

    const char *debug = "conttrace";
    std::string label = rec._kLabel;

    do {
       auto colon = label.rfind("::");
       if ( colon != label.npos ) { 
         label.erase(colon); // to the end
       }

       auto amp = label.rfind('&');
       if ( amp == label.npos ) {
         break;
       }

       label.erase(0,amp+1); // from the start
       label.insert(0,"cont_");
       debug = label.c_str();
    } while(false);

    std::string buff;

    if ( call._extCallerChain && call._extCallerChainLen ) {
      auto extCaller = (*call._extCallerChain)[call._extCallerChainLen-1]._assignPoint->_kLabel;
      buff.resize(100);
      snprintf(const_cast<char*>(buff.data()),buff.size(),"<-- %s %s",extCaller,msg);
      msg = buff.data();
    }

    if ( len == 1 ) 
    {
      Debug(debug,"                 :%05u[ +%9d -%9d ~%5f%s callback %s] [%s:%d] %s",
                                     call._event, call._allocDelta, call._deallocDelta, call._delay / div, units, rec._kLabel, rec._kFile, rec._kLine, msg);
      continue;
    }

    Debug(debug,"%s(%ld) %05u[ +%9d -%9d ~%5f%s callback %s] [%s:%d] %s",
           e, i, call._event, call._allocDelta, call._deallocDelta, call._delay / div, units, rec._kLabel, rec._kFile, rec._kLine, msg);
  }
}

void EventHdlrState::push_caller_record(const EventCallContext &ctxt, int event) const
{
  _eventChainPtr->push_back( EventCalled{ctxt, _eventChainPtr, event} );
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
    EventCalled::printLog(_eventChainPtr->begin(),_eventChainPtr->end(),"state dtor");
  }
}

EventCalled::EventCalled(const EventCallContext &ctxt, ChainPtr_t const &calleeChain, int event)
   : _assignPoint(ctxt._assignPoint), // callee info [if set]
     _extCallerChain(),               // dflt
     _event(event),
     _extCallerChainLen() // caller info (for backtrack)
{
  // no change needed
  if ( ctxt._currentCallChain == calleeChain ) {
    if ( ! calleeChain->empty() ) {
      Debug("conttrace","push-call#%lu: %s %s %d [%d]",calleeChain->size(),_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
    }
    return;
  }

  // create new refcount-copy
  auto toswap = calleeChain;

  // [ NOTE: minimizes refcount chks ]

  _extCallerChain.swap(toswap);
  _extCallerChain.swap(ctxt._currentCallChain);

  // this->_currentCallChain --> prev toswap
  //       c._extCallerChain --> prev this->_currentCallChain 
  //                  toswap --> prev c._extCallerChain 

  if ( _extCallerChain ) 
  {
    _extCallerChainLen = _extCallerChain->size();

    if ( ! _assignPoint->_kEqualHdlr_Gen() ) {
      return;
    }

    auto &back = _extCallerChain->back();

    Debug("conttrace","separate-object-push[#%lu->#%lu]: %s %s %d --> %s %s %d [%d]", _extCallerChain->size(), ctxt._currentCallChain->size(),
             back._assignPoint->_kLabel, back._assignPoint->_kFile, back._assignPoint->_kLine,
             _assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
    return;
  } 

  if ( ! ctxt._state ) {
    return;
  }

  ink_stack_trace_dump();
  Debug("conttrace","starting-push[-->#%lu]: %s %s %d [%d]",ctxt._currentCallChain->size(),
               _assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
}

//////////////////////////////////////////
// current chain has a record to complete
//    then change to new change if needed..
//////////////////////////////////////////
EventCalled *
EventCalled::pop_caller_record(const EventCallContext &ctxt)
{
  auto currPtr = ctxt._currentCallChain; // save a copy
  auto &curr = *currPtr;
  auto len = curr.size();

  ink_assert( currPtr->size() );

  EventCalled *callerRec = nullptr;

  auto i = curr.end() - 1;
  auto lastReturnLen = i->_intReturnedChainLen;

  if ( lastReturnLen && lastReturnLen < len ) 
  {
    Debug("conttrace","pop-call not-found: #%lu: (#%d>>) @%d<<,  ", len-1,
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

  // may delete the entry that completed
  i->completed(ctxt, currPtr);

  // non-zero and not current??
  ptrdiff_t ith = i - curr.begin();
  auto &ap = *i->_assignPoint;

  // caller was simply from outside all call chains?
  if ( ! i->_extCallerChainLen ) 
  {
    i->_intReturnedChainLen = currPtr->size(); // save return-point call chain length

    // next context is empty
    ctxt._currentCallChain.reset();

    if ( currPtr.use_count() <= 1 ) { // orig and my copy
      EventCalled::printLog(curr.begin(),curr.end(),"top dtor");
      currPtr.reset();
      return nullptr;

    } 
    
    i->trim_call(curr); // snip some if needed

    if ( curr.size() <= ith ) {
      return nullptr; // no caller record to examine
    }

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
    ctxt._currentCallChain.swap(callerChainPtr);
  }

  /*
  auto &ap2 = *callerRec->_assignPoint;
  Debug("conttrace","pop-object prev-top #%ld[%lu]: %s %s %d [evt#%05d] (refs=%ld)",ith, curr.size(), 
           ap._kLabel,ap._kFile,ap._kLine, 
           i->_event, currPtr.use_count());
  Debug("conttrace","over-object new-top  #%d[%lu]: %s %s %d [evt#%05d] (refs=%ld)",
           i->_extCallerChainLen-1, ctxt._currentCallChain->size(), 
           ap2._kLabel,ap2._kFile,ap2._kLine, 
           callerRec->_event, ctxt._currentCallChain.use_count());
  */

  // To Be assured...
  i->_intReturnedChainLen = len;

  if ( currPtr.use_count() <= 1 ) { // orig and my copy
     printLog(curr.begin(),curr.end(),"DTOR");
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

void
EventCalled::completed(EventCallContext const &ctxt, const ChainPtr_t &chain )
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  // NOTE: uses this_thread() to get ptrs fast
  _allocDelta = ctxt._allocCounterRef - ctxt._allocStamp;
  _deallocDelta = ctxt._deallocCounterRef - ctxt._deallocStamp;

  _intReturnedChainLen = chain->size(); // save return-point call chain length
}

