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
#include "ts/DebugCont.h"

#include "ts/jemallctl.h"
#include "ts/ink_assert.h"
#include "ts/Diags.h"

#include <chrono>
#include <string>
#include <algorithm>

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

void EventCalled::printLog(EventChainPtr_t const &chainPtr)
{
  ptrdiff_t len = chainPtr->size();

  for( auto &&call : *chainPtr )
  {
    auto i = &call - &*chainPtr->begin(); 

    const char *units = ( call._delay >= 10000 ? "ms" : "us" ); 
    float div = ( call._delay >= 10000 ? 1000.0 : 1.0 ); 

    const EventHdlrAssignRec &rec = *call._assignPoint;

    if ( ! call._allocDelta && ! call._deallocDelta ) {
      continue;
    }

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

    if ( len == 1 ) 
    {
      Debug(debug,"                 :%05u[ +%9d -%9d ~%5f%s callback %s] [%s:%d]",
                                     call._event, call._allocDelta, call._deallocDelta, call._delay / div, units, rec._kLabel, rec._kFile, rec._kLine);
      continue;
    }

    Debug(debug,"%s(%ld) %05u[ +%9d -%9d ~%5f%s callback %s] [%s:%d]",
           e, i, call._event, call._allocDelta, call._deallocDelta, call._delay / div, units, rec._kLabel, rec._kFile, rec._kLine);
  }
}

void EventHdlrState::push_caller_record(const EventCallContext &ctxt, unsigned event) const
{
  _eventChainPtr->push_back( EventCalled{ctxt, _eventChainPtr, event} );
}

void EventHdlrState::add_stats_leaf(EventCalled &call, const EventCallContext &ctxt)
{
  add_stats(call,ctxt);

  if ( call._allocDelta == call._deallocDelta             // no memory change
             && ! call._assignPoint->_kEqualHdlr_Gen      // marked 
             && &_eventChainPtr->back() == &call ) 
  {
     _eventChainPtr->pop_back();
  }
}

void EventHdlrState::add_stats(EventCalled &call, const EventCallContext &ctxt)
{
  call.completed(ctxt, _eventChainPtr);
}

EventHdlrState::~EventHdlrState()
   // detaches from shared-ptr!
{
}

EventCalled::EventCalled(const EventCallContext &ctxt, EventChainPtr_t const &nextChain, unsigned event)
   : _assignPoint(ctxt._assignPoint), // callee info
     _extCallerChain(),               // dflt
     _event(event),
     _extCallerChainLen(ctxt._currentCallChain ? ctxt._currentCallChain->size() : 0) // caller info (for backtrack)
{
  // no change needed
  if ( ctxt._currentCallChain == nextChain ) {
    Debug("conttrace","push-call#%lu: %s %s %d [%d]",nextChain->size(),_assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);
    return;
  }

  // create new refcount-copy
  auto toswap = nextChain; 

  // [ NOTE: minimizes refcount chks ]

  _extCallerChain.swap(toswap);
  _extCallerChain.swap(ctxt._currentCallChain);

  Debug("conttrace","push-swap-call[#%lu->#%lu]: %s %s %d [%d]",( _extCallerChain ? _extCallerChain->size() : -1 ),ctxt._currentCallChain->size(),
                 _assignPoint->_kLabel,_assignPoint->_kFile,_assignPoint->_kLine, event);

  // this->_currentCallChain == prev toswap
  //       c._extCallerChain == prev this->_currentCallChain 
  //                  toswap == prev c._extCallerChain 
}

EventCalled *
EventCalled::pop_caller_record(const EventCallContext &ctxt)
{
  auto currPtr = ctxt._currentCallChain; // save a copy
  auto &curr = *currPtr;
  auto len = curr.size();

  ink_assert( currPtr->size() );

  EventCalled *callerRec = nullptr;

  auto i = curr.end() - 1;

  // caller/call -> return from prev. call
  if ( curr.back()._intReturnedChainLen == len ) 
  {
    // scan to shallowest just-completed call
    auto rev = std::find_if(curr.rbegin(), curr.rend(), 
                             [len](EventCalled &c){ return c._intReturnedChainLen != len; });
    // rev-distance from rend() == ind+1 of first match (or 0 if none)
    // rev-distance from rend() == ind of last non-match
    i = curr.begin() + ( curr.rend() - rev );
  } 
  // non-zero and not current??
  else if ( curr.back()._intReturnedChainLen ) 
  {
    Debug("conttrace","pop-call not-found: #%lu: (#%d>>) @%d<<,  ", len-1,
                curr.back()._extCallerChainLen, 
                curr.back()._intReturnedChainLen);
    return nullptr; // unclear why final return index is off!
  }

  ptrdiff_t ith = i - curr.begin();

  // caller was outside all call chains?
  if ( ! i->_extCallerChainLen ) 
  {
    i->_intReturnedChainLen = currPtr->size(); // save return-point call chain length

    // next context is empty
    ctxt._currentCallChain.reset();

    auto &ap = *i->_assignPoint;

    Debug("conttrace","pop-top #%ld[%lu]: %s %s %d [%d]",ith, curr.size(), ap._kLabel,ap._kFile,ap._kLine, i->_event);

    if ( currPtr.use_count() == 1 ) {
      printLog(currPtr);
    }

    return nullptr; // no caller record to examine
  }

  auto callerChainPtr = ( i->_extCallerChain ? i->_extCallerChain  // chain-external caller made call
                                             : currPtr );          // chain-internal caller made call
  callerRec = &(*callerChainPtr)[i->_extCallerChainLen];
  callerRec->_intReturnedChainLen = callerChainPtr->size(); // save return-point call chain length

  // pop back to earlier context (maybe null)
  ctxt._currentCallChain = callerChainPtr;

  auto &ap = *i->_assignPoint;
  auto &ap2 = *callerRec->_assignPoint;
  Debug("conttrace","pop prev-top #%ld[%lu]: %s %s %d [%d]",ith, curr.size(),  ap._kLabel,ap._kFile,ap._kLine, i->_event);
  Debug("conttrace","pop new-top  #%d[%lu]: %s %s %d [%d]",i->_extCallerChainLen-1, callerChainPtr->size(), ap2._kLabel,ap2._kFile,ap2._kLine, callerRec->_event);

  if ( ctxt._state ) {
    if ( ! i->_intReturnedChainLen ) {
      ctxt._state->add_stats_leaf(*callerRec,ctxt);
    } else {
      ctxt._state->add_stats(*callerRec,ctxt);
    }
  }

  // to be assured...
  i->_intReturnedChainLen = len;

  if ( currPtr.use_count() == 1 ) {
     printLog(currPtr);
  }

  return callerRec;
}

void
EventCalled::completed(EventCallContext const &ctxt, const EventChainPtr_t &chain )
{
  auto duration = std::chrono::steady_clock::now() - ctxt._start;

  _delay = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  if ( ! _delay ) {
    _delay = FLT_MIN;
  }

  // NOTE: uses this_thread() to get ptrs fast
  _allocDelta = ctxt._allocCounterRef - ctxt._allocStamp;
  _deallocDelta = ctxt._deallocCounterRef - ctxt._deallocStamp;
}

