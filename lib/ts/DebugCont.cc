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

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////


unsigned EventHdlrAssignGrp::s_hdlrAssignGrpCnt = 0;
const EventHdlrAssignGrp *EventHdlrAssignGrp::s_assignGrps[EventHdlrAssignRec::MAX_ASSIGNGRPS] = { nullptr };

inline unsigned int EventHdlrAssignGrp::add_if_unkn(const EventHdlrAssignRec &rec) 
{
   if ( ! _assigns[rec.id()] ) {
      _assigns[rec.id()] = &rec; // should be room
   }
   return _id;
}

inline unsigned int EventHdlrAssignRec::grpid() const
{ 
  return _assignGrp.add_if_unkn(*this); 
}

inline EventHdlrAssignRec::Ptr_t 
EventHdlrAssignGrp::lookup(const EventHdlrID &rec)
{
  return s_assignGrps[rec._assignGrpID]->_assigns[rec._assignID];
}

void EventHdlrAssignGrp::printLog(void *ptr, EventHdlrLogPtr logPtr, EventHdlrChainPtr chainPtr)
{
  unsigned len = static_cast<unsigned>(logPtr->size());

  ink_release_assert( len == chainPtr->size() );

  for( unsigned i = 0 ; i < len ; ++i )
  {
     auto &stats = (*logPtr)[i];
     auto &id = (*chainPtr)[i];

     const char *units = ( stats._delay >= 10000 ? "ms" : "us" ); 
     float div = ( stats._delay >= 10000 ? 1000.0 : 1.0 ); 

     EventHdlrAssignRec::Ptr_t recp = id; // convert
     if ( ! recp ) {
       Debug("conttrace","%p:%05u[[ +%9ld -%9ld %5f%s unkn: %d/%d]",
             ptr, id._event, stats._allocDelta, stats._deallocDelta, stats._delay / div, units, id._assignID, id._assignGrpID);
       continue;
     }

     if ( ! stats._allocDelta && ! stats._deallocDelta ) {
        continue;
     }

     const char *e = ( len == 1 ? "  " 
                    : i == len-1 ? "~~" 
                          : ! i ? "!!" 
                                : "##" );
 

     const EventHdlrAssignRec &rec = *recp;
     const char *debug = "conttrace";
     std::string label = rec._label;

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
       Debug(debug,"                 :%05u[ +%9ld -%9ld ~%5f%s callback %s] [%s:%d]",
                                       id._event, stats._allocDelta, stats._deallocDelta, stats._delay / div, units, rec._label, rec._file, rec._line);
       continue;
     }

     Debug(debug,"%s(%u) %p:%05u[ +%9ld -%9ld ~%5f%s callback %s] [%s:%d]",
             e, i, ptr, id._event, stats._allocDelta, stats._deallocDelta, stats._delay / div, units, rec._label, rec._file, rec._line);
  }
}

void EventHdlrAssignRec::set() const
{
  // assign to Thread field
}

int
EventHdlrState::operator()(TSCont ptr, TSEvent event, void *data)
{
  return EventHdlrState::operator()(reinterpret_cast<Continuation*>(ptr),static_cast<TSEvent>(event), data);
}

int
EventHdlrState::operator()(Continuation *self,int event, void *data)
{
  void *ptr = self;

  auto logPtr = ( _handlerLogPtr ? : std::make_shared<EventHdlrLog>() );
  auto chainPtr = ( _handlerChainPtr ? : std::make_shared<EventHdlrChain>() );
  auto &log = *logPtr;
  auto &chain = *chainPtr;
  auto &cbrec = *_handlerRec;

  auto called = std::chrono::steady_clock::now();

  auto alloced = *jemallctl::thread_allocatedp();
  auto dealloced = *jemallctl::thread_deallocatedp();

  auto r = (*cbrec._wrapHdlr_Gen())(self,event,data);

  auto duration = std::chrono::steady_clock::now() - called;
  float span = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  EventHdlrLogRec stats{ *jemallctl::thread_allocatedp() - alloced,
                         *jemallctl::thread_deallocatedp() - dealloced,
                         span
                       };
  
  EventHdlrID id{
                  cbrec.id(),
                  cbrec.grpid(),
                  static_cast<unsigned>(event)
               };

  log.push_back(stats);
  chain.push_back(id);

  // deleted continuation?  call a printer to log it / compile it
  if ( logPtr.use_count() > 1 ) {
    return r;
  }

  cbrec._assignGrp.printLog(ptr,std::move(logPtr), std::move(chainPtr));
  return r;
}
