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
#include "ts/jemallctl.h"

#include "P_EventSystem.h"
#include "I_Thread.h"
#include "ts/ink_string.h"

#include <chrono>
#include <string>

///////////////////////////////////////////////
// Common Interface impl                     //
///////////////////////////////////////////////

static ink_thread_key init_thread_key();

ProxyMutex *global_mutex                          = NULL;
ink_hrtime Thread::cur_time                       = 0;
inkcoreapi ink_thread_key Thread::thread_data_key = init_thread_key();

Thread::Thread()
{
  mutex     = new_ProxyMutex();
  mutex_ptr = mutex;
  MUTEX_TAKE_LOCK(mutex, (EThread *)this);
  mutex->nthread_holding = THREAD_MUTEX_THREAD_HOLDING;
}

static void
key_destructor(void *value)
{
  (void)value;
}

ink_thread_key
init_thread_key()
{
  ink_thread_key_create(&Thread::thread_data_key, key_destructor);
  return Thread::thread_data_key;
}

///////////////////////////////////////////////
// Unix & non-NT Interface impl              //
///////////////////////////////////////////////

struct thread_data_internal {
  ThreadFunction f;
  void *a;
  Thread *me;
  char name[MAX_THREAD_NAME_LENGTH];
};

static void *
spawn_thread_internal(void *a)
{
  thread_data_internal *p = (thread_data_internal *)a;

  jemallctl::set_thread_arena(0); // default init first

  p->me->set_specific();
  ink_set_thread_name(p->name);
  if (p->f)
    p->f(p->a);
  else
    p->me->execute();
  ats_free(a);
  return NULL;
}

ink_thread
Thread::start(const char *name, size_t stacksize, ThreadFunction f, void *a)
{
  thread_data_internal *p = (thread_data_internal *)ats_malloc(sizeof(thread_data_internal));

  p->f  = f;
  p->a  = a;
  p->me = this;
  memset(p->name, 0, MAX_THREAD_NAME_LENGTH);
  ink_strlcpy(p->name, name, MAX_THREAD_NAME_LENGTH);
  tid = ink_thread_create(spawn_thread_internal, (void *)p, 0, stacksize);

  return tid;
}

uint64_t &Thread::alloc_bytes_count_direct() 
{ 
  return *jemallctl::thread_allocatedp(); 
}

uint64_t &Thread::dealloc_bytes_count_direct() 
{ 
  return *jemallctl::thread_deallocatedp(); 
}

unsigned EventHdlrAssignGrp::s_hdlrAssignGrpCnt = 0;
const EventHdlrAssignGrp *EventHdlrAssignGrp::s_assignGrps[MAX_ASSIGNGRPS] = { nullptr };

struct EventHdlrLogRec
{
  size_t       _allocDelta:32;
  size_t       _deallocDelta:32;

  float        _delay;
};

struct EventHdlrID
{
  unsigned int _assignID:8;
  unsigned int _assignGrpID:8;
  unsigned int _event:16;

  operator Continuation::Hdlr_t() const { return EventHdlrAssignGrp::lookup(*this); }
};

int
Continuation::handleEvent(int event, void *data)
{
  void *ptr = this;

  auto logPtr = ( _handlerLogPtr ? : std::make_shared<EventHdlrLog>() );
  auto chainPtr = ( _handlerChainPtr ? : std::make_shared<EventHdlrChain>() );
  auto &log = *logPtr;
  auto &chain = *chainPtr;
  auto &cbrec = *_handlerRec;

  auto called = std::chrono::steady_clock::now();
  auto alloced = Thread::alloc_bytes_count(); 
  auto dealloced = Thread::dealloc_bytes_count();

  auto r = (*cbrec._cbGenerator())(this,event,data);

  auto duration = std::chrono::steady_clock::now() - called;
  float span = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();

  EventHdlrLogRec stats{ this_thread()->alloc_bytes_count() - alloced,
                     this_thread()->dealloc_bytes_count() - dealloced,
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

EventHdlrAssignGrp::EventHdlrAssignGrp() 
   : _id(s_hdlrAssignGrpCnt++) 
{
   ink_release_assert( _id < MAX_ASSIGNGRPS );
   s_assignGrps[_id] = this; // should be room
}

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

inline Continuation::Hdlr_t 
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

     Continuation::Hdlr_t recp = id; // convert
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
