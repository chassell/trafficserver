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

#ifndef _P_UnixEvent_h_
#define _P_UnixEvent_h_

#include "P_Continuation.h"

inline Event *Event::set_thread(EThread *ethr) 
{
  ethread_ = ethr;
  if ( contptr_ && mutex() != contptr_->mutex() ) 
  {
    // assign the default (thread's mutex)
    contptr_->set_mutex(mutex());
  }
  return this;
}

TS_INLINE Event *
Event::pre_schedule(int callevent, ink_hrtime atimeout_at, ink_hrtime aperiod)
{
  if ( ethread() && in_the_priority_queue) {
    ethread()->EventQueue.remove(this);
  }
  callback_event = callevent;
  timeout_at   = atimeout_at;
  period       = aperiod;
  immediate    = !period && !atimeout_at;
  return this;
}

TS_INLINE Event *
Event::init(Continuation *c, EThread *eth)
{
  contptr_     = c;
  cancelled    = false;
  set_thread(eth);
#ifdef ENABLE_TIME_TRACE
  if ( immediate ) {
    start_time = Thread::get_hrtime();
  }
#endif
  return this;
}

TS_INLINE void
Event::free()
{
  this->~Event(); // releases all references
  eventAllocator.free(this);
}

TS_INLINE
Event::Event() : ActionBase(contptr_), in_the_prot_queue(false), in_the_priority_queue(false), immediate(false), globally_allocated(true), in_heap(false)
{
}

inline Event *Event::schedule_imm(int callback_event)
   { ethread()->schedule_local_checked( pre_schedule(callback_event,0,0) ); return this; }

inline Event *Event::schedule_at(ink_hrtime atimeout_at, int callback_event)
   { ethread()->schedule_local_checked( pre_schedule(callback_event,atimeout_at,0) ); return this; }

inline Event *Event::schedule_in(ink_hrtime atimeout_in, int callback_event)
   { ethread()->schedule_local_checked( pre_schedule(callback_event, Thread::get_hrtime()+atimeout_in, 0) ); return this; }

inline Event *Event::schedule_every(ink_hrtime aperiod, int callback_event)
{ 
  auto first = ( aperiod < 0 ? aperiod : Thread::get_hrtime() + aperiod );
  ethread()->schedule_local_checked(pre_schedule(callback_event, first, aperiod ));
  return this;
}

inline Event *Event::schedule_imm_global(int callback_event)
   { return ethread()->schedule( pre_schedule(callback_event,0,0) ); }

inline Event *Event::schedule_imm_signal_global(int callback_event)
   { return ethread()->schedule_signal( pre_schedule(callback_event,0,0) ); }

inline Event *Event::schedule_at_global(ink_hrtime atimeout_at, int callback_event)
   { return ethread()->schedule( pre_schedule(callback_event,atimeout_at,0) ); }

inline Event *Event::schedule_in_global(ink_hrtime atimeout_in, int callback_event)
   { return ethread()->schedule( pre_schedule(callback_event, Thread::get_hrtime()+atimeout_in, 0) ); }

inline Event *Event::schedule_every_global(ink_hrtime aperiod, int callback_event)
   { return ethread()->schedule(pre_schedule(callback_event, Thread::get_hrtime()+aperiod, aperiod )); }

#endif /*_UnixEvent_h_*/
