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

  P_UnixEThread.h



*****************************************************************************/
#ifndef _P_UnixEThread_h_
#define _P_UnixEThread_h_

#include "I_EThread.h"
#include "I_EventProcessor.h"

const int DELAY_FOR_RETRY = HRTIME_MSECONDS(10);

Event *DedicatedEThread::schedule_local(Event *e)
    { return ::eventProcessor.assign_thread(ET_CALL)->schedule(e); }

TS_INLINE EThread *
this_ethread()
{
  // XXX not type-safe
  return static_cast<EThread*>(this_thread());
}

TS_INLINE void
EThread::free_event(Event *e)
{
  ink_assert(!e->in_the_priority_queue && !e->in_the_prot_queue);
  EVENT_FREE(e, eventAllocator, this);
}

#endif /*_EThread_h_*/
