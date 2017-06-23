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

#ifndef _P_UnixEventProcessor_h_
#define _P_UnixEventProcessor_h_

#include "I_EventProcessor.h"
#include "ts/ink_align.h"
#include "ts/ink_atomic.h"

const int LOAD_BALANCE_INTERVAL = 1;

TS_INLINE off_t
EventProcessor::allocate(int size)
{
  static off_t start = INK_ALIGN(offsetof(EThread, thread_private), 16);
  static off_t loss  = start - offsetof(EThread, thread_private);
  size               = INK_ALIGN(size, 16); // 16 byte alignment

  int old;
  do {
    old = thread_data_used;
    if (old + loss + size > PER_THREAD_DATA)
      return -1;
  } while (!ink_atomic_cas(&thread_data_used, old+0, old + size));

  return (off_t)(old + start);
}

TS_INLINE EThread *
EventProcessor::assign_thread(EventType etype)
{
  int next;
  ThreadGroupDescriptor *tg = &thread_group[etype];

  ink_assert(etype < MAX_EVENT_TYPES);
  if (tg->n_threads() > 1) {
    next = tg->next_round_robin() % tg->n_threads();
  } else {
    next = 0;
  }
  return (*tg)[next];
}

#endif
