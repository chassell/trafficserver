
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

  @section details Details

  Continuations have a handleEvent() method to invoke them. Users
  can determine the behavior of a Continuation by suppling a
  "Hdlr" (member function name) which is invoked
  when events arrive. This function can be changed with the
  "setHandler" method.

  Continuations can be subclassed to add additional state and
  methods.

 */

#ifndef _I_DebugCCont_h_
#define _I_DebugCCont_h_

#include "ts/apidefs.h"

#define MAX_ASSIGN_COUNTER   60

typedef struct EventCHdlrAssignGrp EventCHdlrAssignGrp_t;

typedef struct EventCHdlrAssignRec EventCHdlrAssignRec_t;
typedef const struct EventCHdlrAssignRec *EventCHdlrAssignRecPtr_t;

struct EventCHdlrAssignRec
{
  const EventCHdlrAssignGrp_t *_assignGrp; // static-global for compile
  const char                      *_label;           // at point of assign
  const char                      *_file;            // at point of assign
  uint32_t                        _line:20;         // at point of assign
  uint32_t                        _assignID:8;      // unique for each in compile-object
  const TSEventFunc               _callback;
};

struct EventCHdlrAssignGrp
{
  EventCHdlrAssignRecPtr_t _assigns[MAX_ASSIGN_COUNTER];
  unsigned int          _id;
};

static EventCHdlrAssignGrp_t s_fileAssignGrp = { { (void*)0 }, 0 };

#define STATIC_C_HANDLER_RECORD(_h, name) \
     static const EventCHdlrAssignRec_t name = \
                     {                        \
                       &s_fileAssignGrp,      \
                       ((#_h)+0),             \
                       (__FILE__+0),          \
                       __LINE__,              \
                       __COUNTER__,           \
                       (_h)                   \
                     }

#define TSThreadCreate(func,data)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSThreadCreate(func,data);                      \
             })


#define TSContCreate(func,mutexp)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSContCreate(func,mutexp);                      \
             })

#define TSTransformCreate(func,txnp)                           \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSTransformCreate(func,txnp);                   \
             })

#define TSVConnCreate(func,mutexp)                             \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSVConnCreate(func,mutexp);                     \
             })

#endif
