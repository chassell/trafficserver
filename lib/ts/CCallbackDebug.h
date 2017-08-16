
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

#include <alloca.h>

typedef const struct EventCHdlrAssignRec EventCHdlrAssignRec_t, 
                                        *EventCHdlrAssignRecPtr_t;

typedef const TSEventFunc (*TSEventFuncGen)();

struct EventCHdlrAssignRec
{
  const char                      *_kLabel;   // at point of assign
  const char                      *_kFile;    // at point of assign
  uint32_t                        _kLine;     // at point of assign
  const TSEventFunc               _kCallback;
  const TSEventFuncGen            _pad[4];
};

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tsapi_contdebug *TSContDebug;

const TSEventFunc cb_null_return();
size_t            cb_sizeof_stack_context();
TSContDebug       *cb_init_stack_context(void *, EventCHdlrAssignRecPtr_t);
void              cb_free_stack_context(TSContDebug*);

#ifdef __cplusplus
}
#endif

#define STATIC_C_HANDLER_RECORD(_h, name) \
     static const EventCHdlrAssignRec_t name = \
                     {                        \
                       ((#_h)+0),             \
                       (__FILE__+0),          \
                       __LINE__,              \
                       (TSEventFunc)(_h),     \
                       { &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return  }   \
                     }


#ifndef __cplusplus

#define SET_NEXT_HANDLER_RECORD(_h)                       \
            STATIC_C_HANDLER_RECORD(_h, kHdlrAssignRec);  \
            
#define TSThreadCreate(func,data)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSContDebug *ctxtp = cb_init_stack_context(alloca(cb_sizeof_stack_context()), &kHdlrAssignRec); \
               TSThread t = TSThreadCreate(func,data);         \
               cb_free_stack_context(ctxtp);                   \
               t;                                              \
             })


#define TSContCreate(func,mutexp)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSContDebug *ctxtp = cb_init_stack_context(alloca(cb_sizeof_stack_context()), &kHdlrAssignRec); \
               TSCont c = TSContCreate(func,mutexp);           \
               cb_free_stack_context(ctxtp);                   \
               c;                                              \
             })

#define TSTransformCreate(func,txnp)                           \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSContDebug *ctxtp = cb_init_stack_context(alloca(cb_sizeof_stack_context()), &kHdlrAssignRec); \
               TSVConn r = TSTransformCreate(func,txnp);       \
               cb_free_stack_context(ctxtp);                   \
               r;                                              \
             })

#define TSVConnCreate(func,mutexp)                             \
            ({                                                 \
               STATIC_C_HANDLER_RECORD((func),kHdlrAssignRec); \
               TSContDebug *ctxtp = cb_init_stack_context(alloca(cb_sizeof_stack_context()), &kHdlrAssignRec); \
               TSVConn r = TSVConnCreate(func,mutexp);         \
               cb_free_stack_context(ctxtp);                   \
               r;                                              \
             })
#endif

#endif
