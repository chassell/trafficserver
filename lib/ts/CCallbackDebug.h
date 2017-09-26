
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

#ifndef _CCallbackDebug_h_
#define _CCallbackDebug_h_

#include "ts/apidefs.h"

#include <alloca.h>

typedef const struct EventCHdlrAssignRec EventCHdlrAssignRec_t, 
                                        *EventCHdlrAssignRecPtr_t;

typedef int(*EventFuncFxnPtr_t)(TSCont contp, TSEvent event, void *edata);
typedef const EventFuncFxnPtr_t (*EventFuncFxnGenPtr_t)(void);

struct EventCHdlrAssignRec
{
  const char                      *_kLabel;   // at point of assign
  const char                      *_kFile;    // at point of assign
  uint32_t                        _kLine;     // at point of assign
  EventFuncFxnPtr_t         const _kCallback;
  EventFuncFxnGenPtr_t         const _pad[4];
};

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct tsapi_contdebug *TSContDebug;

const EventFuncFxnPtr_t cb_null_return();
void              cb_set_ctor_initial_callback(EventCHdlrAssignRecPtr_t crec);
const char       *cb_alloc_plugin_label(const char *path, const char *symbol);
void              cb_enable_thread_prof_active();
void              cb_disable_thread_prof_active();
void              cb_flush_thread_prof_active();
int64_t           cb_get_thread_alloc_surplus();
void              cb_allocate_hook(void *, unsigned n);
void              cb_remove_mem_delta(const char *, unsigned n);

#if defined(__cplusplus)
}
#endif

#ifndef __cplusplus

#define C_EVENT_HANDLER(_h) \
   ({                                       \
     static const EventCHdlrAssignRec_t _kHdlrAssignRec_ = \
                     {                        \
                       ((#_h)+0),             \
                       (__FILE__+0),          \
                       __LINE__,              \
                       (EventFuncFxnPtr_t)(_h),     \
                       { &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return  }   \
                     };                    \
     &_kHdlrAssignRec_;                    \
   })

#define TSThreadCreate(func,data)    TSThreadCreate(C_EVENT_HANDLER(func),data)
#define TSContCreate(func,mutexp)    TSContCreate(C_EVENT_HANDLER(func),mutexp)
#define TSTransformCreate(func,txnp) TSTransformCreate(C_EVENT_HANDLER(func),txnp)
#define TSVConnCreate(func,mutexp)   TSVConnCreate(C_EVENT_HANDLER(func),mutexp)
#endif

#endif
