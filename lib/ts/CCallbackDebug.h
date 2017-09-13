
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

typedef const TSEventFunc (*TSEventFuncGen)();

struct EventCHdlrAssignRec
{
  const char                      *_kLabel;   // at point of assign
  const char                      *_kFile;    // at point of assign
  uint32_t                        _kLine;     // at point of assign
  TSEventFunc               const _kCallback;
  TSEventFuncGen            const _pad[4];
};

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct tsapi_contdebug *TSContDebug;

const TSEventFunc cb_null_return();
void              cb_set_ctor_initial_callback(EventCHdlrAssignRecPtr_t crec);

#if defined(__cplusplus)
}
#endif

#ifndef __cplusplus

#define STATIC_C_HANDLER_RECORD(_fxn, name) \
     static const EventCHdlrAssignRec_t name = \
                     {                        \
                       ((#_fxn)+0),             \
                       (__FILE__+0),          \
                       __LINE__,              \
                       (TSEventFunc)(_fxn),     \
                       { &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return,     \
                         &cb_null_return  }   \
                     }


#define SET_NEXT_HANDLER_RECORD(_fxn)                       \
            STATIC_C_HANDLER_RECORD(_fxn, kHdlrAssignRec);  \
            
#define TSThreadCreate(_fxn,data)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD(_fxn,kHdlrAssignRec);   \
               cb_set_ctor_initial_callback(&kHdlrAssignRec);  \
               TSThreadCreate((_fxn),data);                    \
             })

#define TSContCreate(_fxn,mutexp)                              \
            ({                                                 \
               STATIC_C_HANDLER_RECORD(_fxn,kHdlrAssignRec);   \
               cb_set_ctor_initial_callback(&kHdlrAssignRec);  \
               TSContCreate(_fxn,mutexp);                      \
             })

#define TSTransformCreate(_fxn,txnp)                           \
            ({                                                 \
               STATIC_C_HANDLER_RECORD(_fxn,kHdlrAssignRec);   \
               cb_set_ctor_initial_callback(&kHdlrAssignRec);  \
               TSTransformCreate(_fxn,txnp);                   \
             })

#define TSVConnCreate(_fxn,mutexp)                             \
            ({                                                 \
               STATIC_C_HANDLER_RECORD(_fxn,kHdlrAssignRec);   \
               cb_set_ctor_initial_callback(&kHdlrAssignRec);  \
               TSVConnCreate(_fxn,mutexp);                     \
             })
#endif

#endif
