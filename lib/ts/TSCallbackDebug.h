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

#ifndef _I_TSCallbackDebug_h_
#define _I_TSCallbackDebug_h_

#include "ts/CallbackDebug.h"
#include "ts/apidefs.h"
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

#define TSContCreate(func,mutexp)    TSContCreate(&EVENT_HANDLER(func),mutexp)
#define TSVConnCreate(func,mutexp)   TSVConnCreate(&EVENT_HANDLER(func),mutexp)
#define TSTransformCreate(func,txnp) TSTransformCreate(&EVENT_HANDLER(func),txnp)

// standard TSAPI event handler signature
template <EventFuncFxnPtr_t FXN>
struct EventHdlrAssignRec::const_cb_callgen<EventFuncFxnPtr_t,FXN>
   : public const_cb_callgen_base
{
  static EventFuncCompare_t *cmpfunc(void) {
    return [](EventFuncFxnPtr_t fxn) { return FXN == fxn; };
  }
  static EventFuncFxnPtr_t func(void)
  {
    return [](TSCont cont, TSEvent event, void *data) {
       return (*FXN)(cont,event,data);
     };
  }
};

// extra case that showed up
template <void(*FXN)(TSCont,TSEvent,void *data)>
struct EventHdlrAssignRec::const_cb_callgen<void(*)(TSCont,TSEvent,void *data),FXN>
   : public const_cb_callgen_base
{
  static EventFuncCompare_t *cmpfunc(void) {
    return [](EventFuncFxnPtr_t fxn) { 
      return reinterpret_cast<EventFuncFxnPtr_t>(FXN) == fxn; 
    };
  }
  static EventFuncFxnPtr_t func(void)
  {
    return [](TSCont cont, TSEvent event, void *data) {
       (*FXN)(cont,event,data);
       return 0;
     };
  }
};


// thread functs should not be wrapped currently at all
template <TSThreadFunc FXN>
struct EventHdlrAssignRec::const_cb_callgen<TSThreadFunc,FXN>
   : public const_cb_callgen_base
{
};

#endif
