/**
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
#include "util_types.h"

#include "ts/ts.h"

APICacheKey::APICacheKey(const atscppapi::Url &url, uint64_t offset)
     : TSCacheKey_t(TSCacheKeyCreate()) 
{
   auto str = url.getUrlString();
   str.append(reinterpret_cast<char*>(&offset),sizeof(offset)); // append *unique* position bytes
   TSCacheKeyDigestSet(get(), str.data(), str.size() );
   auto host = url.getHost();
   TSCacheKeyHostNameSet(get(), host.data(), host.size()); 
}


template <typename T_DATA, typename T_REFCOUNTED>
TSCont APICont::create_temp_tscont(std::shared_future<T_DATA> &cbFuture, const T_REFCOUNTED &counted)
{
  std::function<void(TSEvent,void*)> stub;

  // alloc two objs to pass into lambda
  auto contp = std::unique_ptr<APICont>(new APICont(std::move(stub),TSMutexCreate())); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<T_DATA>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref

  cbFuture = prom.get_future().share(); // link to promise

  cont._userCB = std::move( [&cont,&prom,counted](TSEvent evt, void *data) {
              decltype(contp) contp(&cont); // free after this call
              decltype(promp) promp(&prom); // free after this call

              (void) counted; // "use" value here
              prom.set_value(static_cast<T_DATA>(data));
          });

  contp.release(); // owned as ptr in lambda
  promp.release(); // owned as ptr in lambda

  return cont; // convert to TSCont
}

// accepts TSHttpTxn handler functions
template <class T_OBJ, typename T_DATA>
APICont::APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,TSHttpTxn,T_DATA), T_DATA cbdata)
   : TSCont_t(TSContCreate(&APICont::handleTSEvent,TSMutexCreate())) 
{
  // point back here
  TSContDataSet(get(),this);
  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj,funcp,cbdata](TSEvent event, void *evtdata) 
     {
      (obj.*funcp)(event,static_cast<TSHttpTxn>(evtdata),cbdata);
     });
}

// bare Continuation lambda adaptor
APICont::APICont(std::function<void(TSEvent,void*)> &&fxn, TSMutex mutex)
   : TSCont_t(TSContCreate(&APICont::handleTSEvent,mutex)),
     _userCB(fxn)
{
  // point back here
  TSContDataSet(get(),this);
}

int APICont::handleTSEvent(TSCont cont, TSEvent event, void *data) 
{
  APICont *self = static_cast<APICont*>(TSContDataGet(cont));
  ink_assert(self->operator TSCont() == cont);
  self->_userCB(event,data);
  return 0;
}

// Transform continuations
APIXformCont::APIXformCont(std::function<void(TSEvent,TSVConn)> &&fxn, TSHttpTxn txnHndl, TSHttpHookID xformType)
   : TSCont_t(TSTransformCreate(&APIXformCont::handleXformTSEvent,txnHndl)),
     _userXformCB(fxn)
{
  // point back here
  TSContDataSet(get(),this);
  TSHttpTxnHookAdd(txnHndl, xformType, get());
}

int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *) 
{
  APIXformCont *self = static_cast<APIXformCont*>(TSContDataGet(cont));
  ink_assert(self->operator TSCont() == cont);
  self->_userXformCB(event,static_cast<TSVConn>(cont));
  return 0;
}


template TSCont APICont::create_temp_tscont(std::shared_future<TSVConn> &, const std::nullptr_t &);
template TSCont APICont::create_temp_tscont(std::shared_future<TSVConn> &, const std::shared_ptr<class BlockReadXform>&);

//}
