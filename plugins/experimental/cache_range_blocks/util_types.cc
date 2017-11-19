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
#include "ts/InkErrno.h"

APICacheKey::APICacheKey(const atscppapi::Url &url, uint64_t offset)
     : TSCacheKey_t(TSCacheKeyCreate()) 
{
   auto str = url.getUrlString();
   str.append(reinterpret_cast<char*>(&offset),sizeof(offset)); // append *unique* position bytes
   TSCacheKeyDigestSet(get(), str.data(), str.size() );
   auto host = url.getHost();
   TSCacheKeyHostNameSet(get(), host.data(), host.size()); 
}


template <typename T_DATA>
TSCont APICont::create_temp_tscont(TSMutex shared_mutex, std::shared_future<T_DATA> &cbFuture, std::shared_ptr<void> &&countedRV)
{
  // alloc two objs to pass into lambda
  auto contp = std::make_unique<APICont>(shared_mutex); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<T_DATA>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref
  auto counted = countedRV;

  cbFuture = prom.get_future().share(); // link to promise

  // assign new handler
  cont = [&cont,&prom,counted](TSEvent evt, void *data) {
              decltype(contp) contp(&cont); // free after this call
              decltype(promp) promp(&prom); // free after this call

              prom.set_value(static_cast<T_DATA>(data)); // store correctly
               
              // bad ptr?  then call deleter on this!
              auto ptrErr = -reinterpret_cast<intptr_t>(data);
              if ( ptrErr >= 0 && ptrErr < INK_START_ERRNO+1000 ) {
                auto deleter = std::get_deleter<void(*)(void*)>(counted);
                if ( deleter ) {
                  (*deleter)(counted.get());
                }
              }
          };

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
APICont::APICont(TSMutex mutex)
   : TSCont_t(TSContCreate(&APICont::handleTSEvent,mutex))
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
APIXformCont::APIXformCont(TSHttpTxn txnHndl, TSHttpHookID xformType, int64_t limit)
   : TSCont_t(TSTransformCreate(&APIXformCont::handleXformTSEvent,txnHndl)),
     _outputBuff(TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K)),
     _outputRdr( TSIOBufferReaderAlloc(this->_outputBuff.get()) )
{

  // NOTE: delay for OutputVConnGet() to be valid ...
  _xformCB = [this,limit](TSEvent evt, TSVIO event_vio) {
    TSVConn invconn = *this;
    this->_outputVConn = TSTransformOutputVConnGet(invconn);
    TSVConnWrite(this->output(), invconn, this->_outputRdr.get(), limit);
    this->_xformCB = _userXformCB; // replace with users' now

    this->_xformCB(evt,event_vio); // continue on...
  };

  // point back here
  TSContDataSet(get(),this);
  TSHttpTxnHookAdd(txnHndl, xformType, get());
}

int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata) 
{
  APIXformCont *self = static_cast<APIXformCont*>(TSContDataGet(cont));
  ink_assert(self->operator TSVConn() == cont);

  self->_xformCB(event,static_cast<TSVIO>(edata));
  return 0;
}

class BlockStoreXform;

template APICont::APICont(BlockStoreXform &obj, void(BlockStoreXform::*funcp)(TSEvent,TSHttpTxn,decltype(nullptr)), decltype(nullptr));
template TSCont APICont::create_temp_tscont(TSMutex,std::shared_future<TSVConn> &, std::shared_ptr<void> &&);

//}
