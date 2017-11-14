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
   : TSCont_t(TSContCreate(&APICont::handleEvent,TSMutexCreate())) 
{
  // point back here
  TSContDataSet(get(),this);
  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj,funcp,cbdata](TSEvent event, void *evtdata) 
     {
      (obj.*funcp)(event,static_cast<TSHttpTxn>(evtdata),cbdata);
     });
}

// callback is "cb(TSEvent, TSVIO, ntodo)"
// can get with TSVConnWrite(), TSVConnRead() :
//		VC_EVENT_WRITE_READY / VC_EVENT_READ_READY
//		VC_EVENT_WRITE_COMPLETE / VC_EVENT_READ_COMPLETE,
//	[wr/rd] VC_EVENT_EOS, VC_EVENT_INACTIVITY_TIMEOUT, VC_EVENT_ACTIVE_TIMEOUT
template <class T_OBJ>
APICont::APICont(T_OBJ &obj, int64_t(T_OBJ::*funcp)(TSEvent,TSVIO,int64_t,int64_t), TSHttpTxn txnHndl)
   : TSCont_t(TSTransformCreate(&APICont::handleEvent,txnHndl))
{
  TSCont cont = get();

  // point back here
  TSContDataSet(cont,this);

  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj,funcp,cont](TSEvent event, void *) 
     {
       if ( TSVConnClosedGet(cont) ) {
         (obj.*funcp)(TS_EVENT_VCONN_EOS,nullptr,-1,-1); // bytes "written" and bytes actually consumed
         return;
       }

       auto input_vio = static_cast<TSVIO>( TSVConnWriteVIOGet(cont) );

       auto r = 0;
       auto ndone = TSVIONDoneGet(input_vio);

       if ( event != TS_EVENT_VCONN_READ_READY && event != TS_EVENT_IMMEDIATE ) 
       {
         (obj.*funcp)(event,input_vio,ndone,-1); // bytes "written" and bytes actually consumed
         return;
       }

       if ( ! TSVIOBufferGet(input_vio) ) {
         return;
       }

       auto inreader = TSIOBufferReader(TSVIOReaderGet(input_vio));

       // total amt left and amt available

       auto avail = TSIOBufferReaderAvail(inreader);
       avail = std::min(avail+0, TSVIONTodoGet(input_vio));

       r = (obj.*funcp)(event,input_vio,ndone,avail); // bytes "written" and bytes actually consumed
       if ( r <= 0 || r > avail ) {
         return;
       }

       TSIOBufferReaderConsume(inreader,r); // consume data
       TSVIONDoneSet(input_vio, ndone + r ); // inc offset

       auto evt = ( TSVIONTodoGet(input_vio) > 0 ? TS_EVENT_VCONN_WRITE_READY
                                                 : TS_EVENT_VCONN_WRITE_COMPLETE );
       TSContCall(TSVIOContGet(input_vio), evt, input_vio);
     });
}

int APICont::handleEvent(TSCont cont, TSEvent event, void *data) 
{
  APICont *self = static_cast<APICont*>(TSContDataGet(cont));
  ink_assert(self->operator TSCont() == cont);
  self->_userCB(event,data);
  return 0;
}

APICont::APICont(std::function<void(TSEvent,void*)> &&fxn, TSMutex mutex)
   : TSCont_t(TSContCreate(&APICont::handleEvent,mutex)),
     _userCB(fxn)
{
  // point back here
  TSContDataSet(get(),this);
}

template TSCont APICont::create_temp_tscont(std::shared_future<TSVConn> &, const std::nullptr_t &);
template TSCont APICont::create_temp_tscont(std::shared_future<TSVConn> &, const std::shared_ptr<class BlockReadXform>&);

//}
