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
#include "utils_internal.h"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

void forward_vio_event(TSEvent event, TSVIO vio);

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
TSCont APICont::create_temp_tscont(TSMutex shared_mutex, std::shared_future<T_DATA> &cbFuture, const std::shared_ptr<void> &counted)
{
  // alloc two objs to pass into lambda
  auto contp = std::make_unique<APICont>(shared_mutex); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<T_DATA>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref

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
APICont::APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,void*,T_DATA), T_DATA cbdata)
   : TSCont_t(TSContCreate(&APICont::handleTSEventCB,TSMutexCreate())) 
{
  // point back here
  TSContDataSet(get(),this);

  static_cast<void>(cbdata);
  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj,funcp,cbdata](TSEvent event, void *evtdata) {
      (obj.*funcp)(event,evtdata,cbdata);
     });
}

// bare Continuation lambda adaptor
APICont::APICont(TSMutex mutex)
   : TSCont_t(TSContCreate(&APICont::handleTSEventCB,mutex))
{
  // point back here
  TSContDataSet(get(),this);
}


// Transform continuations
APIXformCont::APIXformCont(atscppapi::Transaction &txn, TSHttpHookID xformType, int64_t len, int64_t offset)
   : TSCont_t(TSTransformCreate(&APIXformCont::handleXformTSEventCB,static_cast<TSHttpTxn>(txn.getAtsHandle()))), 
     _txn(txn), 
     _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
     _outBufferP(TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K)),
     _outReaderP( TSIOBufferReaderAlloc(this->_outBufferP.get()))
{
  // #4) xform handler: skip trailing download from server
  auto skipBodyEndFn = [this](TSEvent evt, TSVIO vio, int64_t left) { 
    auto r = this->skip_next_len( this->call_body_handler(evt,vio,left) ); // last one...
    return r;
  };

  // #3) xform handler: copy body required by client from server
  auto copyBodyFn = [this,len,skipBodyEndFn](TSEvent evt, TSVIO vio, int64_t left) { 
    auto r = this->copy_next_len( this->call_body_handler(evt,vio,left) );
    this->set_copy_handler(len,skipBodyEndFn);
    return r;
  };

  // #2) xform handler: skip an offset of bytes from server
  auto skipBodyOffsetFn = [this,offset,copyBodyFn](TSEvent evt, TSVIO vio, int64_t left) { 
    auto r = this->skip_next_len( this->call_body_handler(evt,vio,left) );
    this->set_copy_handler(offset,copyBodyFn);
    return r;
  };

  // #1) xform handler: copy headers through
  auto copyHeadersFn = [this,skipBodyOffsetFn](TSEvent evt, TSVIO vio, int64_t left) { 
    // copy headers simply...
    auto r = this->copy_next_len(left); 
    this->set_copy_handler(_outHeaderLen,skipBodyOffsetFn);
    return r;
  };

  // #0) handler: init values once
  auto initFldsFn = [this,len,copyHeadersFn](TSEvent evt, TSVIO vio, int64_t left)
  {
    // finally know the length of resp-hdr (as is)
    this->_outHeaderLen = this->_txn.getServerResponseHeaderSize();

    TSVConn invconn = *this;
    this->_inVIO = TSVConnWriteVIOGet(invconn);
    this->_inReader = TSIOBufferReader(TSVIOBufferGet(this->_inVIO));

    // finally initialize output write
    this->_outVConn = TSTransformOutputVConnGet(invconn);
    TSVConnWrite(this->_outVConn, invconn, this->_outReaderP.get(), this->_outHeaderLen + len);

    this->set_copy_handler(this->_outHeaderLen, copyHeadersFn);
    return 0;
  };

  set_copy_handler(0L, initFldsFn);

  // point back here
  TSContDataSet(get(),this);
  TSHttpTxnHookAdd(_atsTxn, xformType, get());
}


int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  ink_assert(this->operator TSVConn() == cont);

  // more rude shutdown
  if ( TSVConnWriteVIOGet(*this) != _inVIO || ! TSVIOBufferGet(_inVIO) ) {
    _xformCB(event, nullptr, 0);
    return 0;
  }

  if ( TSVConnClosedGet(*this) ) {
    _xformCB(event, nullptr, 0);
    return 0;
  }

  // "ack" end-of-write completion [zero bytes] to upstream
  if ( ! TSVIONTodoGet(_inVIO) ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,_inVIO); // complete upstream
    return 0;
  }

  auto pos = TSVIONDoneGet(_inVIO);
  auto buffReady = TSIOBufferReaderAvail(_inReader);

  if ( event == TS_EVENT_IMMEDIATE ) {
    event = TS_EVENT_VCONN_WRITE_READY;
  }

  for(;;) {
    buffReady = std::min(buffReady,_xformCBAbsLimit - pos);

    /////////////
    // perform real callback if data is ready
    /////////////
    
    if ( event == TS_EVENT_VCONN_WRITE_READY && edata == _inVIO ) {
      _xformCB(event, _inVIO, buffReady); // send bytes-left in segment
    }

    if ( pos < _xformCBAbsLimit || pos > _nextXformCBAbsLimit ) {
      break;
    }

    if ( _xformCB.target<void(*)()>() == _nextXformCB.target<void(*)()>() ) {
      break;
    }

    // go again...
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;
  }

  auto buffLeft = TSIOBufferReaderAvail(_inReader);

  // if done with this buffer ....
  if ( buffReady && ! buffLeft ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
  }

  return 0;
}


class BlockStoreXform;

template APICont::APICont(BlockStoreXform &obj, void(BlockStoreXform::*funcp)(TSEvent,void*,decltype(nullptr)), decltype(nullptr));
template TSCont APICont::create_temp_tscont(TSMutex,std::shared_future<TSVConn> &, const std::shared_ptr<void> &);

//}
