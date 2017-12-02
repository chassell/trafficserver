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

void
forward_vio_event(TSEvent event, TSVIO invio)
{
  //  if ( invio && TSVIOContGet(invio) && TSVIOBufferGet(invio)) {
  if (invio && TSVIOContGet(invio)) {
    DEBUG_LOG("delivered: #%d", event);
    TSContCall(TSVIOContGet(invio), event, invio);
  } else {
    DEBUG_LOG("not delivered: #%d", event);
  }
}

APICacheKey::APICacheKey(const atscppapi::Url &url, std::string const &etag, uint64_t offset) : TSCacheKey_t(TSCacheKeyCreate())
{
  auto str = url.getUrlString();
  str.append(etag);                                              // etag for version handling
  str.append(reinterpret_cast<char *>(&offset), sizeof(offset)); // append *unique* position bytes
  TSCacheKeyDigestSet(get(), str.data(), str.size());
  auto host = url.getHost();
  TSCacheKeyHostNameSet(get(), host.data(), host.size());
}

template <typename T_DATA>
TSCont
APICont::create_temp_tscont(TSMutex shared_mutex, std::shared_future<T_DATA> &cbFuture, const std::shared_ptr<void> &counted)
{
  // alloc two objs to pass into lambda
  auto contp = std::make_unique<APICont>(shared_mutex); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<T_DATA>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref

  cbFuture = prom.get_future().share(); // link to promise

  // assign new handler
  cont = [&cont, &prom, counted](TSEvent evt, void *data) {
    decltype(contp) contp(&cont); // free after this call
    decltype(promp) promp(&prom); // free after this call

    prom.set_value(static_cast<T_DATA>(data)); // store correctly

    // bad ptr?  then call deleter on this!
    auto ptrErr = -reinterpret_cast<intptr_t>(data);
    if (ptrErr >= 0 && ptrErr < INK_START_ERRNO + 1000) {
      auto deleter = std::get_deleter<void (*)(void *)>(counted);
      if (deleter) {
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
APICont::APICont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, T_DATA), T_DATA cbdata)
  : TSCont_t(TSContCreate(&APICont::handleTSEventCB, TSMutexCreate()))
{
  // point back here
  TSContDataSet(get(), this);

  static_cast<void>(cbdata);
  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj, funcp, cbdata](TSEvent event, void *evtdata) { (obj.*funcp)(event, evtdata, cbdata); });
}

// bare Continuation lambda adaptor
APICont::APICont(TSMutex mutex) : TSCont_t(TSContCreate(&APICont::handleTSEventCB, mutex))
{
  // point back here
  TSContDataSet(get(), this);
}

int
APICont::handleTSEventCB(TSCont cont, TSEvent event, void *data)
{
  atscppapi::ScopedContinuationLock lock(cont);
  APICont *self = static_cast<APICont *>(TSContDataGet(cont));
  ink_assert(self->operator TSCont() == cont);
  self->_userCB(event, data);
  return 0;
}

// Transform continuations
APIXformCont::APIXformCont(atscppapi::Transaction &txn, TSHttpHookID xformType, int64_t len, int64_t pos)
  : TSCont_t(TSTransformCreate(&APIXformCont::handleXformTSEventCB, static_cast<TSHttpTxn>(txn.getAtsHandle()))),
    _txn(txn),
    _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
    _outBufferP(TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K)),
    _outReaderP(TSIOBufferReaderAlloc(this->_outBufferP.get()))
{
  init_body_range_handlers(pos, len); // hdrs -> skip-pre-range -> range -> skip-post-range

  // point back here
  TSContDataSet(get(), this);
  TSHttpTxnHookAdd(_atsTxn, xformType, get());
}

int
APIXformCont::handleXformTSEventCB(TSCont cont, TSEvent event, void *data)
{
  atscppapi::ScopedContinuationLock lock(cont);
  APIXformCont *self = static_cast<APIXformCont *>(TSContDataGet(cont));
  return self->handleXformTSEvent(cont, event, data);
}

BlockTeeXform::BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset)
  : APIXformCont(txn, TS_HTTP_RESPONSE_TRANSFORM_HOOK, xformLen, xformOffset),
    _writeHook(writeHook),
    _teeBufferP(TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K)),
    _teeReaderP(TSIOBufferReaderAlloc(this->_teeBufferP.get()))
{
  // get to method via callback
  set_body_handler([this](TSEvent evt, TSVIO vio, int64_t left) { return this->handleEvent(evt, vio, left); });
}

class BlockStoreXform;
class BlockReadXform;

template APICont::APICont(BlockStoreXform &obj, void (BlockStoreXform::*funcp)(TSEvent, void *, decltype(nullptr)),
                          decltype(nullptr));
template APICont::APICont(BlockReadXform &obj, void (BlockReadXform::*funcp)(TSEvent, void *, decltype(nullptr)),
                          decltype(nullptr));
template TSCont APICont::create_temp_tscont(TSMutex, std::shared_future<TSVConn> &, const std::shared_ptr<void> &);

//}
