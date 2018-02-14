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

const int8_t base64_values[80] = {
  /* 0-4 */ /*0x2b: +*/ 62, /*0x2c,2d,0x2e:*/ ~0, ~0, ~0, /*0x2f: / */ 63,
  /* 5-14 */  /*0x30-0x39: 0-9*/ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 
  /* 15-21 */   ~0, ~0, ~0, ~0, ~0, ~0, ~0,
  /* 22-47 */ /*0x41-0x5a: A-Z*/ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
  /* 48-53 */   ~0, ~0, ~0, ~0, ~0, ~0,
  /* 54-79 */ /*0x61-0x6a: a-z*/ 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
}; 

const char base64_chars[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

void
forward_vio_event(TSEvent event, TSVIO tgt, TSCont mutexCont)
{
  auto tgtcont = TSVIOContGet(tgt);
//  TSMutexUnlock(mutex);

  //  if ( tgt && TSVIOContGet(tgt) && TSVIOBufferGet(tgt)) {
  if (tgt && tgtcont) {
    DEBUG_LOG("delivered: e#%d", event);
    TSContCall(tgtcont, event, tgt);
  } else {
    DEBUG_LOG("not delivered: e#%d", event);
  }

//  TSMutexLock(mutex);
}

ATSCacheKey::ATSCacheKey(const std::string &url, std::string const &etag, uint64_t offset) : TSCacheKey_t(TSCacheKeyCreate())
{
  auto str = url;
  auto origLen = str.size();
  str.push_back('\0');
  for ( ; offset ; offset >>= 6 ) {
    str.push_back(base64_chars[offset & 0x3f]); // append *unique* position bytes (until empty)
  }
  str.push_back('\0');
  str.append(etag);                                              // etag for version handling
  auto key = get();
  // match must be same etag and block pos
  TSCacheKeyDigestSet(key, str.data(), str.size());
  // disk randomized by etag and pos
  TSCacheKeyHostNameSet(key, str.data()+origLen, str.size()-origLen);
}

template <class T_FUTURE>
TSCont
ATSCont::create_temp_tscont(TSCont mutexSrc, T_FUTURE &cbFuture, const std::shared_ptr<void> &counted)
{
  using FutureData_t = decltype(cbFuture.get());

  // alloc two objs to pass into lambda
  auto contp = std::make_unique<ATSCont>(mutexSrc); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<FutureData_t>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref

  cbFuture = prom.get_future().share(); // link to promise

  // assign new handler
  cont = [&cont, &prom, counted](TSEvent evt, void *data) {
    decltype(contp) contp(&cont); // free after this call
    decltype(promp) promp(&prom); // free after this call

    prom.set_value(static_cast<FutureData_t>(data)); // store correctly

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
// bare Continuation lambda adaptor
ATSCont::ATSCont(TSCont mutexSrc) : TSCont_t(TSContCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
  // point back here
  TSContDataSet(get(), this);
}

ATSCont::~ATSCont() 
{
  auto cont = get();
  if ( ! cont ) {
    return; // cont was deleted or none added
  }
  
  if ( TSContMutexGet(cont) ) {
    atscppapi::ScopedContinuationLock lock(cont);
    DEBUG_LOG("final destruct cont=%p",cont);
    TSContDataSet(cont, nullptr); // prevent new events
  } else {
    DEBUG_LOG("final destruct cont=%p nomutex",cont);
    TSContDataSet(cont, nullptr); // prevent new events
  }
}

int
ATSCont::handleTSEventCB(TSCont cont, TSEvent event, void *data)
{
  atscppapi::ScopedContinuationLock lock(cont);
  ATSCont *self = static_cast<ATSCont *>(TSContDataGet(cont));
  if ( ! self || self->get() != cont ) {
    DEBUG_LOG("late event e#%d %p",event,data);
    return 0;
  } 
  self->_userCB(event, data);
  return 0;
}

ATSXformOutVConn::Uniq_t
ATSXformOutVConn::create_if_ready(const ATSXformCont &xform, int64_t bytes, int64_t offset)
{
  return TSTransformOutputVConnGet(xform) 
     ?  std::make_unique<ATSXformOutVConn>(xform, bytes, offset)
     :  ATSXformOutVConn::Uniq_t{};
}

ATSXformOutVConn::ATSXformOutVConn(const ATSXformCont &xform, int64_t bytes, int64_t offset)
  : _inVConn(xform),
    _inVIO(xform.inputVIO()),
    _outVConn( TSTransformOutputVConnGet(xform) ),
    _outBuffer(xform.outputBuffer()),
    _outReader(xform.outputReader()),
    _skipBytes(offset),
    _writeBytes(bytes)
{
  if ( ! bytes && _inVIO ) {
    const_cast<int64_t&>(_writeBytes) = TSVIONBytesGet(_inVIO) - offset;
  }
}

ATSVConnFuture::~ATSVConnFuture()
{
  auto vconn = get();
  if ( ! vconn ) {
    return; // happens often
  }

  if ( TSContMutexGet(vconn) ) {
    atscppapi::ScopedContinuationLock lock(vconn);
    DEBUG_LOG("final close cont=%p",vconn);
    TSVConnClose(vconn);
  } else {
    DEBUG_LOG("final close cont=%p nomutex",vconn);
    TSVConnClose(vconn);
  }
}

bool
ATSVConnFuture::is_close_able() const
{
  using namespace std::chrono;
  using std::future_status;

  return ( ! std::shared_future<TSVConn>::valid() || wait_for(seconds::zero()) == future_status::ready );
}

int
ATSVConnFuture::error() const
{
  using namespace std::chrono;
  using std::future_status;

  if ( ! std::shared_future<TSVConn>::valid() ) {
    return SOCK_ERRNO;
  }
  if ( wait_for(seconds::zero()) != future_status::ready ) {
    return ESOCK_TIMEOUT;
  }
  auto ptrErr = reinterpret_cast<intptr_t>(std::shared_future<TSVConn>::get());
  if ( ptrErr >= -INK_START_ERRNO - 1000 && ptrErr <= 0 ) {
    return -ptrErr;
  }
  return 0;
}

void
ATSXformOutVConn::set_close_able()
{
  if ( _outVIO ) {
    TSVIONBytesSet(_outVIO, TSVIONDoneGet(_outVIO)); // define it as complete
  }
}

bool
ATSXformOutVConn::is_close_able() const
{
  if ( ! _outVIO || ! TSVIONTodoGet(_outVIO) ) {
    return true;
  }

  return false; // not ready to delete now...
}

ATSXformOutVConn::~ATSXformOutVConn()
{
  if ( ! _inVConn ) {
    DEBUG_LOG("late destruct");
    return;
  }

  atscppapi::ScopedContinuationLock lock(_inVConn);
  if ( _outVIO ) {
    TSVIONBytesSet( _outVIO, TSVIONDoneGet(_outVIO) );
    DEBUG_LOG("write-complete @%#lx invconn=%p outvconn=%p",TSVIONDoneGet(_outVIO), _inVConn, _outVConn);
    TSVIOReenable(_outVIO);
    _outVIO = nullptr;
  }

  if ( _outVConn ) {
    TSVConnShutdown(_outVConn, 0, 1); // do only once!
  }

  const_cast<TSVConn&>(_outVConn) = nullptr;
  const_cast<TSVConn&>(_inVConn) = nullptr;

  DEBUG_LOG("shutdown-complete");
}

// Transform continuations
ATSXformCont::ATSXformCont(atscppapi::Transaction &txn, TSHttpHookID xformType, int64_t bytes, int64_t offset)
  : TSCont_t(TSTransformCreate(&ATSXformCont::handleXformTSEventCB, static_cast<TSHttpTxn>(txn.getAtsHandle()))),
    _txnID(TSHttpTxnIdGet(static_cast<TSHttpTxn>(txn.getAtsHandle()))),
    _xformCB( [](TSEvent evt, TSVIO vio, int64_t left) { DEBUG_LOG("xform-event empty body handler"); return 0; }),
    _outSkipBytes(offset),
    _outWriteBytes(bytes),
    _outBufferU(TSIOBufferCreate()),
    _outReaderU(TSIOBufferReaderAlloc(this->_outBufferU.get()))
{
  ink_assert( bytes + offset >= 0LL );

  // point back here
  auto xformCont = get();
  TSContDataSet(xformCont, this);

  long maxAgg = 0;
  if ( TSMgmtIntGet("proxy.config.cache.agg_write_backlog",&maxAgg) != TS_SUCCESS ) {
    maxAgg = 20 * (1<<20); // 20M watermark
  }

  // NOTE: maybe called long past TXN_CLOSE!
  TSHttpTxnHookAdd(static_cast<TSHttpTxn>(txn.getAtsHandle()), xformType, xformCont);
  // get to method via callback
  TSIOBufferWaterMarkSet(_outBufferU.get(), maxAgg); // never produce a READ_READY
  DEBUG_LOG("output block level set to: %ldK",maxAgg);
}

int
ATSXformCont::handleXformTSEventCB(TSCont cont, TSEvent event, void *data)
{
  atscppapi::ScopedContinuationLock lock(cont);
  ATSXformCont *self = static_cast<ATSXformCont *>(TSContDataGet(cont));
  if ( !self || self->get() != cont ) {
    DEBUG_LOG("late event e#%d %p",event,data);
    return 0;
  }
  return self->handleXformTSEvent(cont, event, data);
}

ATSXformCont::~ATSXformCont()
{
  auto cont = get();
  if ( ! cont ) {
    DEBUG_LOG("late destruct");
    return;
  }

  atscppapi::ScopedContinuationLock lock(cont);
  TSContDataSet(cont, nullptr); // prevent new events

  DEBUG_LOG("final destruct %p",cont);

  _xformCB = XformCB_t{}; // no callbacks
  TSCont_t::reset();
}

BlockTeeXform::BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset)
  : ATSXformCont(txn, TS_HTTP_RESPONSE_TRANSFORM_HOOK, xformLen, xformOffset),
    _writeHook(writeHook),
    _teeBufferP(TSIOBufferCreate()),
    _teeReaderP(TSIOBufferReaderAlloc(this->_teeBufferP.get()))
{
  ink_assert( xformLen + xformOffset >= 0LL );

  // get to method via callback
  set_body_handler([this](TSEvent evt, TSVIO vio, int64_t left) { return this->inputEvent(evt, vio, left); });
  TSIOBufferWaterMarkSet(_teeBufferP.get(), TSIOBufferWaterMarkGet(outputBuffer()) ); // never produce a READ_READY
  DEBUG_LOG("tee-buffer block level set to: %ld", xformLen + xformOffset);
}

void
BlockTeeXform::teeReenable()
{
  auto range = teeAvail();
  _writeHook(_teeReaderP.get(), range.first, range.second); // attempt new absorb of input
}

class BlockStoreXform;
class BlockReadXform;

// template ATSCont::ATSCont(BlockStoreXform &obj, void (BlockStoreXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);
// template ATSCont::ATSCont(BlockReadXform &obj, void (BlockReadXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);

template TSCont ATSCont::create_temp_tscont(TSCont, ATSVConnFuture &, const std::shared_ptr<void> &);

//}
