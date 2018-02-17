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

template <> void std::default_delete<TSVIO_t::element_type>::operator()(TSVIO vio) const
{
  TSIOBuffer_t buff{ TSVIOBufferGet(vio) };
  if ( ! buff ) {
    return;
  }
  TSIOBufferReader_t rdr{ TSVIOReaderGet(vio) };

  if ( TSVIONTodoGet(vio) ) 
  {
    TSVIONBytesSet(vio, TSVIONDoneGet(vio));
    TSContCall(TSVIOContGet(vio), TS_EVENT_VCONN_WRITE_COMPLETE, vio); // notify it's done...
  }

  auto vconn = TSVIOVConnGet(vio);

  if ( vconn ) {
    TSVConnClose(vconn);
  }

  auto bytes = TSIOBufferReaderAvail(TSIOBufferReader_t{TSIOBufferReaderAlloc(buff.get())}.get());

  // no reader ... means no responsibility for it..
  if ( ! rdr ) {
    DEBUG_LOG("vc %p: read ended with %ld bytes", vconn, bytes);
    buff.release(); // don't delete it..
    return;
  }

  // only close if drained ...
  if ( bytes ) {
    DEBUG_LOG("vc %p: write ended with %ld bytes", vconn, bytes);
    buff.release(); // leave alone .. with bytes...
    return;
  } 

  DEBUG_LOG("vc %p: write ended", vconn);
}

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
  TSCacheKeyHostNameSet(key, str.data()+origLen+1, str.size()-origLen-1);
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
  cont = [&cont, &prom, counted](TSEvent evt, void *data) 
  {
    decltype(contp) contp(&cont); // free after this call
    decltype(promp) promp(&prom); // free after this call

    prom.set_value(static_cast<FutureData_t>(data)); // store correctly

    auto errVal = reinterpret_cast<intptr_t>(data) >> 16;
    if ( errVal != -1 ) {
      return;
    }

    auto deleter = std::get_deleter<void (*)(void *)>(counted);
    if (deleter) {
      (*deleter)(counted.get());
    }
  };

  contp.release(); // owned as ptr in lambda
  promp.release(); // owned as ptr in lambda

  return cont; // convert to TSCont
}

template <>
TSCont
ATSCont::create_temp_tscont(TSCont mutexSrc, std::shared_future<TSVIO> &cbFuture, const std::shared_ptr<void> &counted)
{
  // alloc two objs to pass into lambda
  auto contp = std::make_unique<ATSCont>(mutexSrc); // uses empty stub-callback!!
  auto promp = std::make_unique<std::promise<TSVIO>>();

  auto &cont = *contp; // hold scoped-ref
  auto &prom = *promp; // hold scoped-ref

  cbFuture = prom.get_future().share(); // link to promise

  // assign new handler
  cont = [&cont, &prom, counted](TSEvent evt, void *data) 
  {
    auto errVal = reinterpret_cast<intptr_t>(data) >> 16;
    TSVIO vio = static_cast<TSVIO>(data);
    TSVConn vconn = static_cast<TSVConn>(data);

    switch (evt) {
      case TS_EVENT_CACHE_OPEN_READ:
        TSVConnRead(vconn, cont, TSIOBufferCreate(), TSVConnCacheObjectSizeGet(vconn));
        return; // try again...
      case TS_EVENT_VCONN_READ_READY:
        return; // try again...

      case TS_EVENT_VCONN_READ_COMPLETE:
        break; // set vio from data...

      default:
        if ( ~errVal && errVal ) {
          // no error result?
          errVal = -1; // call deleter
          vio = reinterpret_cast<TSVIO>(-evt); // store it...
        }
        break;
    }

    // last callback now...

    decltype(contp) contp(&cont);
    decltype(promp) promp(&prom);

    prom.set_value(vio); // done (or error)

    // call deleter if not a normal pointer...
    if ( ! ~errVal || ! errVal ) {
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


TSCont
ATSCont::create_temp_tscont(TSCont mutexSrc, const std::shared_ptr<void> &counted)
{
  auto contp = std::make_unique<ATSCont>(mutexSrc); // uses empty stub-callback!!
  auto &cont = *contp; // hold scoped-ref

  // assign new handler
  cont = [&cont, counted](TSEvent evt, void *data) {
    decltype(contp) contp(&cont); // free after this call
    intptr_t ptrErr = reinterpret_cast<intptr_t>(data);
    if (ptrErr >= 0 && ptrErr < INK_START_ERRNO + 1000) {
      auto deleter = std::get_deleter<void (*)(void *)>(counted);
      if (deleter) {
        (*deleter)(counted.get());
      }
    }
  };

  contp.release(); // owned as ptr in lambda
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

ATSXformOutVConn::ATSXformOutVConn(const ATSXformCont &xform, int64_t bytes, int64_t offset)
  : _cont(xform),
    _skipBytes(offset),
    _writeBytes(bytes),
    _outVIO(nullptr),
    _inVIO(xform.inputVIO())
{
  if ( ! bytes && offset && _inVIO ) {
    bytes = TSVIONBytesGet(_inVIO) - offset;
  }
  _outVIOPtr.reset( TSVConnWrite( TSTransformOutputVConnGet(xform), _cont,
                            TSIOBufferReaderAlloc(xform.outputBuffer()),
                            bytes) );
  const_cast<TSVIO&>(_outVIO) = _outVIOPtr.get();
}

template <>
ATSFuture<TSVConn>::~ATSFuture()
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

template <typename T_DATA> 
bool ATSFuture<T_DATA>::is_close_able() const
{
  using namespace std::chrono;
  using std::future_status;

  return ( ! std::shared_future<TSVConn>::valid() || this->wait_for(seconds::zero()) == future_status::ready );
}

template <typename T_DATA> 
int ATSFuture<T_DATA>::error() const
{
  using namespace std::chrono;
  using std::future_status;

  if ( ! std::shared_future<TSVConn>::valid() ) {
    return EINVAL;
  }
  if ( this->wait_for(seconds::zero()) != future_status::ready ) {
    return EAGAIN;
  }
  auto ptrErr = reinterpret_cast<intptr_t>(std::shared_future<TSVConn>::get());
  auto errChk = ptrErr >> 16;
  if ( ! errChk || ! ~errChk ) {
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
  if ( ! _cont ) {
    DEBUG_LOG("late destruct");
    return;
  }

  atscppapi::ScopedContinuationLock lock(_cont);
  if ( _outVIO ) {
    DEBUG_LOG("write-complete @%#lx [@%#lx] invconn=%p outvconn=%p",
             TSVIONDoneGet(_outVIO), TSVIONBytesGet(_outVIO), 
             _cont, _cont);
    TSVIONBytesSet( _outVIO, TSVIONDoneGet(_outVIO) );
    TSVIOReenable(_outVIO);
    const_cast<TSVIO&>(_outVIO) = nullptr;
    _outVIOPtr.reset(); // close, end, free VIO ..
  }

  const_cast<TSVConn&>(_cont) = nullptr;

  DEBUG_LOG("shutdown-complete");
}

// Transform continuations
ATSXformCont::ATSXformCont(atscppapi::Transaction &txn, int64_t bytes, int64_t offset)
  : TSCont_t(TSTransformCreate(&ATSXformCont::handleXformTSEventCB, static_cast<TSHttpTxn>(txn.getAtsHandle()))),
    _xformCB( [](TSEvent evt, TSVIO vio, int64_t left) { DEBUG_LOG("xform-event empty body handler"); return 0; }),
    _outSkipBytes(offset),
    _outWriteBytes(bytes),
    _transformHook(TS_HTTP_LAST_HOOK),
    _outBufferU(TSIOBufferCreate())
{
  ink_assert( bytes + offset >= 0LL );

  // point back here
  auto xformCont = get();
  TSContDataSet(xformCont, this);

  if ( ! offset && ! (bytes % ( 1 << 20 )) ) {
    _transformHook = TS_HTTP_RESPONSE_CLIENT_HOOK; // no active output pumping needed...
  } else {
    _transformHook = TS_HTTP_RESPONSE_TRANSFORM_HOOK;
  }

  if ( txn.getCacheStatus() == atscppapi::Transaction::CACHE_LOOKUP_HIT_FRESH ) {
    // must add real transform now now...
    TSHttpTxnHookAdd(static_cast<TSHttpTxn>(txn.getAtsHandle()), _transformHook, xformCont);
    _transformHook = TS_HTTP_LAST_HOOK;
  } else {
    // allow it to be failed later ...
    TSHttpTxnHookAdd(static_cast<TSHttpTxn>(txn.getAtsHandle()), TS_HTTP_READ_RESPONSE_HDR_HOOK, xformCont);
  }

  // get to method via callback
  DEBUG_LOG("output buffering begins with: %ldK",1L<<6);
  TSIOBufferWaterMarkSet(_outBufferU.get(), 1<<16); // start to flush early 
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

  _transformHook = TS_HTTP_LAST_HOOK; // start no transform on late events...

  _xformCB = XformCB_t{}; // no callbacks
  TSCont_t::reset();
}

void 
ATSXformCont::reset_input_vio(TSVIO vio)
{
  // XXX may check for xfinput --> non-xfinput 
  //    to make hook delayed or not..?
  _inVIO = vio;
  if ( _outVConnU ) {
    _outVConnU->_inVIO = vio;
  }
}

// Xform "client" with skip/truncate
BlockTeeXform::BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset)
  : ATSXformCont(txn, xformLen, xformOffset),
    _writeHook(writeHook),
    _teeBufferP(TSIOBufferCreate()),
    _teeReaderP(TSIOBufferReaderAlloc(this->_teeBufferP.get()))
{
  ink_assert( xformLen + xformOffset >= 0LL );

  long maxAgg = 5 * (1<<20);
  TSMgmtIntGet("proxy.config.cache.agg_write_backlog",&maxAgg);

  // get to method via callback
  set_body_handler([this](TSEvent evt, TSVIO vio, int64_t left) { return this->inputEvent(evt, vio, left); });
  DEBUG_LOG("tee buffering set to: %ldK", maxAgg>>10);
  TSIOBufferWaterMarkSet(_teeBufferP.get(), maxAgg); // avoid producing a READ_READY
}

void
BlockTeeXform::teeReenable()
{
  atscppapi::ScopedContinuationLock lock(*this);

  auto range = teeAvail();
  auto teemax = TSIOBufferWaterMarkGet(_teeBufferP.get()); // without bytes copied

  DEBUG_LOG("performing reenable: [%#lx-%#lx)",range.first,range.second);

  _writeHook(_teeReaderP.get(), range.first, range.second, 0); // attempt new absorb of input
  auto nrange = teeAvail(); // check new..

  // still too many?
  if ( nrange.second >= nrange.first + teemax ) {
    DEBUG_LOG("too full for new input: [%#lx-%#lx)",nrange.first,nrange.second);
    return; // need another reenable
  }

  auto inrange = inputAvail();
  if ( inrange.first >= inrange.second ) { // bytes can be absorbed?
    DEBUG_LOG("waiting on empty xform-input");
    return; // need another reenable
  }

  DEBUG_LOG("re-submitting input: %ld", inrange.second - inrange.first );
  TSContSchedule(*this, 0, TS_THREAD_POOL_DEFAULT); // attempt re-use of input buffer
}

static void sub_server_hdrs(atscppapi::Transaction &origTxn, TSIOBuffer reqBuffer, const std::string &rangeStr)
{
  auto &purl = origTxn.getClientRequest().getPristineUrl();
  auto txnh = static_cast<TSHttpTxn>(origTxn.getAtsHandle());
  TSMLoc urlLoc, hdrLoc, loc;
  TSMBuffer buf;
  TSMBuffer_t nbuf{ TSMBufferCreate() };

  // make a clone [nothing uses mloc field handles]
  TSHttpTxnClientReqGet(txnh, &buf, &loc);
  TSHttpHdrClone(nbuf.get(), buf, loc, &hdrLoc);
  TSHttpHdrUrlGet(nbuf.get(), hdrLoc, &urlLoc);

  // replace the one header
  atscppapi::Headers hdrs(nbuf.get(), hdrLoc);
  hdrs.set(RANGE_TAG, rangeStr);

  atscppapi::Url url(nbuf.get(), urlLoc);

  url.setPath(purl.getPath());
  url.setQuery(purl.getQuery());
  url.setScheme(purl.getScheme());
  url.setHost(purl.getHost());
  url.setPort(purl.getPort());

  DEBUG_LOG("internal request\n------------------------\nGET %s HTTP/1.1\n%s\n---------------------------", url.getUrlString().c_str(), hdrs.wireStr().c_str());

  // print corrected request to output buffer
  TSHttpHdrPrint(nbuf.get(), hdrLoc, reqBuffer);
  TSIOBufferWrite(reqBuffer, "\r\n", 2); 
}

TSVConn 
spawn_sub_range(atscppapi::Transaction &origTxn, int64_t begin, int64_t end)
{
  struct WriteHdr {
     TSIOBuffer_t       _buf{ TSIOBufferCreate() };
     TSIOBufferReader_t _rdr{ TSIOBufferReaderAlloc(_buf.get()) };
  };

  auto data = std::make_shared<WriteHdr>();
  auto reqbuf = data->_buf.get();
  auto reqrdr = data->_rdr.get();

  auto newRange = std::string("bytes=") + std::to_string(begin);

  // need range endpoint?
  if ( begin >= 0 ) {
    newRange += ( end > 0 ? std::to_string(-end).c_str() : "-" );
  }

  sub_server_hdrs(origTxn, reqbuf, newRange);

  auto vconn = TSHttpConnectWithPluginId(origTxn.getClientAddress(), PLUGIN_NAME, 0); 
  auto wrlen = TSIOBufferReaderAvail(reqrdr);

  // destruct when an event (any) returns
  TSVConnWrite(vconn, ATSCont::create_temp_tscont(nullptr, std::move(data)), reqrdr, wrlen);
  return vconn;
}

void 
spawn_range_request(atscppapi::Transaction &origTxn, int64_t begin, int64_t end, int64_t rdlen)
{
  // ignore TSVConn and assume it is cleared on its own...

//  struct ReadHdr {
//     TSIOBuffer_t       _buf{ TSIOBufferCreate() };
//     ATSVConnFuture     _vc;
//  };

//  auto data = std::make_shared<ReadHdr>();
//  auto rspbuf = data->_buf.get();

  // add 64K to the watermark... for response header itself
//  TSIOBufferWaterMarkSet(rspbuf, rdlen + (1<<16)); 

//  TSVConn vconn = spawn_sub_range(origTxn, begin, end);
//  data->_vc = ATSVConnFuture(vconn);
//  TSVConnRead(vconn, ATSCont::create_temp_tscont(nullptr, std::move(data)), rspbuf, rdlen + (1<<16));
}

class BlockStoreXform;
class BlockReadXform;

// template ATSCont::ATSCont(BlockStoreXform &obj, void (BlockStoreXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);
// template ATSCont::ATSCont(BlockReadXform &obj, void (BlockReadXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);

template TSCont ATSCont::create_temp_tscont(TSCont, ATSVConnFuture &, const std::shared_ptr<void> &);
template TSCont ATSCont::create_temp_tscont(TSCont, ATSVIOFuture &, const std::shared_ptr<void> &);

//}
