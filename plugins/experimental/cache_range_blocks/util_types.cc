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

#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%d] [%s:%d] %s(): " fmt,  ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%d] [%s:%d] %s(): " fmt, ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

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

  //  if ( tgt && TSVIOContGet(tgt) && TSVIOBufferGet(tgt))
  if (tgt && tgtcont) {
    DEBUG_LOG("delivered: e#%d", event);
    TSContCall(tgtcont, event, tgt);
  } else {
    DEBUG_LOG("not delivered: e#%d", event);
  }

//  TSMutexLock(mutex);
}

template <> void std::default_delete<TSVIO_t::element_type>::operator()(TSVIO vio) const
{
  auto errVal = reinterpret_cast<intptr_t>(vio) >> 16;
  if ( ! ~errVal || ! errVal ) {
    return; // vio holds an error number
  }

  TSIOBufferReader_t rdr{ TSVIOReaderGet(vio) };
  TSIOBuffer_t buff{TSVIOBufferGet(vio)};

  if ( ! buff ) {
    return; // vio has been shutdowned!
  }

  if ( ! rdr ) {
    buff.release(); // dst-buffer is never freed by reader
  }

  auto vconn = TSVIOVConnGet(vio);
  atscppapi::ScopedContinuationLock lock(vconn); ///// locked

  if ( TSVIONTodoGet(vio) )
  {
    auto cont = TSVIOContGet(vio);
    auto evt = rdr ? TS_EVENT_VCONN_WRITE_COMPLETE : TS_EVENT_VCONN_READ_COMPLETE;
    TSVIONBytesSet(vio, TSVIONDoneGet(vio)); // cut it short now...
    TSContCall(cont, evt, vio); // notified...
  }

  auto rvio = TSVConnReadVIOGet(vconn);
  auto wvio = TSVConnWriteVIOGet(vconn);

  // INKVConn closed is safe to check only if rvio or wvio are valid...

  if ( wvio == vio && TSVConnClosedGet(vconn) ) {
    DEBUG_LOG("vc %p: xform write freed (pre-closed)", vconn);
    return;
  }

  if ( rvio == vio && TSVConnClosedGet(vconn) ) {
    DEBUG_LOG("vc %p: xform read tested (pre-closed)", vconn);
    return;
  }

  if ( wvio == vio ) {
    DEBUG_LOG("vc %p: xform closed (write closing)", vconn);
    TSVConnShutdown(vconn, 0, 1); // no more events please
  } else if ( rvio == vio ) {
    DEBUG_LOG("vc %p: xform closed (read closing)", vconn);
    TSVConnShutdown(vconn, 1, 0); // no more events please
  } else if ( rdr ) {
    DEBUG_LOG("vc %p: vconn write closed", vconn);
  } else {
    DEBUG_LOG("vc %p: vconn read closed", vconn);
  }

  TSVConnClose(vconn);
}

ATSCacheKey::ATSCacheKey(const std::string &url, std::string const &etag, uint64_t offset) : TSCacheKey_t(TSCacheKeyCreate())
{
  auto str = url;
  str.push_back('\0');
  auto offsetPos = str.size();
  for ( ; offset ; offset >>= 6 ) {
    str.push_back(base64_chars[offset & 0x3f]); // append *unique* position bytes (until empty)
  }
  str.push_back('\0');
  auto etagPos = str.size();
  str.append(etag);                                              // etag for version handling
  auto key = get();
  // match must be same etag and block pos
  TSCacheKeyDigestSet(key, str.data(), str.size());
  
  CryptoHash *ch = reinterpret_cast<CryptoHash*>(key);
  DEBUG_LOG("key %p : [%s][%s] -> %#016lx:%016lx", key, &str[offsetPos], &str[etagPos], ch->u64[0], ch->u64[1]);
  // disk randomized by etag and pos
  TSCacheKeyHostNameSet(key, str.data()+offsetPos, str.size()-offsetPos);
}

template <>
ATSFuture<TSVConn>::~ATSFuture()
{
  auto vconn = get();
  if ( ! vconn ) {
    return; // happens often
  }

  atscppapi::ScopedContinuationLock lock(vconn);
  DEBUG_LOG("final close cont=%p",vconn);
  TSVConnClose(vconn);
}

template <>
ATSFuture<TSVIO>::~ATSFuture()
{
  auto vio = get();
  if ( ! vio ) {
    DEBUG_LOG("final close err:%d",error());
    return; // happens often
  }

  DEBUG_LOG("final close vio=%p",vio);
  std::default_delete<TSVIO_t::element_type>()(vio); // completed
}


/*
TSEvent
ATSCont::handle_event(ATSCont &cont, std::promise<TSVIO> &prom, TSIOBufferReader rdr, TSEvent evt, void *data)
{
  auto vio = static_cast<TSVIO>(data);

  auto errVal = reinterpret_cast<intptr_t>(data) >> 16;
  if ( ! errVal || ! ~errVal ) {
    vio = nullptr;
  }

  // no crazy event params ...
  if ( ! vio ) {
    prom.set_value( static_cast<TSVIO>(data) ? : reinterpret_cast<TSVIO>(-EINVAL) );
    return TS_EVENT_ERROR;
  }

  switch (evt) 
  {
    case TS_EVENT_IMMEDIATE:
      DEBUG_LOG("vc: %p temp-vio immed event ignored",TSVIOVConnGet(vio));
      break;

    case TS_EVENT_NET_ACCEPT:
    {
      auto vconn = static_cast<TSVConn>(data);
      DEBUG_LOG("vc: %p temp-vio stream-read started",TSVIOVConnGet(vio));
      TSVConnRead(vconn, cont, TSIOBufferCreate(), INT64_MAX);
      prom.set_value(  );
      break;
    }

    case TS_EVENT_CACHE_OPEN_READ:
    {
      auto vconn = static_cast<TSVConn>(data);
      auto rdlen = TSVConnCacheObjectSizeGet(vconn);
      auto buff = TSIOBufferCreate();
      auto rdr = TSIOBufferReaderAlloc(buff);
      DEBUG_LOG("vc: %p temp-vio read started: %ld bytes",TSVIOVConnGet(vio),rdlen);
      TSVConnRead(vconn, cont, buff, rdlen);
      ATSVIOFuture temp;
      ATSCont::create_temp_tsvconn(cont, 

      prom.set_value( TSVConnRead(TSContMutexGet(cont)), cont, TSIOBufferCreate(), rdlen) );
      break;
    }

    case TS_EVENT_NET_CONNECT:
    case TS_EVENT_CACHE_OPEN_WRITE:
    {
      auto vconn = static_cast<TSVConn>(data);
      auto wrlen = TSIOBufferReaderAvail(rdr);
      // don't store vio yet...
      DEBUG_LOG("vc: %p temp-vio write started: %ld bytes",TSVIOVConnGet(vio),wrlen);
      prom.set_value( TSVConnWrite(vconn, cont, rdr, wrlen) );
      break;
    }

    case TS_EVENT_VCONN_READ_READY:
    case TS_EVENT_VCONN_WRITE_READY:
      DEBUG_LOG("vc: %p temp-vio reenable attempted",TSVIOVConnGet(vio));
      TSVIOReenable(vio); // infinite loop?
      break;

    case TS_EVENT_VCONN_READ_COMPLETE:
      DEBUG_LOG("vc: %p temp-vio complete",TSVIOVConnGet(vio));
      return TS_EVENT_NONE; // end continuation events

    case TS_EVENT_VCONN_WRITE_COMPLETE:
      DEBUG_LOG("vc: %p temp-vio complete",TSVIOVConnGet(vio));
      return TS_EVENT_NONE; // end continuation events

    default:
      DEBUG_LOG("vc: %p temp-vio failure: e#%d",TSVIOVConnGet(vio),evt);
      return TS_EVENT_NONE; // end continuation events
  }

  return TS_EVENT_CONTINUE; // retain continuation
}

TSCont
ATSCont::create_temp_tscont(ATSVIOFuture &vioFuture, TSIOBufferReader rdr, const std::shared_ptr<void> &counted)
{
  // hold ptrs..
  auto contp = std::make_unique<ATSCont>(vconn); // use mutex if possible
  auto promp = std::make_unique<std::promise<TSVIO>>();

  auto &cont = *contp; // capture the pointer only...
  auto &prom = *promp; // hold scoped-ref

  vioFuture = prom.get_future().share(); // start an incomplete value first...

  // assign new handler
  cont = [&cont,&prom,rdr,counted](TSEvent evt, void *data)
  {
    auto r = handle_event(cont,prom,rdr,evt,data);
    if ( r == TS_EVENT_CONTINUE ) {
      return;
    }

    if ( r == TS_EVENT_ERROR ) {
      auto deleter = std::get_deleter<void (*)(void *)>(counted);
      if (deleter) {
        (*deleter)(counted.get());
      }
    }

    std::default_delete<ATSCont>()(&cont); // completed (lambda delete)
  };

  contp.release(); // owned by lambda
  promp.release(); // owned by lambda

  if ( vconn && rdr ) {
    TSContCall(cont.get(), TS_EVENT_NET_CONNECT, vconn); // start write
  } else if ( vconn ) {
    TSContCall(cont.get(), TS_EVENT_NET_ACCEPT, vconn); // start read
  }

  return ! vioFuture.completed() ? cont.get() : nullptr; // detect if done already!
}


TSCont
ATSCont::create_temp_write(TSIOBufferReader rdr, TSVConn vconn, ATSVIOFuture &vioFuture, const std::shared_ptr<void> &counted)
{
  // hold ptrs..
  auto contp = std::make_unique<ATSCont>(vconn); // use mutex if possible
  auto promp = std::make_unique<std::promise<TSVIO>>();

  auto &cont = *contp; // capture the pointer only...
  auto &prom = *promp; // hold scoped-ref

  vioFuture = prom.get_future().share(); // start an incomplete value first...

  // assign new handler
  cont = [&cont,&prom,rdr,counted](TSEvent evt, void *data)
  {
    auto r = handle_event(cont,prom,rdr,evt,data);
    if ( r == TS_EVENT_CONTINUE ) {
      return;
    }

    if ( r == TS_EVENT_ERROR ) {
      auto deleter = std::get_deleter<void (*)(void *)>(counted);
      if (deleter) {
        (*deleter)(counted.get());
      }
    }

    std::default_delete<ATSCont>()(&cont); // completed (lambda delete)
  };

  contp.release(); // owned by lambda
  promp.release(); // owned by lambda

  if ( vconn && rdr ) {
    TSContCall(cont.get(), TS_EVENT_NET_CONNECT, vconn); // start write
  } else if ( vconn ) {
    TSContCall(cont.get(), TS_EVENT_NET_ACCEPT, vconn); // start read
  }

  return ! vioFuture.completed() ? cont.get() : nullptr; // detect if done already!
}
*/

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

void ATSVIO::close_vconn() 
{
  if ( ready() ) {
    atscppapi::ScopedContinuationLock lock{cont()};
    TSVConnClose(vconn()); // close finally...
  }
}

void ATSVIO::free_owned() 
{
  atscppapi::ScopedContinuationLock lock(cont());

  if ( ! is_ivc_writer() ) {
    TSIOBufferReader_t{operator TSIOBufferReader()}; // (if any)
    TSIOBuffer_t{operator TSIOBuffer()}; // writers have other ways...
  }
}

void ATSVIO::complete_vio() 
{
  if ( ! _vio || ! ntodo() ) {
    return;
  }

  auto evt = TS_EVENT_VCONN_READ_COMPLETE;
  atscppapi::ScopedContinuationLock lock(cont());
  auto rdr = operator TSIOBufferReader();
  if ( rdr ) {
    TSIOBufferReaderConsume(rdr, TSIOBufferReaderAvail(rdr)); // flush bytes...
    evt = TS_EVENT_VCONN_WRITE_COMPLETE;
  }

  TSVIONBytesSet(_vio, ndone()); // cut it short now...
  TSContCall(cont(), evt, _vio); // notified...
}

int64_t
ATSVIO::ivc_copy(int64_t inskip)
{
  auto buf = ivc_buffer();
  auto rdr = ivc_input();
  if ( ! buf || ! rdr ) {
    return -1;
  }
  // bytes copied only (0 if not enough present)
  return rdr.ntodo() > inskip ? TSIOBufferCopy(buf, rdr, rdr.ntodo()-inskip, inskip) : 0;
}

int64_t
ATSVIO::ivc_transfer(int64_t inskip)
{
  auto n = ivc_copy(inskip); // copied any?
  if ( n > 0 ) {
    TSIOBufferReaderConsume(ivc_input(),n + inskip);
    ivc_input().drain(n + inskip);
    ivc_buffer().fill(n);
  }
  return n;
}

/// deciphering all situations that occur with IMMEDIATE/wakeup-only 
int64_t ATSVConnAPI::bytes_ready()
{
  auto invio = input();
  if ( ! invio ) {
    return -1;
  }

  // ignore ClosedGet() here...

  int64_t n = 0;
  if ( ! invio.ntodo() ) {
    auto inbuf = invio.operator TSIOBuffer();

    this->on_input_ended(n); // externally aborted?

    // replaced by a callback?
    if ( inbuf == invio ) {
      invio.free(); // if same one it will be closed..
    }

    return -1; // can't use wakeup...
  }

  // bytes left exist ...

  n = invio.waiting(); // [don't call "waiting()" often]

  // at start and bytes present?
  if ( ! invio.ndone() && n > 0 ) { //  
    this->on_input(n); // new stream is started..
  }

  return n; // return bytes after reader...
}

void 
ATSVConnAPI::handleTSEvent(int closed, TSEvent event, void *evtdata, const std::shared_ptr<void>&)
{
  auto evtvio = ATSVIO{static_cast<TSVIO>(evtdata)}; // wrapper for it
  auto evtn = abs(waiting(evtvio));
  switch (event) {

    // wakeup for internal transfer 
    case TS_EVENT_IMMEDIATE:
    { // writing input has been activated
      auto n = bytes_ready();
      n > 0 ? on_reenabled(n) : (void)n;
      // ignore failed [repeat?] reenables
      break;
    }

    // external inward transfer halted (needs reenable)
    case TS_EVENT_VCONN_READ_READY:
    { // reading blocked until reactivated ... (reenable needed)
      if ( evtn >= TSIOBufferWaterMarkGet(evtvio) ) {
        this->on_read_blocked(evtn, evtvio.get()); // update internal marker (buffer)
      }
      buffer().nbytes(evtvio.nbytes()); // reset to latest if different..
      break;
    }

    case TS_EVENT_VCONN_READ_COMPLETE:
    { // end of read... (no action needed)
      this->on_read_ended(evtn, evtvio.get()); // update internal marker (buffer)
      buffer().nbytes(evtvio.nbytes()); // reset to last
      TSVConnClose(evtvio.vconn()); // any read source (non-IVC) should be closed
      break;
    }

    // external outward transfer halted (needs reenable)
    case TS_EVENT_VCONN_WRITE_READY:
      if ( evtvio.operator TSIOBuffer() != buffer() && evtvio.is_ivc_writer() ) { 
        this->on_writer_chain(evtvio.get()); // shift this previous write...
      } else {
        this->on_writer_blocked(evtn, evtvio.get()); // external pull/startup
      }
      break;

    case TS_EVENT_VCONN_WRITE_COMPLETE:
    { // writing to an external VC
      this->on_writer_ended(evtn, evtvio.get()); // external pull/startup
      break;
    }

    default:
    {
      this->on_event(event, evtdata);
      break;
    }
  }
}

// NOTE: auto-wakeup via VC for starting read/write 
TSVIO BufferVConn::add_output(TSVConn dstvc, int64_t len, int64_t skip) 
{
  auto dstInput = ATSVConn(dstvc).input();

  // need to enqueue a write?
  if ( dstInput.ready() && _resetWriteLen ) {
    return nullptr; // cannot wait for two
  }

  if ( dstInput.ready() ) {
    _resetWriteLen = len; // save length to use ...
    dstInput.send_dst_event(TS_EVENT_VCONN_PRE_ACCEPT, operator TSVConn()); 
    // call us back to start...
    return dstInput; // (correct VC at least)
  }

  // remote is not busy with another input

  if ( ! buffer() ) {
    TSVConnRead(*this,*this, TSIOBufferCreate(), 0); // symbolic read from "output-buffer" ...
    _currReader.reset( TSIOBufferReaderAlloc(buffer()) ); // mark empty buffer start
  }

  // TODO: use negative to fail a too-empty buffer
  if ( buffer().ntodo() < skip ) {
    return nullptr; // data not ready..
  }

  // give this to writer..
  _prevReader.reset( TSIOBufferReaderClone(_currReader.get()) );

  TSIOBufferReaderConsume(_currReader.get(),skip); // take onwards..

  // buffer is readable...
  auto vio = TSVConnWrite(dstvc, *this, _currReader.get(), len);

  if ( buffer().ntodo() == skip ) {
    _outvio = vio; // need reenable..
  }
  return vio;
}


void BufferVConn::on_input(int64_t bytes) // new input with data 
{
  auto skip = _inputSkip.exchange(0L); // new skips can be added
  auto n = input().ivc_transfer(skip); // use up skip length if present...
  switch ( n ) {
    case 0:
       add_input_skip(bytes); // recover skip bytes
       input().send_src_event(TS_EVENT_VCONN_WRITE_READY); // need reenable..
       break;
    case -1:
       // XXX error...
       add_input_skip(bytes); // recover skip bytes
       break;
    default: // not a "starting" stream any more...
       break;
  }
}

void BufferVConn::on_reenabled(int64_t) // possibly new data
{
  // actual amount..
  auto xfer = input().ivc_transfer();
  auto left = bytes_ready();
  // wait until buffer is more full?
  if ( xfer && _outvio.vconn() ) {
    TSVConnReenable(_outvio.vconn());
  }
  if ( xfer && _teevio.vconn() ) {
    TSVConnReenable(_outvio.vconn());
  }
}

void BufferVConn::on_input_ended(int64_t bytes) // input source is stopping
{
  if ( _outputQueue.empty() ) {
    return;
  }

    // in case this is unlocked?

  atscppapi::ScopedContinuationLock lock(_outputQueue.front());
  auto cont = _outputQueue.front();
  _outputQueue.erase(_outputQueue.begin());

  TSContCall(cont, TS_EVENT_VCONN_WRITE_READY, input().operator TSVIO());
}

TSVIO BufferVConn::add_stream(TSVConn vc) 
{
  return add_output(vc, INT64_MAX, 0);
}

TSVIO BufferVConn::add_tee_stream(TSVConn vc) 
{
  auto outvio = _outvio;
  _teevio = add_output(vc, INT64_MAX, 0);
  _outvio = outvio;
  return _teevio;
}

void BufferVConn::on_pre_input(TSCont wrreply) // new input with data 
{
  if ( input().ready() ) {
    _outputQueue.push_back(wrreply);
  } else {
    // TODO: instant-startup...
  }
}


// destination of reads (queueable)
void BufferVConn::on_read_blocked(int64_t bytes, ATSVIO vio) 
{
}
void BufferVConn::on_read_ended(int64_t bytes, ATSVIO vio) 
{
}

void BufferVConn::on_writer_chain(ATSVIO vio) 
{
  auto len = _resetWriteLen.exchange(0L);
  auto vconn = vio.vconn();
  vio.free(); // clear and shutdown
  add_output(vio.vconn(), len, 0);
}


// source of writes (queueable)
void BufferVConn::on_writer_blocked(int64_t bytes, ATSVIO vio) 
{
  // TODO: check if immediate reenable would work?
  if ( vio != _teevio && bytes ) {
    TSVConnReenable(vio.vconn());
    return;
  }
  if ( vio != _teevio ) {
    _outvio = vio;
  }

  // any situations to handle?
  auto n = bytes_ready(); // anything ready?
  if ( n > 0 ) {
    on_reenabled(n);
    return;
  }

  // can still refill?
  if ( ! n && input().ntodo() ) {
    input().send_src_event(TS_EVENT_VCONN_WRITE_READY);
  }

  // TODO: okay to ignore no result?
}

void BufferVConn::on_writer_ended(int64_t bytes, ATSVIO vio) 
{
}


ATSXformOutVConn::ATSXformOutVConn(const ATSXformCont &xform, TSIOBufferReader rdr, int64_t bytes, int64_t offset)
  : _cont(xform),
    _skipBytes(offset),
    _writeBytes(bytes),
    _outVIO(nullptr),
    _inVIO(xform.inputVIO())
{
  if ( ! bytes && offset && _inVIO ) {
    bytes = TSVIONBytesGet(_inVIO) - offset;
  }
  auto vconn = TSTransformOutputVConnGet(xform);
  // immediate call ...
  const_cast<TSVIO&>(_outVIO) = TSVConnWrite(vconn, _cont, rdr, bytes);
}

template <typename T_DATA> 
bool ATSFuture<T_DATA>::is_close_able() const
{
  using namespace std::chrono;
  using std::future_status;

  return ( ! std::shared_future<T_DATA>::valid() || this->wait_for(seconds::zero()) == future_status::ready );
}

template <typename T_DATA> 
int ATSFuture<T_DATA>::error() const
{
  using namespace std::chrono;
  using std::future_status;

  if ( ! std::shared_future<T_DATA>::valid() ) {
    return ENOENT;
  }
  if ( this->wait_for(seconds::zero()) != future_status::ready ) {
    return EAGAIN;
  }
  auto ptrErr = reinterpret_cast<intptr_t>(std::shared_future<T_DATA>::get());
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
    const_cast<TSVIO&>(_outVIO) = nullptr;
  }

  const_cast<TSVConn&>(_cont) = nullptr;

  DEBUG_LOG("shutdown-complete");
}

// Transform continuations
ATSXformCont::ATSXformCont(atscppapi::Transaction &txn, int64_t bytes, int64_t offset)
  : TSCont_t(TSTransformCreate(&ATSXformCont::handleXformTSEventCB, static_cast<TSHttpTxn>(txn.getAtsHandle()))),
    _txn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
    _xformCB( [](TSEvent evt, TSVIO vio, int64_t left) { DEBUG_LOG("xform-event empty body handler"); return 0; }),
    _outSkipBytes(offset),
    _outWriteBytes(bytes),
    _transformHook(TS_HTTP_LAST_HOOK),
    _outBufferU(TSIOBufferCreate()),  // until output starts..
    _outReaderU(TSIOBufferReaderAlloc(this->_outBufferU.get()))
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

  // get to method via callback
  DEBUG_LOG("output buffering begins with: %ldK",1L<<6);
  TSIOBufferWaterMarkSet(_outBufferU.get(), 1<<16); // start to flush early 
}

void
ATSXformCont::init_enabled_transform()
{
  if (_transformHook == TS_HTTP_LAST_HOOK) {
    DEBUG_LOG("transform txn disabled");
    return;
  }
  DEBUG_LOG("transform txn started e#%d", _transformHook);
  TSHttpTxnHookAdd(_txn, _transformHook, *this);
  _transformHook = TS_HTTP_LAST_HOOK;
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

  atscppapi::ScopedContinuationLock lock(*this);
  // get a new input source started upon next return ...
  TSContSchedule(*this,0,TS_THREAD_POOL_DEFAULT);
}

void ATSXformCont::reset_output_length(int64_t len) 
{
  _outWriteBytes = len; 
  if ( outputVIO() ) 
  {
    auto olen = TSVIONBytesGet(outputVIO());
    auto otodo = TSVIONTodoGet(outputVIO());
    TSVIONBytesSet(outputVIO(), len);
    if ( olen < len && ! otodo ) {
      TSVIOReenable(outputVIO()); // start it up again...
    }
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
    newRange += ( end > 0 ? std::to_string(1-end).c_str() : "-" );
  }

  sub_server_hdrs(origTxn, reqbuf, newRange);

  auto vconn = TSHttpConnectWithPluginId(origTxn.getClientAddress(), PLUGIN_NAME, 0); 
  auto wrlen = TSIOBufferReaderAvail(reqrdr);

  // prevent TSVConnClose()!
  TSVConnWrite(vconn, ATSCont::create_temp_tscont(nullptr, std::move(data)), reqrdr, wrlen);
  return vconn;
}

class BlockStoreXform;
class BlockReadXform;

// template ATSCont::ATSCont(BlockStoreXform &obj, void (BlockStoreXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);
// template ATSCont::ATSCont(BlockReadXform &obj, void (BlockReadXform::*funcp)(TSEvent, void *, const decltype(nullptr) &), decltype(nullptr),TSCont);
// template TSCont ATSCont::create_temp_tscont(TSCont, ATSVConnFuture &, const std::shared_ptr<void> &);

//}
