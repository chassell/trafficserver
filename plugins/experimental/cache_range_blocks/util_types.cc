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

TSIOBuffer_t BufferVConn::g_emptyBuffer{ TSIOBufferCreate() };
TSIOBufferReader_t BufferVConn::g_emptyReader{ TSIOBufferReaderAlloc(g_emptyBuffer.get()) };

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

  auto rdr = operator TSIOBufferReader();
  if ( rdr ) {
    TSVIONBytesSet(_vio, ndone()); // cut it short now...
    TSIOBufferReaderConsume(rdr, TSIOBufferReaderAvail(rdr)); // flush bytes...
    send_chg_event(TS_EVENT_VCONN_WRITE_COMPLETE); // notified...
  } else {
    TSVIONBytesSet(_vio, ndone()); // cut it short now...
    send_chg_event(TS_EVENT_VCONN_READ_COMPLETE); // notified...
  }
}

int64_t
ATSVIO::ivc_copy(int64_t inskip)
{
  auto buf = ivc_data();
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
    ivc_data().fill(n);
  }
  return n;
}

/// deciphering all situations that occur with IMMEDIATE/wakeup-only 
int64_t ATSVConnAPI::input_ready()
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

    // src buffer is ended only once...
    if ( inbuf == invio.buffer() ) {
      invio.free(); // shutdown
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
      auto n = input_ready();
      n > 0 ? this->on_reenabled(n) : (void)n;
      // ignore failed [repeat?] reenables
      break;
    }

    // external inward transfer halted (needs reenable)
    case TS_EVENT_VCONN_READ_READY:
    { // reading blocked until reactivated ... (reenable needed)
      if ( evtn >= TSIOBufferWaterMarkGet(evtvio) ) {
        this->on_read_blocked(evtn, evtvio.get()); // update internal marker (buffer)
      }
      data().nbytes(evtvio.nbytes()); // reset to latest if different..
      break;
    }

    case TS_EVENT_VCONN_READ_COMPLETE:
    { // end of read... (no action needed)
      this->on_read_ended(evtn, evtvio.get()); // update internal marker (buffer)
      data().nbytes(evtvio.nbytes()); // reset to last
      TSVConnClose(evtvio.vconn()); // any read source (non-IVC) should be closed
      break;
    }

    // external outward transfer halted (needs reenable)
    case TS_EVENT_VCONN_WRITE_READY:
      if ( evtvio.buffer() != data() && evtvio.is_ivc_writer() ) { 
        this->on_writer_chain(evtvio.get()); // shift this previous write...
      } else if ( evtvio.buffer() == data() ) {
        this->on_writer_blocked(evtn, evtvio.get()); // external pull/startup
      }
      break;

    case TS_EVENT_VCONN_WRITE_COMPLETE:
    { // writing to an external VC
      if ( evtvio.buffer() != data() && evtvio.is_ivc_writer() ) { 
        this->on_writer_chain(evtvio.get()); // shift this previous write...
      } else if ( evtvio.buffer() == data() ) {
        this->on_writer_ended(evtn, evtvio.get()); // external pull/startup
      }
      break;
    }

    default:
    {
      this->on_event(event, evtdata);
      break;
    }
  }
}


BufferVConn::~BufferVConn()
{
  input().complete_vio();
  if ( _outvio ) {
    _outvio.complete_vio();
    _outvio.free(); // shutdown
  }
  if ( _teevio ) {
    _teevio.complete_vio();
    _teevio.free(); // shutdown
  }
  input().free(); // shutdown

  _prevReader.reset();
  _currReader.reset();
  data().free(); // free buffer
  data().close_vconn(); // not closed elsewhere
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
    dstInput.send_vc_event(TS_EVENT_VCONN_PRE_ACCEPT, operator TSVConn()); 
    // call us back to start...
    return dstInput; // (correct VC at least)
  }

  // remote is not busy with another input

  // grab our own input if any!?!
  on_reenabled(0);

  // data() is non-null now

  // TODO: use negative to fail a too-empty buffer
  if ( data().ntodo() < skip ) {
    return nullptr; // data not ready..
  }

  if ( _currReader && skip ) {
    return nullptr; // skipping write cannot be queued...
  }

  if ( _currReader ) {
    auto waiting = TSVConnWrite(dstvc, *this, g_emptyReader.get(), len);
    _outputQueue.push_back(waiting);
    return waiting; // correct VIO addr...
  }

  _currReader.reset( TSIOBufferReaderClone(_prevReader.get()) );

  TSIOBufferReaderConsume(_currReader.get(),skip); // take onwards..

  // buffer is readable...
  auto vio = TSVConnWrite(dstvc, *this, _currReader.get(), len);

  // no bytes left to full point?
  if ( data().ntodo() == skip ) {
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
       input().send_chg_event(TS_EVENT_VCONN_WRITE_READY); // need reenable..
       break;
    case -1:
       // XXX error...
       add_input_skip(bytes); // recover skip bytes
       break;
    default: // not a "starting" stream any more...
       break;
  }
}

void BufferVConn::on_reenabled(int64_t n) // possibly new data
{
  if ( ! data() ) {
    // used for both cont and vconn
    TSVConnRead(*this,*this, TSIOBufferCreate(), 0); // symbolic read from "output-buffer" ...
    _prevReader.reset( TSIOBufferReaderAlloc(data()) ); // mark empty buffer start
  }

  auto n = input().ivc_transfer(); // if anything...
  if ( n > 0 ) {
    this->on_read_blocked(n,input()); // handle awaken once..
  }
}

void BufferVConn::on_input_ended(int64_t bytes) // input source is stopping
{
  input().free(); // shutdown inward write

  atscppapi::ScopedContinuationLock lock(*this); // lock self

  if ( ! _preInputs.empty() ) {
    auto cont = _preInputs.front();
    _preInputs.erase(_preInputs.begin()); // only call once...
    this->on_pre_input(cont); // upstream requested output
  }
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
  atscppapi::ScopedContinuationLock ulock(wrreply);
  atscppapi::ScopedContinuationLock llock(*this);

  if ( input().ready() ) {
    _preInputs.push_back(wrreply); // pull out when done...
    return;
  } 

  // make a new input() vio.. [maybe extra immed event]
  ATSVIO{ TSVConnWrite(*this, wrreply, g_emptyReader.get(), 0) };

  // TODO: instant-startup...
  input().send_chg_event(TS_EVENT_VCONN_WRITE_READY); // synchronous ...
}

void BufferVConn::on_read_blocked(int64_t bytes, ATSVIO vio) 
{
  atscppapi::ScopedContinuationLock llock(*this);
  auto left = input_ready();
  if ( vio != input() ) {
    data().nbytes(vio.nbytes()); // reset data vio
  }
  if ( left && _outvio ) {
    _outvio.reenable();
    _outvio = nullptr;
  }
  if ( left && _teevio ) {
    _teevio.reenable();
  }
}

void BufferVConn::on_read_ended(int64_t bytes, ATSVIO vio) 
{
  this->on_read_blocked(bytes,vio);
  vio.shutdown(); // no resources ... so shutdown VIO
}

void BufferVConn::on_writer_chain(ATSVIO vio) 
{
  auto vconn = vio.vconn(); // target vconn
  auto ntodo = vio.ntodo(); // incomplete length
  vio.free();

  if ( ! ntodo && _resetWriteLen ) {
    ntodo = _resetWriteLen.exchange(0L); // take a nonzero length 
  }

  add_output(vconn, ntodo, 0); // queue up the replacement ...
}

// writer from our buffer is blocked ...
void BufferVConn::on_writer_blocked(int64_t bytes, ATSVIO vio) 
{
  // TODO: check if immediate reenable would work?
  if ( vio != _teevio && bytes ) {
    atscppapi::ScopedContinuationLock llock(vio.cont());
    atscppapi::ScopedContinuationLock dlock(vio.vconn());
    TSVConnReenable(vio.vconn()); // an extra wakeup?
    return;
  }

  if ( vio != _teevio ) {
    _outvio = vio;
  }

  // any situations to handle?
  auto n = input_ready(); // anything ready?
  if ( n > 0 ) {
    this->on_reenabled(n);
    return;
  }

  // can still refill?
  if ( ! n && input().ntodo() ) {
    input().send_chg_event(TS_EVENT_VCONN_WRITE_READY);
  }

  // TODO: okay to ignore no result?
}

void BufferVConn::on_writer_ended(int64_t bytes, ATSVIO vio) 
{
  if ( vio == _teevio ) {
    TSIOBufferReader_t{_teevio}; // free the cloned reader (not stored)
    _teevio.free(); // shutdown 
    return;
  }

  if ( vio == _outvio ) {
    _outvio.free(); // don't touch it
  }

  ink_assert( vio.reader() == _currReader.get() );

  _prevReader.reset(_currReader.release()); // shift forward
  data().ndone( data().nbytes() - vio.waiting() ); // shift ndone forward

  vio.free(); // shutdown outward write

  atscppapi::ScopedContinuationLock lock(*this); ///// locked

  if ( ! _outputQueue.empty() ) {
    auto vio = _outputQueue.front();
    _outputQueue.erase(_outputQueue.begin()); // only call once
    this->on_writer_chain(vio); // create new write from it
  }
}

void ProxyVConn::on_event(TSEvent evt, void *data) 
{
  switch (evt) {
    case TS_EVENT_CACHE_OPEN_WRITE_FAILED:
        break;
    case TS_EVENT_CACHE_OPEN_READ_FAILED:
        break;
    case TS_EVENT_CACHE_OPEN_WRITE:
        break;
    case TS_EVENT_CACHE_OPEN_READ:
        break;
    default:
        break;
  }
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
  TSVIOReenable(xformInputVIO());
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
  TSVIOReenable(xformInputVIO());
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
