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

TSIOBuffer_t ATSVConn::g_emptyBuffer{ TSIOBufferCreate() };
TSIOBufferReader_t ATSVConn::g_emptyReader{ TSIOBufferReaderAlloc(g_emptyBuffer.get()) };

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
ATSCont::create_temp_tscont(TSCont mutexSrc, const std::shared_ptr<void> &ref)
{
  auto contp = std::make_unique<ATSCont>(mutexSrc); // uses empty stub-callback!!
  auto &cont = *contp; // hold scoped-ref

  // assign new handler
  cont = [&cont, ref](TSEvent evt, void *data) {
    decltype(contp) contp(&cont); // free after this call
    intptr_t ptrErr = reinterpret_cast<intptr_t>(data);
    if (ptrErr >= 0 && ptrErr < INK_START_ERRNO + 1000) {
      auto deleter = std::get_deleter<std::function<void(void*)>>(ref);
      if (deleter) {
		(*deleter)(reinterpret_cast<void*>(ref.use_count())); // callback immediately to show an error...
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

void ATSVIO::shutdown() {
  if ( ! _vio || ! ready() ) {
    return;
  }

  atscppapi::ScopedContinuationLock lock{vconn()};

  if ( is_ivc_writer() && ! ivc_reader().ready() ) {
    TSVConnClose(vconn()); // shutting last side down..
  } else if ( is_ivc_reader() && ! ivc_writer().ready() ) {
    TSVConnClose(vconn()); // shutting last side down..
  } else if ( ready() ) {
    TSVConnShutdown(vconn(), ! write_ready(), write_ready());
  }
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
  if ( ! cont() || is_ivc_writer() ) {
    return;
  }

  // ivc_readers and all other ATSVIOs get *all* resources freed
  atscppapi::ScopedContinuationLock lock(cont());
  TSIOBufferReader_t{operator TSIOBufferReader()}; // (if any)
  TSIOBuffer_t{operator TSIOBuffer()}; // writers have other ways...
}

void ATSVIO::complete_vio() 
{
  if ( ! _vio || ! ntodo() ) {
    return;
  }

  TSVIONBytesSet(_vio, ndone()); // cut it short now...

  auto rdr = operator TSIOBufferReader();
  if ( rdr ) {
    TSIOBufferReaderConsume(rdr, TSIOBufferReaderAvail(rdr)); // flush bytes...
    send_chg_event(TS_EVENT_VCONN_WRITE_COMPLETE); // upstream notified...
  } else {
    send_chg_event(TS_EVENT_VCONN_READ_COMPLETE); // downstream notified...
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
int64_t ATSVConnAPI::chk_input_ready(int64_t xfer)
{
  auto invio = input();
  if ( ! invio || ! invio.ready() ) {
    return -1;
  }

  // ignore ClosedGet() here...

  if ( ! invio.ntodo() ) {
    auto inbuf = invio.operator TSIOBuffer();

    this->on_input_ended(xfer); // externally aborted?

    // same and still complete?
    if ( ! invio.ntodo() && inbuf == invio.buffer() ) {
      invio.free(); // shutdown this write..
    }

    return -1;
  }

  // bytes left exist ...

  xfer = ( xfer ? : invio.waiting() ); // [don't call "waiting()" often]

  // at start and bytes present?
  if ( ! invio.ndone() && xfer > 0 ) { //  
    this->on_input(xfer); // new stream is started..
  }

  return xfer; // return bytes after reader...
}

void 
ATSVConnAPI::handleVCEvent(int closed, TSEvent event, void *evtdata, std::shared_ptr<void>&ref)
{
  if ( closed || ! ref ) {
    ref.reset();
    return; // no event handling allowed..
  }

  auto evtvio = ATSVIO{static_cast<TSVIO>(evtdata)}; // wrapper for it
  auto evtn = abs(data_window(evtvio));
  switch (event) {
    // wakeup for internal transfer 
    case TS_EVENT_IMMEDIATE:
    { // writing input has been activated
      this->on_reenabled(0); // xfer determines bytes
      break;
    }

    // external inward transfer halted (needs reenable)
    case TS_EVENT_VCONN_READ_READY:
    { // reading blocked until reactivated ... (reenable needed)
      // [NOTE: trust the window/gap added]
      if ( evtn >= TSIOBufferWaterMarkGet(evtvio) ) {
        this->on_read_blocked(evtn, evtvio.get()); // update internal marker (buffer)
      } else {
        data().nbytes(evtvio.nbytes()); // update buffered amt
      }
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
        // data window is likely meaningless...
        this->on_writer_blocked(evtvio.get()); // external pull/startup
      }
      break;

    case TS_EVENT_VCONN_WRITE_COMPLETE:
    { // writing to an external VC
      if ( evtvio.buffer() != data() && evtvio.is_ivc_writer() ) { 
        this->on_writer_chain(evtvio.get()); // shift this previous write...
      } else if ( evtvio.buffer() == data() ) {
        this->on_writer_ended(evtvio.get()); // external pull/startup
      }
      break;
    }

    default:
    {
      this->on_event(event, evtdata);
      break;
    }
  }

  // if closed because of event .. leave data behind
  if ( TSVConnClosedGet(*this) && ref ) {
    auto deleter = std::get_deleter<std::function<void(void*)>>(ref);
    if (deleter) {
		(*deleter)(reinterpret_cast<void*>(ref.use_count())); // callback immediately to show an error...
	}
	ref.reset(); // clear counted ref
  }
}


BufferVConn::~BufferVConn()
{
  input().complete_vio();
  if ( _outvio ) {
    _outvio.complete_vio(); // truncate..
    _outvio.free(); // shutdown from our buffer
  }
  if ( _teevio ) {
    _teevio.complete_vio(); // truncate..
    _teevio.free(); // shutdown from our buffer
  }
  error_shutdown(0);
}

void BufferVConn::init() 
{
  if ( ! _cont ) {
    _cont = ATSCont{static_cast<ATSVConnAPI&>(*this), &BufferVConn::handleVCEvent, std::shared_ptr<void>(), nullptr };
  }

  if ( ! data() && ! error() ) {
    TSVConnRead(*this,*this, TSIOBufferCreate(), 0); // symbolic read from "output-buffer" ...
    _bufferBaseReaderU.reset( TSIOBufferReaderAlloc(data()) ); // mark empty buffer start
    _baseReader = _bufferBaseReaderU.get();
  }
}

TSVIO BufferVConn::add_ext_input(TSVConn srcvc, int64_t len) 
{
  init();

  if ( input() ) {
    return nullptr;
  }

  // assign placeholder...
  set_dummy_input();

  return TSVConnRead(srcvc,*this,data(),len);
}

TSVIO BufferVConn::add_outflow(ATSVConnAPI &dstvc, int64_t len, int64_t skip) 
{
  return add_outflow_any(dstvc, len, skip, &dstvc);
}

// NOTE: auto-wakeup via VC for starting read/write 
TSVIO BufferVConn::add_outflow(TSVConn dstvc, int64_t len, int64_t skip) 
{
  return add_outflow_any(dstvc, len, skip);
}

TSVIO BufferVConn::add_outflow_any(TSVConn dstvc, int64_t len, int64_t skip, ATSVConnAPI *vcapi)
{
  init();

  if ( error() || ! _baseReader || _nextBasePos < data_base() ) {
    return nullptr; // we're shut down or cannot queue another write ...
  }

  // (only valid if remote is VConn)
  auto dstVIO = ATSVConn(dstvc).pullvio();

  // need to wait for remote vconn while write is active?  nope.
  if ( dstVIO && ( _currReader || ! vcapi ) ) {
    return nullptr; // cannot wait during active..
  }

  on_reenabled(0); // complete any writes currently?

  // consume up to skip point of input
  _nextBasePos += skip; // consume more before next ...
  
  // wait on new input? [_currReader is null --> no queue]
  if ( _currReader || data().nbytes() < _nextBasePos ) {
    auto waiting = TSVConnWrite(dstvc, *this, g_emptyReader.get(), len);
    if ( _outputQueue.empty() ) {
      _outputQueue.push_back(std::make_pair(waiting,_nextBasePos+len));
    }
    return waiting; // correct VIO addr for future...
  }

  // no need to wait on input or queue...

  TSIOBufferReaderConsume(_baseReader,skip); // take out skip amt ...
  data().drain(skip); // forward by N

  _bufferAuxReaderU.reset( TSIOBufferReaderClone(_baseReader) );
  _currReader = _bufferAuxReaderU.get();

  // compute next pos after write 
  if ( len > (INT64_MAX>>1) ) {
    _nextBasePos = -1L; // expected length is unknown...
  } else {
    _nextBasePos = data_reader_pos(_currReader) + len; // next pos expected...
  }

  // reader and nextPos ready...

  // waiting on remote input??
  if ( dstVIO && vcapi ) {
    auto &vcbase = *vcapi;
    _outvio = dstVIO; // as if write-ready...
    // call us back to start...

    atscppapi::ScopedContinuationLock lock(vcbase); // lock destination
    vcbase.on_pre_input(*this, len);
    return nullptr; // (correct VC at least)
  }

  TSVConnWrite(dstvc, *this, _currReader, len);

  // no bytes left to full point? (or empty?)
  _outvio = data().ntodo() ? ATSVIO() : dstVIO; // need reenable..

  return dstVIO;
}


// new input with data ---> call reenables if needed
void BufferVConn::on_input(int64_t xfer) 
{
  init(); // detected new input VIO?

  xfer = input().ivc_transfer(0); // try again?

  if ( xfer <= 0 ) {
    this->on_writer_blocked(nullptr); // detect if event needed...
    return;
  }

  // got zero bytes (after skip) from new input!

  this->on_read_blocked(xfer,input()); // send reenable if needed
}

void BufferVConn::on_reenabled(int64_t rdy) // possibly new data
{
  // not a new input...
  auto xfer = input().ivc_transfer(); // no skipping
  if ( xfer > 0 ) {
    this->on_read_blocked(xfer,input()); // send reenable if needed
  }
}

// input source is stopping --> reset input if needed
void BufferVConn::on_input_ended(int64_t xfer) 
{
  if ( input() ) {
    input().send_chg_event(TS_EVENT_VCONN_WRITE_COMPLETE); // callback..
  }

  atscppapi::ScopedContinuationLock lock(*this); // lock self

  if ( ! _preInputs.empty() ) {
    auto vc = _preInputs.front().first;
    auto len = _preInputs.front().second;
    _preInputs.erase(_preInputs.begin()); // only call once...

    input().free(); // shutdown inward write (if not)
    this->on_pre_input(vc,len); // request an upstream input
  }
}

TSVIO BufferVConn::add_stream(TSVConn vc) 
{
  return add_outflow(vc, INT64_MAX, 0);
}

TSVIO BufferVConn::add_tee_stream(TSVConn vc) 
{
  auto outvio = _outvio;
  _teevio = add_outflow(vc, INT64_MAX, 0);
  _outvio = outvio;
  return _teevio;
}

void BufferVConn::on_pre_input(TSCont wrreply, int64_t len) // new input with data 
{
  atscppapi::ScopedContinuationLock ulock(wrreply);
  atscppapi::ScopedContinuationLock llock(*this);

  if ( input().ready() ) {
    _preInputs.push_back(std::make_pair(wrreply,len)); // pull out when done...
    return;
  }

  // make a new input() vio.. [maybe extra immed event]
  TSVConnWrite(*this, wrreply, g_emptyReader, len);

  // TODO: instant-startup?
  if ( ! len ) { 
    input().send_chg_event(TS_EVENT_VCONN_WRITE_COMPLETE); // synchronous ...
  } else {
    input().send_chg_event(TS_EVENT_VCONN_WRITE_READY); // synchronous ...
  }

  // did they close or ignore the input instead?
  if ( ! input() ) {
    input_shutdown(); // we should shut down all backlogged inputs..
  } else if ( ! input().ntodo() ) {
    on_input_ended(0); // toss this silliness
  }
}

void BufferVConn::on_read_blocked(int64_t added, ATSVIO vio) 
{
  // buffer advanced in *some* fashion...

  atscppapi::ScopedContinuationLock lock(*this);

  if ( vio == input() ) {
    added = chk_input_ready(added);
  } else if ( ! _currReader && data().nbytes() < _nextBasePos && _nextBasePos <= vio.nbytes() ) {
    added = vio.nbytes() - data().nbytes();
    data().fill(added); // reset end of bytes vio

    auto rdr = _baseReader;
    auto skip = _nextBasePos - data_reader_pos(rdr);

    TSIOBufferReaderConsume(rdr, skip); // reader is now shaped up..
    on_writer_ended(nullptr); // dequeue a write if ready..
  } else {
    added = vio.nbytes() - data().nbytes();
    data().fill(added); // reset end of bytes vio
  }

  if ( added && _outvio ) {
    _outvio.reenable();
    _outvio = nullptr;
  }
  if ( added && _teevio ) {
    _teevio.reenable();
  }
}

void BufferVConn::on_read_ended(int64_t added, ATSVIO vio) 
{
  this->on_read_blocked(added,vio);
  if ( dummy_input() && vio.buffer() == data().buffer() ) {
    input().free();  // remove placeholder
  }
  vio.shutdown(); // no resources so do shutdown 
}

void BufferVConn::on_writer_chain(ATSVIO vio) 
{
  auto vconn = vio.vconn(); // target vconn
  auto len = ( ! vio.ntodo() && _nextBasePos > data_base() 
                    ? _nextBasePos - data_base()
                    : vio.ntodo() );
  vio.free(); // close the old one...

  if ( error() ) {
    return; // erase the old one and that's it!
  }

  // replace with a real one?
  if ( _outvio == vio && _currReader ) {
    TSVConnWrite(vconn, *this, _currReader, len);
    return; // replaced with *real* write finally...
  }

  // create the replacement (or queue it)
  if ( ! add_outflow(vconn, len) ) {
    error_shutdown(EPIPE); // couldn't complete write!
  }
}

// writer from our buffer is blocked ...
void BufferVConn::on_writer_blocked(ATSVIO vio) 
{
  if ( vio && vio != _teevio ) {
    _outvio = vio; // if bytes in window, perform reenable next
  }

  // any situations to handle?
  auto n = chk_input_ready(0); // unkn...
  if ( n > 0 ) {
    this->on_reenabled(n); // some ready..
    return;
  }

  // ongoing read?
  if ( dummy_input() && vio.waiting() ) {
    vio.reenable(); 
    return;
  }

  if ( dummy_input() ) {
    return; // just gotta wait..
  }

  // no extInput and an empty input?
  if ( ! n ) {
    input().send_chg_event(TS_EVENT_VCONN_WRITE_READY);
    return;
  }

  // input never started?  [keep waiting like a pipe]
  if ( ! data_newest() ) {
    return;
  }

  // input was shutdown

  vio.complete_vio(); // the end is nigh..
  vio.reenable(); // and show its time to complete
}

void BufferVConn::on_writer_ended(ATSVIO vio) 
{
  // protect/free our buffer-readers...

  if ( vio == _teevio ) {
    TSIOBufferReader_t{_teevio}; // quicklyh free our cloned reader 
    return;
  }

  auto rdr = vio.reader();

  if ( ! rdr && _currReader ) { // if pre-closed...
    rdr = _currReader;
  }

  atscppapi::ScopedContinuationLock lock(*this); // (probably redundant)

  uint64_t newbase = 0LL;

  if ( rdr == _bufferAuxReaderU.get() ) {
    _bufferBaseReaderU.swap(_bufferAuxReaderU);
  } else ( rdr && rdr != _bufferBaseReaderU.get() ) {
    _bufferBaseReaderU.reset(rdr);
  }

  // prepare for a sequential next write...
  if ( rdr && (newbase=data_reader_pos(rdr)) > data_base() ) {
    _baseReader = rdr;  // not new position
    _bufferAuxReaderU.reset(); // clear current position
    _currReader = nullptr;
    data().ndone(newbase);
  }

  if ( ! _outputQueue.empty() ) {
    auto pair = _outputQueue.front();
    _outputQueue.erase(_outputQueue.begin()); // only call once
    this->on_writer_chain(pair.first); // create new write from it
  }
}

void ProxyVConn::on_event(TSEvent evt, void *edata) 
{
  switch (evt) {
    case TS_EVENT_TIMEOUT:
    {
      _retry(*this); // may be reentrant
      break;
    }

    case TS_EVENT_CACHE_OPEN_WRITE_FAILED:
    {
      if ( ! _retry ) {
        input().send_chg_event(TS_EVENT_VCONN_EOS);
        break;
      }

      // gotta try again!
      atscppapi::ScopedContinuationLock lock(*this);
      TSContSchedule(*this, 30, TS_THREAD_POOL_TASK);
    }

    case TS_EVENT_CACHE_OPEN_READ_FAILED:
      if ( _outvio ) {
        error_shutdown(-reinterpret_cast<intptr_t>(edata));
      }
      break;

    case TS_EVENT_CACHE_OPEN_WRITE: // start up if no others active!
    {
      auto cvconn = static_cast<TSVConn>(edata);
      auto len = input().nbytes() ? : data().ntodo();
      if ( ! add_outflow(cvconn, len, 0) ) { // write the input or the buffer's fullest point
        error_shutdown(EBUSY);
      }
      break;
    }

    case TS_EVENT_CACHE_OPEN_READ: // immediately start up!
    {
      auto cvconn = static_cast<TSVConn>(edata);
      auto len = TSVConnCacheObjectSizeGet(cvconn); // get precise size now...

      if ( _outvio && _outvio.nbytes() != INT64_MAX && _outvio.nbytes() != len ) {
        error_shutdown(ENOTBLK);
      }

      if ( ! add_ext_input(cvconn,len) ) {
      }
      break;
    }

    default:
      ink_assert(!"unknown event");
      break;
  }
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

// Transform continuations
ATSXformVConn::ATSXformVConn(atscppapi::Transaction &txn, int64_t bytes, int64_t offset)
  : TransactionPlugin(txn), BufferVConn(txn, std::shared_ptr<TSContO_t>()),
    _txn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
    _transformHook(TS_HTTP_LAST_HOOK)
{
  ink_assert( bytes + offset >= 0LL );

  // point back here
  if ( ! offset && ! (bytes % ( 1 << 20 )) ) {
    _transformHook = TS_HTTP_RESPONSE_CLIENT_HOOK; // no active output pumping needed...
  } else {
    _transformHook = TS_HTTP_RESPONSE_TRANSFORM_HOOK;
  }

  // get to method via callback
  DEBUG_LOG("output buffering begins with: %ldK",1L<<6);
  TSIOBufferWaterMarkSet(data().buffer(), 1<<16); // start to flush early 
}

ATSXformVConn::~ATSXformVConn()
{
  DEBUG_LOG("final destruct %p",operator TSVConn());

  _transformHook = TS_HTTP_LAST_HOOK; // start no transform on late events...
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
