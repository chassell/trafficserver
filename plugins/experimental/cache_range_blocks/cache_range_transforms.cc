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
#include "cache_range_blocks.h"

#include "atscppapi/HttpStatus.h"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

// namespace
// {
//
int
APIXformCont::check_completions(TSEvent event) 
{
  TSVConn invconn = *this;
  auto invio = TSVConnWriteVIOGet(invconn);

  // check for rude shutdown
  if ( ! invio || ! TSVIOBufferGet(invio) ) {
    DEBUG_LOG("xform-event shutdown: #%d",event);
    return -1;
  }

  // another possible shutdown
  if ( TSVConnClosedGet(invconn) ) {
    _xformCB(event, nullptr, 0);
    DEBUG_LOG("xform-event closed: #%d",event);
    return -2;
  }

  // "ack" end-of-write completion [zero bytes] to upstream
  if ( ! TSVIONTodoGet(invio) ) {
    DEBUG_LOG("xform-event write-complete: #%d",event);
    // can dealloc entire transaction!!
    _xformCB(event, nullptr, 0);
    TSVIONBytesSet(_outVIO, TSVIONBytesGet(_outVIO)); // define it as complete
    TSVIOReenable(_outVIO);
    TSVConnClose(_outVConn); // attempt faster close??
    TSVConnShutdown(_outVConn,0,1); // attempt faster close??
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,invio); 
    return -3;
  }

  return 0;
}

int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  DEBUG_LOG("xform-event CB: %d %p",event,edata);

  ink_assert(this->operator TSVConn() == cont);

  TSVConn invconn = *this;
  if ( check_completions(event) ) {
    return 0;
  }

  if ( ! _inVIO ) {
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;

    // finally know the length of resp-hdr (as is)
    _outHeaderLen = _txn.getServerResponseHeaderSize();

    _inVIO = TSVConnWriteVIOGet(invconn);
    _outVConn = TSTransformOutputVConnGet(invconn);
  }

  // if output is done ... then shutdown 
  if ( event == TS_EVENT_VCONN_WRITE_COMPLETE ) {
    TSVConnClose(_outVConn);
    TSVConnShutdown(_outVConn, 0, 1); // no more events please
    _outVIO = nullptr;
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,_inVIO); 
    return 0; // nothing more for this event
  }

  // just for init
  auto inrdr = TSVIOReaderGet(_inVIO); // reset if changed!

  auto inpos = TSVIONDoneGet(_inVIO);
  auto odone = inpos;
  auto inavail = TSIOBufferReaderAvail(inrdr);
  auto inready = inavail;

  if ( event == TS_EVENT_IMMEDIATE ) {
    edata = _inVIO; // overwrite the Event*
  }

  if ( event != TS_EVENT_VCONN_WRITE_READY && event != TS_EVENT_VCONN_WRITE_COMPLETE && event != TS_EVENT_IMMEDIATE ) {
    DEBUG_LOG("xform-unkn-event : @%#lx +%#lx +%#lx [%p][%p]",inpos,inavail,inready,inrdr,edata);
    return 0;
  }

  if ( edata != _inVIO ) {
    DEBUG_LOG("xform-unkn-edata : @%#lx +%#lx +%#lx [%p][%p]",inpos,inavail,inready,inrdr,edata);
  }

  for(;;) {
    inready = std::min(inready,_xformCBAbsLimit - inpos);
    DEBUG_LOG(" ----------- xform-event cb-prep: @%#lx in=+%#lx inrdy=+%#lx [%p]",inpos,inavail,inready,inrdr);

    /////////////
    // perform real callback if data is ready
    /////////////
    
    _xformCB(event, static_cast<TSVIO>(edata), inready); // send bytes-left in segment

    // reset new positions
    inrdr = TSVIOReaderGet(_inVIO); // reset if changed!
    inpos = TSVIONDoneGet(_inVIO);
    inready = TSIOBufferReaderAvail(inrdr); // start high again...
    DEBUG_LOG("xform-event cb-after: @%#lx oin=+%#lx inrdy=+%#lx [%p]",inpos,inavail,inready,inrdr);

    if ( inpos < _xformCBAbsLimit || inpos > _nextXformCBAbsLimit ) {
      break;
    }

    // go again...
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;
    DEBUG_LOG("xform advance cb: @%#lx -> @%#lx",inpos,_xformCBAbsLimit);
  }

  if ( event == TS_EVENT_VCONN_WRITE_READY && TSIOBufferReaderAvail(outputReader()) ) {
    DEBUG_LOG("xform reenable: @%#lx avail",TSIOBufferReaderAvail(outputReader()));
    TSVIOReenable(_outVIO);
  }

  // nothing moved or reader has extra?
  if ( odone == inpos || TSIOBufferReaderAvail(inrdr) ) {
    return 0;
  }

  // if done with this buffer ....
  DEBUG_LOG("xform input ready: @%#lx -> @%#lx",odone,inpos);
  if ( TSVIONTodoGet(_inVIO) ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
  }

  return 0;
}

int64_t
APIXformCont::copy_next_len(int64_t left)
{
  if ( ! left ) {
    return 0L;
  }

  auto invio = TSVConnWriteVIOGet(*this);
  if ( ! invio || ! TSVIOBufferGet(invio) ) {
    DEBUG_LOG("lost buffer");
    return -1;
  }

  // copy op defined by:
  //    0) input reader: prev. skipped bytes (consumed)
  //
  //    1) input reader: added new bytes
  //    2) VIO:          op-limit bytes
  //    3) endpos:       copy-limit bytes

  auto inrdr = TSVIOReaderGet(_inVIO);
  auto nbytesin = TSIOBufferReaderAvail(inrdr);
  auto done = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - done);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    DEBUG_LOG("!!! copy-next-none : @%#lx+%#lx +%#lx +%#lx [%p]",done,left,nbytesin,violimit,inrdr);
    return 0L;
  }

  ncopied = TSIOBufferCopy(outputBuffer(), inrdr, ncopied, 0); // copy them
  TSIOBufferReaderConsume(inrdr,ncopied); // advance input
  done = TSVIONDoneGet(_inVIO);  // advance toward end
  TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + ncopied);  // advance toward end

  DEBUG_LOG("!!! copy-next-copied : @%#lx+%#lx +%#lx +%#lx [%p]",done,left,nbytesin,ncopied,inrdr);
  return ncopied;
}

int64_t
APIXformCont::skip_next_len(int64_t left)
{
  if ( ! left ) {
    return 0L;
  }

  // copy op defined by:
  //    0) input reader: prev. skipped bytes (consumed)
  //
  //    1) input reader: added new bytes
  //    2) VIO:          op-limit bytes
  //    3) endpos:       copy-limit bytes

  auto inrdr = TSVIOReaderGet(_inVIO);
  auto nbytesin = TSIOBufferReaderAvail(inrdr);
  auto done = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - done);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    DEBUG_LOG("!!! skip-next-none : @%#lx+%#lx +%#lx +%#lx [%p]",done,left,nbytesin,violimit,inrdr);
    return 0L;
  }

  // TSIOBufferCopy(outputBuffer(), _inReader, ncopied, 0); // copy them
  TSIOBufferReaderConsume(inrdr,ncopied); // advance input
  TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + ncopied);  // advance toward end

  DEBUG_LOG("!!! skip-next-amount : @%#lx+%#lx +%#lx +%#lx [%p]",done,left,nbytesin,ncopied,inrdr);
  return ncopied;
}

void
APIXformCont::init_body_range_handlers(int64_t len, int64_t offset)
{
  // #4) xform handler: skip trailing download from server
  auto skipBodyEndFn = [this](TSEvent event, TSVIO vio, int64_t left) { 
    auto r = skip_next_len( call_body_handler(event,vio,left) ); // last one...
    auto done = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform skip-end: @%#lx+%#lx left=+%#lx",done-r,r,left);
    return r;
  };

  if ( ! len ) {
    set_copy_handler(INT64_MAX>>1,skipBodyEndFn);
    DEBUG_LOG("xform init no-write complete");
    return;
  }

  // #3) xform handler: copy body required by client from server
  auto copyBodyFn = [this,len,skipBodyEndFn](TSEvent event, TSVIO vio, int64_t left) { 
    _outVIO = TSVConnWrite(_outVConn, *this, _outReaderP.get(), len);
    auto r = copy_next_len( call_body_handler(event,vio,left) );
    set_copy_handler(INT64_MAX>>1,skipBodyEndFn);
    auto done = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform copy-body: @%#lx+%#lx left=+%#lx",done-r,r,left);
    return r;
  };

  if ( ! offset ) {
    set_copy_handler(len, copyBodyFn); // don't start a write
    DEBUG_LOG("xform init no-skip complete");
    return;
  }

  // #2) xform handler: skip an offset of bytes from server
  auto skipBodyOffsetFn = [this,offset,len,copyBodyFn](TSEvent event, TSVIO vio, int64_t left) { 
    auto r = skip_next_len( call_body_handler(event,vio,left) );
    set_copy_handler(offset+len,copyBodyFn);
    auto done = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform skip-body: @%#lx+%#lx left=+%#lx",done-r,r,left);
    return r;
  };

  set_copy_handler(offset, skipBodyOffsetFn);
  DEBUG_LOG("xform init complete");
}

int64_t BlockTeeXform::handleEvent(TSEvent event, TSVIO evtvio, int64_t left)
{
  if ( ! evtvio ) {
    DEBUG_LOG("xform output complete");
    _writeHook(nullptr,0,0);
    return left;
  }

  if ( ! left ) {
    return left;
  }

  if ( evtvio == outputVIO() ) {
    DEBUG_LOG("tee output event: #%d +%#lx",event,left);
  } else  if ( evtvio != inputVIO() ) {
    DEBUG_LOG("unkn vio event: #%d +%#lx %p",event,left,evtvio);
  }

  // position *without* left new bytes...
  auto done = TSVIONDoneGet(inputVIO());
  auto oavail = TSIOBufferReaderAvail(_teeReaderP.get());

  DEBUG_LOG("tee buffer bytes pre-copy: @%#lx+%#lx+%#lx",done-oavail,oavail,left);
  TSIOBufferCopy(_teeBufferP.get(), TSVIOReaderGet(inputVIO()), left, 0);

  auto navail = TSIOBufferReaderAvail(_teeReaderP.get());
  DEBUG_LOG("tee buffer bytes post-copy: @%#lx+%#lx [+%#lx]",done-(navail-left),navail,left);

  return _writeHook(_teeReaderP.get(), done, left); // show advance
}

