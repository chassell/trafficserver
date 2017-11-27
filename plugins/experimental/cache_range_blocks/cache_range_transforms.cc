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
void
forward_vio_event(TSEvent event, TSVIO invio)
{
  if ( invio && TSVIOContGet(invio) && TSVIOBufferGet(invio)) {
    TSContCall(TSVIOContGet(invio), event, invio);
  }
}

int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  DEBUG_LOG("xform-event CB: %d %p",event,edata);
  atscppapi::ScopedContinuationLock lock(*this);

  ink_assert(this->operator TSVConn() == cont);

  // check for rude shutdown
  if ( TSVConnWriteVIOGet(*this) != _inVIO || ! TSVIOBufferGet(_inVIO) ) 
  {
    if ( _xformCB ) {
      _xformCB(event, nullptr, 0); // may be an init!
      DEBUG_LOG("xform-event shutdown: %d %p",event,edata);
      return 0;
    }

    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;

    // finally know the length of resp-hdr (as is)
    _outHeaderLen = _txn.getServerResponseHeaderSize();

    TSVConn invconn = *this;
    _inVIO = TSVConnWriteVIOGet(invconn);
    _outVConn = TSTransformOutputVConnGet(invconn);
    // just for init
  }

  // another possible shutdown
  if ( TSVConnClosedGet(*this) ) {
    _xformCB(event, nullptr, 0);
    DEBUG_LOG("xform-event closed: %d %p",event,edata);
    return 0;
  }

  // "ack" end-of-write completion [zero bytes] to upstream
  if ( ! TSVIONTodoGet(_inVIO) ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,_inVIO); // complete upstream
    DEBUG_LOG("xform-event write-complete: %d %p",event,edata);
    return 0;
  }

  // if output is done ... then shutdown 
  if ( _outVIO && event == TS_EVENT_VCONN_WRITE_COMPLETE ) {
    TSVConnShutdown(_outVConn, 0, 1); // no more events please
    _outVIO = nullptr;
    return 0; // nothing more for this event
  }

  auto inrdr = TSVIOReaderGet(_inVIO); // reset if changed!

  auto outpos = ( _outVIO ? TSVIONDoneGet(_outVIO) : 0 );
  auto pos = TSVIONDoneGet(_inVIO);
  auto opos = pos;
  auto inavail = TSIOBufferReaderAvail(inrdr);
  auto inready = inavail;

  if ( event == TS_EVENT_IMMEDIATE ) {
    event = TS_EVENT_VCONN_WRITE_READY;
    edata = _inVIO; // overwrite the Event*
  }

  if ( event != TS_EVENT_VCONN_WRITE_READY || edata != _inVIO ) {
    DEBUG_LOG("xform-unkn-event : @%ld %+ld %+ld [%p][%p]",pos,inavail,inready,inrdr,edata);
    return 0;
  }

  for(;;) {
    inready = std::min(inready,_xformCBAbsLimit - pos);
    DEBUG_LOG(" ----------- xform-event cb-prep: @%ld in=%+ld inrdy=%+ld [%p]",pos,inavail,inready,inrdr);

    /////////////
    // perform real callback if data is ready
    /////////////
    
    _xformCB(event, _inVIO, inready); // send bytes-left in segment

    // reset new positions
    inrdr = TSVIOReaderGet(_inVIO); // reset if changed!
    pos = TSVIONDoneGet(_inVIO);
    inready = TSIOBufferReaderAvail(inrdr); // start high again...
    DEBUG_LOG("xform-event cb-after: @%ld oin=%+ld inrdy=%+ld [%p]",pos,inavail,inready,inrdr);

    if ( pos < _xformCBAbsLimit || pos > _nextXformCBAbsLimit ) {
      break;
    }

    // go again...
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;
    DEBUG_LOG("xform advance cb: @%ld -> @%ld",pos,_xformCBAbsLimit);
  }

  auto noutpos = ( _outVIO ? TSVIONDoneGet(_outVIO) : 0 );

  if ( outpos != noutpos ) {
    DEBUG_LOG("xform reenable: @%ld -> @%ld",outpos,noutpos);
    TSVIOReenable(_outVIO);
  }

  // if done with this buffer ....
  if ( opos != pos && ! TSIOBufferReaderAvail(inrdr) ) {
    DEBUG_LOG("xform input ready: @%ld -> @%ld",opos,pos);
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
  }

  return 0;
}

int64_t
APIXformCont::copy_next_len(int64_t left)
{
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
  auto viopos = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - viopos);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    DEBUG_LOG("!!! copy-next-none : @%ld%+ld %+ld %+ld [%p]",viopos,left,nbytesin,violimit,inrdr);
    return 0L;
  }

  ncopied = TSIOBufferCopy(outputBuffer(), inrdr, ncopied, 0); // copy them
  TSIOBufferReaderConsume(inrdr,ncopied); // advance input
  viopos = TSVIONDoneGet(_inVIO);  // advance toward end
  TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + ncopied);  // advance toward end

  DEBUG_LOG("!!! copy-next-reenable : @%ld%+ld %+ld %+ld [%p]",viopos,left,nbytesin,ncopied,inrdr);
  TSVIOReenable(_outVIO);
  return ncopied;
}

int64_t
APIXformCont::skip_next_len(int64_t left)
{
  // copy op defined by:
  //    0) input reader: prev. skipped bytes (consumed)
  //
  //    1) input reader: added new bytes
  //    2) VIO:          op-limit bytes
  //    3) endpos:       copy-limit bytes

  auto inrdr = TSVIOReaderGet(_inVIO);
  auto nbytesin = TSIOBufferReaderAvail(inrdr);
  auto viopos = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - viopos);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    DEBUG_LOG("!!! skip-next-none : @%ld%+ld %+ld %+ld [%p]",viopos,left,nbytesin,violimit,inrdr);
    return 0L;
  }

  // TSIOBufferCopy(outputBuffer(), _inReader, ncopied, 0); // copy them
  TSIOBufferReaderConsume(inrdr,ncopied); // advance input
  TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + ncopied);  // advance toward end

  DEBUG_LOG("!!! skip-next-amount : @%ld%+ld %+ld %+ld [%p]",viopos,left,nbytesin,ncopied,inrdr);
  return ncopied;
}

void
APIXformCont::init_body_range_handlers(int64_t len, int64_t offset)
{
  // #4) xform handler: skip trailing download from server
  auto skipBodyEndFn = [this](TSEvent evt, TSVIO vio, int64_t left) { 
    auto r = this->skip_next_len( this->call_body_handler(evt,vio,left) ); // last one...
    auto pos = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform skip-end: @%ld left=%+ld r=+%ld",pos,left,r);
    return r;
  };

  // #3) xform handler: copy body required by client from server
  auto copyBodyFn = [this,len,skipBodyEndFn](TSEvent evt, TSVIO vio, int64_t left) { 
    this->_outVIO = TSVConnWrite(this->_outVConn, *this, this->_outReaderP.get(), len);
    auto r = this->copy_next_len( this->call_body_handler(evt,vio,left) );

    this->set_copy_handler(INT64_MAX>>1,skipBodyEndFn);
    auto pos = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform copy-body: @%ld left=%+ld r=%+ld",pos,left,r);
    return r;
  };

  // #2) xform handler: skip an offset of bytes from server
  auto skipBodyOffsetFn = [this,offset,len,copyBodyFn](TSEvent evt, TSVIO vio, int64_t left) { 
    auto r = this->skip_next_len( this->call_body_handler(evt,vio,left) );

    this->set_copy_handler(offset+len,copyBodyFn);
    auto pos = TSVIONDoneGet(_inVIO);
    DEBUG_LOG("xform skip-body: @%ld left=%+ld r=%+ld",pos,left,r);
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
    DEBUG_LOG("tee output event: #%d %+ld",event,left);
  } else  if ( evtvio != inputVIO() ) {
    DEBUG_LOG("unkn vio event: #%d %+ld %p",event,left,evtvio);
  }

  // position *without* left new bytes...
  auto pos = TSVIONDoneGet(inputVIO());
  auto oavail = TSIOBufferReaderAvail(_teeReaderP.get());

  DEBUG_LOG("tee buffer bytes pre-copy: @%ld%+ld%+ld",pos-oavail,oavail,left);
  TSIOBufferCopy(_teeBufferP.get(), TSVIOReaderGet(inputVIO()), left, 0);

  auto navail = TSIOBufferReaderAvail(_teeReaderP.get());
  DEBUG_LOG("tee buffer bytes post-copy: @%ld%+ld [%+ld]",pos-(navail-left),navail,left);

//  return _writeHook(_teeReaderP.get(), pos - outHeaderLen(), left); // show advance
  return _writeHook(_teeReaderP.get(), pos, left); // show advance
}

/*
void
firstTransformationPluginRead(vconn)
{
  _invio = TSVConnWriteVIOGet(vconn);
  _input_reader = TSVIOReaderGet(_invio);

  // after first write (ideally)
  _output_vconn = TSTransformOutputVConnGet(vconn);
  _output_buffer = TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K);
  _output_reader = TSIOBufferReaderAlloc(_output_buffer);
  _outvio = TSVConnWrite(output_vconn, vconn, _output_reader, INT64_MAX); // all da bytes

}

int
BlockStoreXform::handleTransformationPluginEvents(TSEvent event, TSVConn vconn)
{
  if (TSVConnClosedGet(vconn)) {
    // closed connection_closed
    return 0;
  }

  if ( ! TSVConnWriteVIOGet(contp) ) {
    return; // became null!?!
  }

  switch ( event ) {
    case TS_EVENT_VCONN_WRITE_COMPLETE:
      TSVConnShutdown(_output_vconn, 0, 1); // The other end is done reading our output
      break;
    case TS_EVENT_ERROR:
      throw_vio_event(TS_EVENT_ERROR,invio);
      break;
    default:
      if (TSVIONBytesGet(_invio) <= TSVIONDoneGet(_invio)) {
        throw_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,invio);
        break; // now done with reads
      }

      // cannot read anything?
      avail = TSIOBufferReaderAvail(_input_reader);
      if ( ! avail ) {
        break;
      }

      //  to_read = TSIOBufferCopy(_buffer, _input_reader, std::min(avail,TSVIONTodoGet(_invio)), 0);
      //  readChars(_buffer); // read what's in buffer now...
      copied = TSIOBufferCopy(_output_buffer, _input_reader, std::min(avail,TSVIONTodoGet(_invio)), 0);
      TSIOBufferReaderConsume(_input_reader, copied); // not to re-read
      TSVIONDoneSet(_invio, TSVIONDoneGet(_invio) + copied); // decrement todo

      if ( TSVIONTodoGet(_invio) <= 0 ) {
        throw_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE);
        break; // now done with reads
      }

      TSVIOReenable(_invio);
      throw_vio_event(TS_EVENT_VCONN_WRITE_READY,invio);
      break;
  }
}

size_t
close_output()
{
  ink_assert(_outvio);

  // one sign of a close exists?  not a race?
  if ( TSVConnClosedGet(_vconn) || TSVConnClosedGet(_vconn) ) {
    // LOG_ERROR("TransformationPlugin=%p tshttptxn=%p unable to reenable outvio=%p connection was closed=%d.", this,
    return _bytes_written; // nothing to do
  }
 

  // reset where we are...
  TSVIONBytesSet(_outvio, _bytes_written);
  TSVIOReenable(_outvio); // Wake up the downstream vio
  return _bytes_written;
}

shutdown_empty_downstream()
{
  // make sure a write occurs to signal the end
  TSVIONDoneSet(_outvio, 0);
  TSVIOReenable(_outvio); // Wake up the downstream vio
}

*/

/*
void readBufferChars(buffer)
{
  consumed = 0;
  // assert-that (TSIOBufferReaderAvail(_reader) > 0) 
  for( TSIOBufferBlock block = TSIOBufferReaderStart(_reader) ; block ; block=TSIOBufferBlockNext(block) ) {
    char_data = TSIOBufferBlockReadStart(block, reader, &data_len); // char_data, data_len
    consumed += data_len;
  }

  TSIOBufferReaderConsume(_reader, consumed);
}

size_t produceOutputChars(const std::string &data)
{
  // Finally we can copy this data into the output_buffer
  int64_t len_written = TSIOBufferWrite(state_->output_buffer_, write_data, write_length);
  _bytes_written += len_written;

  if (! TSVConnClosedGet(_vconn)) {
    TSVIOReenable(_outvio); // Wake up the downstream vio
  }
}
*/

