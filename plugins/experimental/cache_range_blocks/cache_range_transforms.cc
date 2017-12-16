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
ATSXformCont::check_completions(TSEvent event)
{
  TSVConn invconn = *this;
  auto invio      = TSVConnWriteVIOGet(invconn);

  // check for rude shutdown
  if (!invio || !TSVIOBufferGet(invio)) {
    return -1;
  }

  // another possible shutdown
  if (TSVConnClosedGet(invconn)) {
    _xformCB(event, nullptr, 0);
    DEBUG_LOG("xform-event closed: e#%d", event);
    return -2;
  }

  if ( !TSVIONTodoGet(invio) && _outVConnU && _outVConnU->is_close_able() ) {
    _outVConnU.reset(); // delete, flush and free
    return -3;
  }

  if ( !TSVIONTodoGet(invio) ) {
    DEBUG_LOG("xform-event input-only complete: e#%d", event);
    return -4;
  }

  return 0;
}

int
ATSXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  DEBUG_LOG("xform-event: e#%d %p", event, edata);

  ink_assert(this->operator TSVConn() == cont);

  if (check_completions(event)) {
    return 0;
  }

  auto xformIn = TSVConnWriteVIOGet(*this);
  TSVIO evio = ( event == TS_EVENT_IMMEDIATE ? xformIn : static_cast<TSVIO>(edata) );

  if ( _outVConnU && evio == *_outVConnU ) {
    TSVIO outVIO = *_outVConnU;

    switch ( event ) {
      case TS_EVENT_VCONN_WRITE_COMPLETE:
        // handle output-complete 
        _outVConnU->set_close_able();
        _xformCB(event, outVIO, 0); // notify...
        break;

      case TS_EVENT_VCONN_WRITE_READY:
        if ( _outVConnU->check_refill(event) ) {
          _xformCB(event, outVIO, 0); // no bytes present ...
        }
        break;

      default:
        DEBUG_LOG("unkn event from output: e#%d %p",event,outVIO);
        break;
    }
    return 0; ////////////// RETURN
  }

  if ( evio != xformIn && evio != _inVIO ) {
    DEBUG_LOG("unkn-event from unkn e#%d [%p]",event,evio);
    return 0;
  }

  if ( evio == xformIn ) {
    switch ( event ) {
      // can delay the end!
      case TS_EVENT_ERROR:
      case TS_EVENT_VCONN_WRITE_COMPLETE:
        forward_vio_event(event, evio); // upstream needs full shutdown now ...
        break;

      case TS_EVENT_IMMEDIATE:
        xform_input_event();
        break;

      default:
        DEBUG_LOG("xform-unkn-event from inVIO e#%d",event);
        break;
    }
    return 0;
  }

  // evio == _inVIO read

  switch ( event ) {
    case TS_EVENT_VCONN_READ_READY:
    {
      // shouldn't fail both...
      auto r = ! _outVConnU || ! _outVConnU->check_refill(event);
      ink_assert(r);
      break;
    }

    case TS_EVENT_VCONN_READ_COMPLETE:
      _xformCB(event, evio, 0); // no more fills ready...
      break;

    default:
      DEBUG_LOG("xform-unkn-event from inVIO e#%d",event);
      break;
  }

  return 0;
}

void
ATSXformCont::xform_input_event()
{
  // standard input is needed..
  auto xformIn = TSVConnWriteVIOGet(*this);

  if ( !_inVIO ) {
    _inVIO = xformIn;
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;
  }

  if ( ! using_two_buffers() ) {
    DEBUG_LOG("one-buffer called accidentally");
    return;
  }

  // edata is _inVIO or from it ...

  auto inrdr = TSVIOReaderGet(_inVIO); // reset if changed!

  auto inpos   = TSVIONDoneGet(_inVIO);
  auto inavail = TSIOBufferReaderAvail(inrdr);

  auto oinpos   = inpos;
  auto oinavail = inavail;

  auto inready = std::min(inavail, TSVIONTodoGet(_inVIO));

  /////////////
  // perform real callback if data is ready
  /////////////

  inready = _xformCB(TS_EVENT_IMMEDIATE, _inVIO, inready); // send bytes-left in segment

  inready = TSIOBufferCopy(outputBuffer(), inrdr, inready, 0); // copy them
  TSIOBufferReaderConsume(inrdr, inready);                     // advance the input used

  TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + inready);      // advance toward end

  DEBUG_LOG("!!! copy-next-copied : @%#lx+%#lx +%#lx", inpos, inready, inavail);

  // reset new positions
  inrdr   = TSVIOReaderGet(_inVIO); // reset if changed!
  inpos   = TSVIONDoneGet(_inVIO);
  inavail = TSIOBufferReaderAvail(inrdr);
  DEBUG_LOG(" ^^^^^^^^^^^ cb-event: @%#lx buff=+%#lx rdy=+%#lx [%p]", inpos, inavail, inready, inrdr);

  // no change at all and no data left?
  if ( ! inavail && oinpos == inpos ) {
    DEBUG_LOG("xform input waiting: @%#lx w/no change", inpos);
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
    return;
  }

  // have output and data was consumed?
  if ( oinavail > inavail ) {
    if ( _outVConnU ) {
      _outVConnU->check_refill(TS_EVENT_IMMEDIATE);
    } else {
      _outVConnU = ATSXformOutVConn::create_if_ready(*this, _writeBytes, _skipBytes);
    }
  }

  // the input is waiting for something...
  if ( inavail ) {
    DEBUG_LOG("xform input buffer left: buff=%#lx", TSIOBufferReaderAvail(inrdr));
    _inVIOWaiting = TS_EVENT_IMMEDIATE;
    return; 
    // RETURN
  }

  // buffer was fully consumed...

  if ( _inVIOWaiting ) {
    DEBUG_LOG("xform input wait-flushed: @%#lx -> @%#lx", oinpos, inpos);
    _inVIOWaiting = TS_EVENT_NONE;
  }

  if (check_completions(TS_EVENT_IMMEDIATE)) {
    return; 
    // RETURN
  }

  DEBUG_LOG("xform input pull: @%#lx -> @%#lx", oinpos, inpos);
  forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
}

bool
ATSXformOutVConn::check_refill(TSEvent event)
{
  auto outready = TSIOBufferReaderAvail(*this);

  // time to flag for Reenable?
  if ( ! outready ) {
    _outVIOWaiting = event; // can't do it now...
    return true;
  }

  // ready to start write?
  if ( ! _outVIO && outready >= _skipBytes ) {
    TSIOBufferReaderConsume(*this, _skipBytes);
    _outVIO = TSVConnWrite(_outVConn,_outVConn,_outReaderU.get(),_writeBytes);
    return false;
  }

  // not ready
  if ( ! _outVIO ) {
    return false; // need more to start
  }

  // past end of write?
  if ( !TSVIONTodoGet(_outVIO) ) {
    TSIOBufferReaderConsume(*this, outready);
    _outVIOWaiting = TS_EVENT_NONE; // flushed new data...
    return false;
  }

  // event with empty buffer above?
  if ( _outVIOWaiting ) {
    DEBUG_LOG("xform flush reenable: @%#lx w/avail", outready);
    TSVIOReenable(*this);
    _outVIOWaiting = TS_EVENT_NONE; // flushed new data...
  }

  // not waiting and not at end...

  return false;
}

int64_t
ATSXformCont::skip_next_len(int64_t left)
{
  if (!left || !_inVIO) {
    return 0L;
  }

  TSIOBufferReader reader    = nullptr;

  if ( using_one_buffer() && _outVConnU && ! static_cast<TSVIO>(*_outVConnU) ) {
    reader = *_outVConnU; // VIO hasn't started yet ...
  } else if ( using_two_buffers() ) {
    reader = TSVIOReaderGet(_inVIO);
  } 

  auto done = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - done);

  if ( ! reader ) {
    DEBUG_LOG("!!! cannot skip currently: @%#lx+%#lx limit=%#lx", done, left, violimit);
    return 0L;
  }

  auto nbytesin = TSIOBufferReaderAvail(reader);
  auto ncopied  = std::min(nbytesin + 0, violimit);

  if (!ncopied) {
    DEBUG_LOG("!!! skip-next-none : @%#lx+%#lx +%#lx +%#lx", done, left, nbytesin, violimit);
    return 0L;
  }

  // TSIOBufferCopy(outputBuffer(), _inReader, ncopied, 0); // copy them
  TSIOBufferReaderConsume(reader, ncopied);                // advance input

  if ( using_two_buffers() ) {
    DEBUG_LOG("!!! skip-input-write: @%#lx+%#lx +%#lx +%#lx", done, left, nbytesin, ncopied);
    TSVIONDoneSet(_inVIO, TSVIONDoneGet(_inVIO) + ncopied); // advance toward end
  } else {
    DEBUG_LOG("!!! skip-pre-write: @%#lx+%#lx +%#lx +%#lx", done, left, nbytesin, ncopied);
  }

  return ncopied;
}

int64_t
BlockTeeXform::inputEvent(TSEvent event, TSVIO evtvio, int64_t left)
{
  if (!evtvio) {
    DEBUG_LOG("xform output complete");
    _writeHook(nullptr, 0, 0);
    return left;
  }

  if (!left) {
    return left;
  }

  if (evtvio == outputVIO()) {
    DEBUG_LOG("tee output event: e#%d +%#lx", event, left);
  } else if (evtvio != inputVIO()) {
    DEBUG_LOG("unkn vio event: e#%d +%#lx %p", event, left, evtvio);
  }

  // position *without* left new bytes...
  auto done   = TSVIONDoneGet(inputVIO());
  auto oavail = TSIOBufferReaderAvail(_teeReaderP.get());
  DEBUG_LOG("tee buffer bytes pre-copy: @%#lx+%#lx+%#lx", done - oavail, oavail, left);
  left = TSIOBufferCopy(_teeBufferP.get(), TSVIOReaderGet(inputVIO()), left, 0);

  auto navail = TSIOBufferReaderAvail(_teeReaderP.get());
  DEBUG_LOG("tee buffer bytes post-copy: @%#lx+%#lx [+%#lx]", done - oavail, navail, left);

  return _writeHook(_teeReaderP.get(), done, left); // show advance
}
