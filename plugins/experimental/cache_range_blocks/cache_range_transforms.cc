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
#include "ts/ink_time.h"

#include <atscppapi/HttpStatus.h>

int
ATSXformCont::xform_input_completion(TSEvent event)
{
  TSVConn xfinvconn = *this;

  // check for rude shutdown?
  if (!xfinvconn) {
    DEBUG_LOG("xform-event null-vconn event: e#%d", event);
    return -1;
  }

  TSVIO xfinvio = xformInputVIO();

  // check for rude shutdown?
  if (!TSVIOBufferGet(xfinvio)) {
    DEBUG_LOG("xform-event no buffer event: e#%d", event);
    return -1;
  }

  // bytes are not transferred and VConn is not closed early
  // ** ONLY ** if this is past can a shutdown start [i.e. input exhausted]
  if ( TSVIONTodoGet(xfinvio) && ! TSVConnClosedGet(xfinvconn) ) {
    return 0; // not complete at all...
  }

  // closed suddenly?
  if ( TSVConnClosedGet(xfinvconn) ) {
    if ( outputVIO() ) {
      _xformCB(event, outputVIO(), 0); // notify...
      _outVConnU.reset(); // delete, flush and free
    }
    _xformCB(event, nullptr, 0);
    DEBUG_LOG("xform-event closed: e#%d", event);
    return -2; // cannot proceed when closed
  }

  if ( outputVIO() && ! _outVConnU->is_close_able() ) {
    DEBUG_LOG("xform-event input-only complete: @%#lx / @%#lx e#%d", TSVIONDoneGet(xfinvio), TSVIONDoneGet(outputVIO()), event);
    return -3; // cannot proceed with no data
  }

  // input N-Todo is complete...
  if ( outputVIO() ) {
    _xformCB(event, outputVIO(), 0); // notify...
    _outVConnU.reset(); // delete, flush and free
    // DROP THROUGH...
  }

  // attempt a start of shutdown...

  if ( xfinvio == _inVIO ) {
    DEBUG_LOG("xform-event one-input pos @%#lx",TSVIONDoneGet(_inVIO));
  } else {
    DEBUG_LOG("xform-event two-input pos @%#lx / @%#lx",TSVIONDoneGet(xfinvio),TSVIONDoneGet(_inVIO));
  }

  // input and output both complete
  _xformCB(event, xfinvio, 0);
  forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE, xfinvio); // required for upstream...
  return -5; // cannot proceed with no data
}

void
ATSXformCont::handleXformOutputEvent(TSEvent event)
{
  DEBUG_LOG("xform-event from outVIO e#%d",event);
  TSVIO outvio = *_outVConnU;

  switch ( event ) {
    case TS_EVENT_VCONN_WRITE_COMPLETE:
      // handle an early output-complete?
      if ( ! _outVConnU->is_close_able() ) {
        _outVConnU->set_close_able(); // in case something cut it early...
      }
      _xformCB(event, outputVIO(), 0); // notify...

      if ( ! TSVIONDoneGet(xformInputVIO()) ) {
        TSVIONDoneSet( xformInputVIO(), TSVIONBytesGet(xformInputVIO()) );
        forward_vio_event(TS_EVENT_VCONN_WRITE_READY, xformInputVIO());   // skip
      }
      break;

    case TS_EVENT_VCONN_WRITE_READY:
      if ( _outVConnU->check_refill(event) ) {
        _xformCB(event, outvio, 0); // no bytes present ...
      }
      break;

    default:
      DEBUG_LOG("unkn event from output: e#%d %p",event,outvio);
      break;
  }
}

void
ATSXformCont::handleXformInputEvent(TSEvent event, TSVIO evio)
{
    DEBUG_LOG("xform-event from xformInVIO e#%d",event);
    switch ( event ) {
      case TS_EVENT_IMMEDIATE:
        if ( ! xform_input_completion(event) ) {
          xform_input_event();
        }
        break;

      // can delay the end!
      case TS_EVENT_ERROR:
      case TS_EVENT_VCONN_WRITE_COMPLETE:
        forward_vio_event(event, xformInputVIO()); // upstream needs full shutdown now ...
        break;

      default:
        DEBUG_LOG("xform-unkn-event from xformInVIO e#%d",event);
        break;
    }
}

void
ATSXformCont::handleXformBufferEvent(TSEvent event, TSVIO evio)
{
  // evio == _inVIO and a one-buffer write
  DEBUG_LOG("xform-event from buffer VIO e#%d",event);

  switch ( event ) {
    case TS_EVENT_VCONN_READ_READY:
    {
      // shouldn't fail both...
      auto r = ! _outVConnU || ! _outVConnU->check_refill(event);
      ink_assert(r);
      break;
    }

    case TS_EVENT_VCONN_READ_COMPLETE:
      _outVConnU->check_refill(event); // if first is last
      _xformCB(event, _inVIO, 0); // no more fills ready...
      break;

    default:
      DEBUG_LOG("xform-unkn-event buffer VIO e#%d",event);
      break;
  }
}


int
ATSXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  ThreadTxnID txnid{_txn};
  DEBUG_LOG("xform-event: e#%d %p", event, edata);

  ink_assert(this->operator TSVConn() == cont);

  TSVIO evio = ( event == TS_EVENT_IMMEDIATE ? xformInputVIO() : static_cast<TSVIO>(edata) );

  ink_assert(evio);

  // output got this?
  if ( _outVConnU && evio == *_outVConnU ) {
    handleXformOutputEvent(event);
    xform_input_completion(event); // check if complete ...
  } else if ( evio == xformInputVIO() ) {
    handleXformInputEvent(event, evio);
    xform_input_completion(event); // check if complete ...
  } else if ( using_one_buffer() ) {
    handleXformBufferEvent(event,evio); // don't check xform
  } else {
    ink_assert( ! evio || ! using_two_buffers() );
    DEBUG_LOG("unkn-event from unkn e#%d [%p]",event,evio);
  }
  
  return 0;
}

void
ATSXformCont::xform_input_event()
{
  const auto event = TS_EVENT_IMMEDIATE; // only way in ...

  // only create at precise start..
  if ( ! TSVIONDoneGet(xformInputVIO()) && ! _outVConnU ) {
    DEBUG_LOG("create xform write vio: len=%#lx skip=%#lx", _outWriteBytes, _outSkipBytes);
    _outVConnU = ATSXformOutVConn::create_if_ready(*this, _outWriteBytes, _outSkipBytes);
    _outVConnU->check_refill(event);
  }

  auto xfinrdr = TSVIOReaderGet(xformInputVIO());
  auto avail = TSIOBufferReaderAvail(xfinrdr);

  if ( using_one_buffer() ) {
    TSIOBufferReaderConsume(xfinrdr, avail); // skip
    // TSVIONDoneSet( xformInputVIO(), TSVIONDoneGet(xformInputVIO()) + avail );
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, xformInputVIO());   // skip
    DEBUG_LOG("consume input-buffer event: avail %ld",avail);
    return;
  }

  // standard input is needed if not present
  if ( !_inVIO ) {
    _inVIO = xformInputVIO();
  }

  // event edata is _inVIO or from it ...

  auto inrdr = TSVIOReaderGet(_inVIO); // reset if changed!

  auto inpos   = TSVIONDoneGet(_inVIO);
  auto inavail = TSIOBufferReaderAvail(inrdr);

  auto oinpos   = inpos;
  auto oinavail = inavail;

  auto inready = std::min(inavail, TSVIONTodoGet(_inVIO));

  /////////////
  // perform real callback if data is ready
  /////////////

  inready = _xformCB(event, _inVIO, inready); // send bytes-left in segment

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
  if ( oinavail > inavail && _outVConnU ) {
    _outVConnU->check_refill(event);
  }

  // the input is waiting for something...
  if ( inavail ) {
    DEBUG_LOG("xform input buffer left: buff=%#lx", TSIOBufferReaderAvail(inrdr));
    _inVIOWaiting = event;
    return; 
    // RETURN
  }

  // buffer was fully consumed...

  if ( _inVIOWaiting ) {
    DEBUG_LOG("xform input wait-flushed: @%#lx -> @%#lx", oinpos, inpos);
    _inVIOWaiting = event;
  }

  if (xform_input_completion(event)) {
    return; // RETURN
  }

  DEBUG_LOG("xform input pull: @%#lx -> @%#lx", oinpos, inpos);
  forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
}

bool
ATSXformOutVConn::check_refill(TSEvent event)
{
  auto outready = ( _outVIO ? TSIOBufferReaderAvail(TSVIOReaderGet(_outVIO)) : TSIOBufferReaderAvail(_outReader) );
  auto pos = ( _outVIO ? TSVIONDoneGet(_outVIO) : 0 ) - _skipBytes;

  // ready to start write?
  if ( ! _outVIO && outready >= _skipBytes ) {
    if ( _skipBytes ) {
      DEBUG_LOG("xform begin write: skip %#lx w/extra +%#lx", _skipBytes, outready + pos);
      TSIOBufferReaderConsume(_outReader, _skipBytes);
      outready = TSIOBufferReaderAvail(_outReader);
    }
    DEBUG_LOG("xform begin write: %#lx w/avail %#lx", _writeBytes, outready);
    _outVIO = TSVConnWrite(_outVConn,_inVConn,_outReader,_writeBytes);
    return false;
  }

  // not ready
  if ( ! _outVIO ) {
    DEBUG_LOG("xform no-write with early data: @-%#lx w/avail %#lx", - (outready + pos), outready);
    return false; // need more to start
  }

  auto left = TSVIONTodoGet(_outVIO);

  // past end of write?
  if ( ! left ) {
    DEBUG_LOG("xform consume past end: @%#lx w/avail %#lx", pos, outready);
    TSIOBufferReaderConsume(_outReader, outready);
    _outVIOWaiting = TS_EVENT_NONE; // flushed new data...
    return false;
  }

  // time to flag for Reenable?
  if ( ! outready ) {
    DEBUG_LOG("xform empty: @-%#lx w/avail 0", pos);
    _outVIOWaiting = event; // can't do it now...
    _outVIOWaitingTS = ink_microseconds(MICRO_REAL); // can't do it now...
    return true;
  }

  if ( ! _outVIOWaiting ) {
    DEBUG_LOG("xform no-wait no-end: @%#lx w/avail %#lx", TSVIONDoneGet(_outVIO), outready);
    // not waiting and not at end...
    return false;
  }

  auto delay = ( ink_microseconds(MICRO_REAL) - _outVIOWaitingTS ); // can't do it now...

  // not full enough and delay is too small (<10ms RTT)?
  if ( outready < std::min(left+0,0x20000L) && delay < 10000 ) {
    return false;
  }

  DEBUG_LOG("xform flush reenable: @%#lx w/avail %#lx", TSVIONDoneGet(_outVIO),outready);
  TSVIOReenable(_outVIO);
  _outVIOWaiting = TS_EVENT_NONE; // flushed new data...
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

TSIOBufferReader
BlockTeeXform::cloneAndSkip(int64_t skip)
{
  auto ordr = _teeReaderP.release(); // caller must free it
  _teeBufferP.release(); // caller must free it

  TSIOBufferReader_t ordr2( TSIOBufferReaderClone(ordr) ); // delete upon return

  TSIOBufferReaderConsume(ordr2.get(), skip); // advance input to next point

  _teeBufferP.reset(TSIOBufferCreate());
  _teeReaderP.reset(TSIOBufferReaderAlloc(_teeBufferP.get()));

  auto left = TSIOBufferReaderAvail(ordr2.get());
  TSIOBufferCopy(_teeBufferP.get(), ordr2.get(), left, 0); // save extra bytes as before
  return ordr;
}

