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

enum {
  eNoError=0,
  eErrXformNoBuff=1,
  eErrXformClosed=2,
  eErrXformOutputWait=3,
  eErrXformComplete=4,
  eErrXformExtraImmEvent=5
};

const char *
ATSXformCont::xform_input_completion_desc(int error)
{
  if ( _inLastError == error ) {
    return nullptr;
  }

  _inLastError = error;

  switch (abs(error)) {
    case eErrXformExtraImmEvent:
       return "xform-event input unchanged";
    case eErrXformNoBuff: 
       return "xform-event no buffer event";
    case eErrXformClosed:
       return "xform-event closed";
    case eErrXformOutputWait:
       return "xform-event output-wait";
    case eErrXformComplete:
       return "xform-event completed";
    default:
       break;
  }
  return nullptr;
}

int
ATSXformCont::xform_input_completion(TSEvent event)
{
  TSVConn xfinvconn = *this;
  TSVIO xfinvio = xformInputVIO();

  // check for rude shutdown?
  if (!TSVIOBufferGet(xfinvio)) {
    return eErrXformNoBuff;
  }

  // bytes are not transferred and VConn is not closed early
  // ** ONLY ** if this is past can a shutdown start [i.e. input exhausted]
  if ( TSVIONTodoGet(xfinvio) && ! TSVConnClosedGet(xfinvconn) ) {
    return eNoError; // not complete at all...
  }

  // input at-end or input closed

  // input closed suddenly/on-error?
  if ( TSVConnClosedGet(xfinvconn) ) {
    if ( outputVIO() ) {
      _outVConnU.reset(); // flush and free
      _outVIOWaiting = TS_EVENT_HTTP_TXN_CLOSE;
    }
    _xformCB(TS_EVENT_HTTP_TXN_CLOSE, nullptr, 0); // signal it's been aborted...
    return eErrXformClosed; // cannot proceed as closed...
  }

  // input at-end 

  // waiting on output?
  if ( outputVIO() && ! _outVConnU->is_close_able() ) {
    return eErrXformOutputWait; // cannot proceed with no data
  }

  // input at-end and output can now be closed...
  if ( outputVIO() ) {
    _outVConnU.reset(); // delete, flush and free
    _outVIOWaiting = TS_EVENT_HTTP_TXN_CLOSE;
    // CONTINUE ON...
  }

  // input at-end and output shut down..

  // input and output both complete
  _xformCB(TS_EVENT_VCONN_WRITE_COMPLETE, xfinvio, 0);
  if ( xfinvio == _inVIO ) {
   DEBUG_LOG("xform-event xform-input pos @%#lx",TSVIONDoneGet(_inVIO));
  } else {
   DEBUG_LOG("xform-event buffer-input pos [xform@%#lx] @%#lx",TSVIONDoneGet(xfinvio),TSVIONDoneGet(_inVIO));
  }
  forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE, xfinvio, *this); // DANGER: may be reentrantly destroyed!!
  return eErrXformComplete; // cannot proceed with no data
}

int
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
      _xformCB(TS_EVENT_VCONN_WRITE_COMPLETE, outputVIO(), 0); // notify...

      if ( ! TSVIONDoneGet(xformInputVIO()) ) {
        TSVIONDoneSet( xformInputVIO(), TSVIONBytesGet(xformInputVIO()) );
        forward_vio_event(TS_EVENT_VCONN_WRITE_READY, xformInputVIO(), *this);   // skip real writes
      }
      break;

    case TS_EVENT_VCONN_WRITE_READY:
      if ( _outVConnU->check_refill(event) ) {
        _xformCB(TS_EVENT_VCONN_WRITE_READY, outvio, 0); // no bytes present ...
      }
      break;

    default:
      DEBUG_LOG("unkn event from output: e#%d %p",event,outvio);
      break;
  }
  return xform_input_completion(event); // detect any problems..
}

int
ATSXformCont::handleXformInputEvent(TSEvent event, TSVIO evio)
{
  auto err = 0;

  switch ( event ) {
    case TS_EVENT_IMMEDIATE:
      if ( (err=xform_input_completion(event)) ) {
        break;
      }

      // past first write and no bytes available?
      if ( TSVIONDoneGet(evio) && ! TSIOBufferReaderAvail(TSVIOReaderGet(evio)) ) {
        err = eErrXformExtraImmEvent; // no data and not done?
        break;
      }
      err = xform_input_event();
      break;

    // can delay the end!
    case TS_EVENT_ERROR:
    case TS_EVENT_VCONN_WRITE_COMPLETE:
      DEBUG_LOG("xform-event from xformInVIO e#%d",event);
      _xformCB(event, xformInputVIO(), 0);
      forward_vio_event(event, xformInputVIO(), *this); // DANGER: may be reentrantly destroyed!!
      err = eErrXformComplete;
      break;

    default:
      DEBUG_LOG("xform-unkn-event from xformInVIO e#%d",event);
      break;
  }
  // check again if anything changed..
  return ( err ? : xform_input_completion(event) );
}

int
ATSXformCont::handleXformBufferEvent(TSEvent event, TSVIO evio)
{
  // evio == _inVIO and a one-buffer write
  DEBUG_LOG("xform-event from buffer VIO e#%d",event);

  switch ( event ) {
    case TS_EVENT_VCONN_READ_READY:
    {
      // shouldn't fail both...
      auto fail = _outVConnU && _outVConnU->check_refill(event);
      ink_assert(!fail);
      break;
    }

    case TS_EVENT_VCONN_READ_COMPLETE:
      // only check if made yet...
      if ( _outVConnU ) {
        _outVConnU->check_refill(event); // if first is last
      }
      _xformCB(TS_EVENT_VCONN_READ_COMPLETE, _inVIO, 0); // no more fills ready...
      break;

    default:
      DEBUG_LOG("xform-unkn-event buffer VIO e#%d",event);
      break;
  }
  return eNoError;
}


int
ATSXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata)
{
  auto xfinvconn = get();

  if (!xfinvconn || !xformInputVIO()) {
    return 0;
  }

  // check for rude shutdown?
  if (cont != xfinvconn) {
    DEBUG_LOG("mismatch vconn event: e#%d", event);
    return 0;
  }

  ThreadTxnID txnid{_txnID};
  TSVIO evio = static_cast<TSVIO>(edata);

  if ( event == TS_EVENT_IMMEDIATE ) {
    DEBUG_LOG("xform-input-event: e#%d %p", event, evio);
    evio = xformInputVIO();
  } else {
    DEBUG_LOG("xform-event: e#%d %p", event, evio);
  }

  ink_assert(this->operator TSVConn() == cont);
  ink_assert(evio);

  auto err = 0;

  // output got this?
  if ( _outVConnU && evio == *_outVConnU ) {
    err = handleXformOutputEvent(event);
    // may have been destroyed!
  } else if ( evio == xformInputVIO() ) {
    err = handleXformInputEvent(event, evio);
    // may have been destroyed!
  } else if ( evio == _inVIO ) {
    handleXformBufferEvent(event,evio); // don't check xform
  } else {
    ink_assert( ! evio || xformInputVIO() != _inVIO );
    DEBUG_LOG("unkn-event from unkn e#%d [%p]",event,evio);
  }

  if ( err ) {
    auto errstr = xform_input_completion_desc(err);
    if ( errstr ) {
      DEBUG_LOG("%s: e#%d",errstr,event);
    }
  }
  
  return 0;
}

int
ATSXformCont::xform_input_event()
{
  const auto event = TS_EVENT_IMMEDIATE; // only way in ...
  auto xfinvio = xformInputVIO();

  // standard input is needed if not present
  if ( !_inVIO ) {
    _inVIO = xfinvio;
  }

  // only create at precise start..
  if ( ! _outVIOWaiting && ! _outVConnU ) {
    DEBUG_LOG("create xform write vio: len=%#lx skip=%#lx", _outWriteBytes, _outSkipBytes);
    _outVConnU = ATSXformOutVConn::create_if_ready(*this, _outWriteBytes, _outSkipBytes);
    _outVConnU->check_refill(event);
    auto wmark = TSIOBufferWaterMarkGet(outputBuffer());
    auto inbuff = TSVIOBufferGet(xformInputVIO());
    auto owmark = TSIOBufferWaterMarkGet(inbuff);
    DEBUG_LOG("input buffering set: %ld [old:%ld]",wmark,owmark);
    TSIOBufferWaterMarkSet(inbuff, wmark);
  }

  auto xfinrdr = TSVIOReaderGet(xfinvio);
  auto avail = TSIOBufferReaderAvail(xfinrdr);

  if ( _inVIO && xfinvio != _inVIO ) {
    TSIOBufferReaderConsume(xfinrdr, avail); // empty out now..
    DEBUG_LOG("consume skip input-buffer event: avail %ld",avail);
    return eNoError;
  }

  // event edata is _inVIO or from it ...

  auto inrdr = TSVIOReaderGet(xfinvio); // reset if changed!
  auto inpos   = TSVIONDoneGet(xfinvio);
  auto inavail = TSIOBufferReaderAvail(inrdr);

  auto oinpos   = inpos;
  auto oinavail = inavail;

  auto inready = std::min(inavail, TSVIONTodoGet(xfinvio));

  /////////////
  // ask for reduced amount to copy
  /////////////

  inready = _xformCB(event, xfinvio, inready); // send bytes-left in segment

  /////////////
  // copy to output...
  /////////////

  inready = TSIOBufferCopy(outputBuffer(), inrdr, inready, 0); // copy them
  TSIOBufferReaderConsume(inrdr, inready);                     // advance the input used

  TSVIONDoneSet(xfinvio, TSVIONDoneGet(xfinvio) + inready);      // advance toward end

  DEBUG_LOG("!!! copy-next-copied : @%#lx+%#lx +%#lx", inpos, inready, inavail);

  // reset new positions
  inrdr   = TSVIOReaderGet(xfinvio); // reset if changed!
  inpos   = TSVIONDoneGet(xfinvio);
  inavail = TSIOBufferReaderAvail(inrdr);
  DEBUG_LOG(" ^^^^^^^^^^^ cb-event: @%#lx buff=+%#lx rdy=+%#lx [%p]", inpos, inavail, inready, inrdr);

  // no change at all and no data left?
  if ( ! inavail && oinpos == inpos ) {
//    DEBUG_LOG("xform input waiting: @%#lx w/no change", inpos);
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, xfinvio, *this);
    return eErrXformExtraImmEvent;
  }

  // have output and data was consumed?
  if ( oinavail > inavail && _outVConnU ) {
    _outVConnU->check_refill(event);
    auto err = xform_input_completion(event);
    if (err) {
      return err; // RETURN
    }
  }

  // the input is waiting for something...
  if ( inavail ) {
    DEBUG_LOG("xform input buffer left: buff=%#lx", TSIOBufferReaderAvail(inrdr));
    _inVIOWaiting = event;
    return eNoError;
    // RETURN
  }

  // buffer was fully consumed...

  if ( _inVIOWaiting ) {
    DEBUG_LOG("xform input wait-flushed: @%#lx -> @%#lx", oinpos, inpos);
    _inVIOWaiting = event;
  }

  DEBUG_LOG("xform input pull: @%#lx -> @%#lx", oinpos, inpos);
  forward_vio_event(TS_EVENT_VCONN_WRITE_READY, xfinvio, *this);
  return eNoError;
}

bool
ATSXformOutVConn::check_refill(TSEvent event)
{
  auto outready = ( _outVIO ? TSIOBufferReaderAvail(TSVIOReaderGet(_outVIO)) : TSIOBufferReaderAvail(_outReader) );
  auto pos = ( _outVIO ? TSVIONDoneGet(_outVIO) : -_skipBytes ) ;

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
    DEBUG_LOG("xform no-write with early data: @%#lx w/avail %#lx", - (outready + pos), outready);
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
    _outVIOWaiting = event; // can't do it now...
    _outVIOWaitingTS = ink_microseconds(MICRO_REAL); // can't do it now...
    auto inbuff = TSVIOBufferGet(_inVIO);
    auto oinMark = TSIOBufferWaterMarkGet(inbuff);

    auto ooutMark = TSIOBufferWaterMarkGet(_outBuffer);
    auto noutMark = std::min(std::min(ooutMark,_writeBytes),1L<<24); // 16M
    noutMark = std::max(noutMark*2,1L<<16);

    TSIOBufferWaterMarkSet(_outBuffer, noutMark);
    TSIOBufferWaterMarkSet(inbuff, std::max(oinMark,noutMark) );

    DEBUG_LOG("xform empty: @%#lx in/out buffering %ldK -> %ldK", pos, ooutMark>>10, noutMark>>10);
    return true;
  }

//  if ( outready < std::min(left+0,(1L<<20)) && ! _outVIOWaiting ) {
  if ( ! _outVIOWaiting ) {
    DEBUG_LOG("xform no-wait no-end: @%#lx w/avail %#lx", pos, outready);
    // not waiting and not at end...
    return false;
  }

  auto delay = ( ink_microseconds(MICRO_REAL) - _outVIOWaitingTS ); // can't do it now...

  // less than 128K (or to end) ready and delay is only <10ms 
  if ( outready < std::min(left+0,(1L<<17)) && delay < 10000 ) {
    return false;
  }

  DEBUG_LOG("xform flush reenable: @%#lx w/avail %#lx delay %f", pos,outready, delay / 1000.0 );
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
  auto xfinvio = xformInputVIO();

  // if special input and output VIO is not started...
  if ( xfinvio != _inVIO && _outVConnU && ! _outVConnU->operator TSVIO() ) {
    reader = *_outVConnU; // skip within pre-write buffer
  } else if ( _inVIO && xfinvio == _inVIO ) {
    reader = TSVIOReaderGet(_inVIO); // within transform input buffer
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

  TSIOBufferReaderConsume(reader, ncopied);                // advance input

  if ( xfinvio == _inVIO ) {
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
    _writeHook(nullptr, 0, 0, 0);
    return left;
  }

  if (!left) {
    // something closed or ended...
    return 0; // TODO: possible abort!
  }

  if (evtvio == outputVIO()) {
    DEBUG_LOG("tee output event: e#%d +%#lx", event, left);
  } else if (evtvio != inputVIO()) {
    DEBUG_LOG("unkn vio event: e#%d +%#lx %p", event, left, evtvio);
  }

  auto range = teeAvail();
  auto oavail = range.second - range.first; // without left added in
  auto teemax = TSIOBufferWaterMarkGet(_teeBufferP.get()); // without bytes copied

  // allow copy if there's enough in buffer right now
  if ( left && oavail < teemax ) {
    left = TSIOBufferCopy(_teeBufferP.get(), TSVIOReaderGet(inputVIO()), left, 0);
    // NOTE: copied but not Consumed

    range.second += left;
    _lastInputNDone += left; // forward 
    auto navail = oavail + left;

    DEBUG_LOG("tee buffer bytes post-copy: @%#lx+%#lx [+%#lx]", range.first, navail, left);
  } else {
    // new bytes will hit watermark level
    DEBUG_LOG("tee buffer bytes blocked-copy: @%#lx+%#lx+%#lx", range.first, oavail, left);
    left = 0;
  }

  _writeHook(_teeReaderP.get(), range.first, range.second, left); // cannot un-consume in the write-hook
  return left; // always show advance from Tee
}

TSIOBufferReader
BlockTeeXform::cloneAndSkip(int64_t oskip)
{
  auto oavail = TSIOBufferReaderAvail(_teeReaderP.get());
  auto skip = std::min(oavail, oskip);

  if ( skip != oskip ) {
    DEBUG_LOG("tee buffer clone shrank: %#lx < %#lx", skip, oskip);
  }

  auto ordr = _teeReaderP.release(); // caller must free it
  auto obuff = _teeBufferP.release(); // caller must free it

  TSIOBufferReader_t ordr2( TSIOBufferReaderClone(ordr) ); // delete upon return

  TSIOBufferReaderConsume(ordr2.get(), skip); // advance input to next point

  _teeBufferP.reset(TSIOBufferCreate());
  _teeReaderP.reset(TSIOBufferReaderAlloc(_teeBufferP.get()));

  if ( TSIOBufferWaterMarkGet(obuff) > 0 ) {
    TSIOBufferWaterMarkSet(_teeBufferP.get(), TSIOBufferWaterMarkGet(obuff) );
    TSIOBufferWaterMarkSet(obuff, skip);
  }

  TSIOBufferCopy(_teeBufferP.get(), ordr2.get(), oavail - skip, 0); // save extra bytes as before
  return ordr;
}

