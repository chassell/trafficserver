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

int64_t
APIXformCont::copy_next_len(int64_t left)
{
  auto invio = TSVConnWriteVIOGet(*this);
  if ( ! invio || ! TSVIOBufferGet(invio) ) {
    return -1;
  }

  // copy op defined by:
  //    0) input reader: prev. skipped bytes (consumed)
  //
  //    1) input reader: added new bytes
  //    2) VIO:          op-limit bytes
  //    3) endpos:       copy-limit bytes

  auto nbytesin = TSIOBufferReaderAvail(_inReader);
  auto viopos = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - viopos);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    return 0L;
  }

  TSIOBufferCopy(outputBuffer(), _inReader, ncopied, 0); // copy them
  TSIOBufferReaderConsume(_inReader,ncopied); // advance input
  TSVIONDoneSet(_inVIO, viopos + ncopied);  // advance toward end

  TSVIOReenable( TSVConnWriteVIOGet(_outVConn) );
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

  auto nbytesin = TSIOBufferReaderAvail(_inReader);
  auto viopos = TSVIONDoneGet(_inVIO);
  auto violimit = std::min(left, TSVIONBytesGet(_inVIO) - viopos);
  auto ncopied = std::min(nbytesin+0, violimit);

  if ( ! ncopied ) {
    return 0L;
  }

  // TSIOBufferCopy(outputBuffer(), _inReader, ncopied, 0); // copy them
  TSIOBufferReaderConsume(_inReader,ncopied); // advance input
  TSVIONDoneSet(_inVIO, viopos + ncopied);  // advance toward end
  return ncopied;
}


int APIXformCont::handleXformTSEvent(TSCont cont, TSEvent event, void *edata) 
{
  ink_assert(this->operator TSVConn() == cont);

  // rude shutdown
  if ( TSVConnWriteVIOGet(*this) != _inVIO || ! TSVIOBufferGet(_inVIO) ) {
    _xformCB(event, nullptr, 0);
    return 0;
  }

  // end-of-write "ack" to upstream
  if ( ! TSVIONTodoGet(_inVIO) ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,_inVIO); // complete upstream
    return 0;
  }

  auto pos = TSVIONDoneGet(_inVIO);
  auto inavail = TSIOBufferReaderAvail(_inReader);

  if ( pos >= _xformCBAbsLimit && pos < _nextXformCBAbsLimit )
  {
    _xformCB = _nextXformCB;
    _xformCBAbsLimit = _nextXformCBAbsLimit;
  }

  auto buffavail = std::min(inavail,_xformCBAbsLimit - pos);

  if ( event == TS_EVENT_IMMEDIATE ) {
    event = TS_EVENT_VCONN_WRITE_READY;
  } 
  
  if ( event == TS_EVENT_VCONN_WRITE_READY && edata == _inVIO ) {
    _xformCB(event, _inVIO, buffavail);
  }

  auto inleft = TSIOBufferReaderAvail(_inReader);

  if ( inavail > inleft ) {
    TSVIOReenable(TSVConnWriteVIOGet(*this));
  }

  if ( inavail && ! inleft ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_READY, _inVIO);
  }

  return 0;
}

int64_t BlockTeeXform::handleEvent(TSEvent event, TSVIO evtvio, int64_t left)
{
  if ( ! evtvio ) {
    DEBUG_LOG("xform output complete");
    _writeHook(nullptr,0,0);
    return left;
  }

  TSIOBufferCopy(_teeBufferP.get(), _inputReaderP.get(), left, 0);

  if ( ! left ) {
    return left;
  }

  // copy in current position-and-setup
  auto pos = TSVIONDoneGet(evtvio);
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

