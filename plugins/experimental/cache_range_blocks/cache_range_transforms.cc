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

void BlockTeeXform::handleEvent(TSEvent event, TSVIO evtvio)
{
   TSVConn invconn = *this;

   ink_assert( TSVIOVConnGet(evtvio) == invconn );

   if ( TSVConnClosedGet(invconn) ) {
     // one chance only from upstream
     return;
   }

   auto invio = TSVConnWriteVIOGet(invconn); // cannot be null

   switch (event) 
   {
     case TS_EVENT_ERROR:
       forward_vio_event(TS_EVENT_ERROR,invio);
       break;

     // sent by output only...
     case TS_EVENT_VCONN_WRITE_COMPLETE:
       TSVConnShutdown(this->output(), 0, 1); // no more writes to downstream
       break;

     case TS_EVENT_VCONN_WRITE_READY:
     default:
       handleWrite(invio);
       break;
   }
}

void BlockTeeXform::handleWrite(TSVIO invio)
{
  // only if safe...
  if ( ! TSVIOBufferGet(invio) ) {
    return;
  }

  auto outvio = TSVConnWriteVIOGet(output());
  if ( outvio && ! TSVIOBufferGet(outvio) ) {
    outvio = nullptr;
  }

  // limit at end of input
  TSIOBufferReader inreader = TSVIOReaderGet(invio);
  auto inavail = TSIOBufferReaderAvail(inreader);
  inavail = std::min(inavail+0, TSVIONTodoGet(invio));

  auto outavail = inavail;

  if ( outvio ) {
    // adjust to what's acceptable
    outavail = std::min(outavail+0, TSVIONTodoGet(outvio));
  } 

  // disable from out write?
  if ( inavail && ! outavail ) {
    TSVIOReenable(outvio); // send zero-bytes on
    return;
  }

  auto ndone = TSVIONDoneGet(invio);
  // disable from input write only?
  if ( ! inavail && outvio ) {
    TSVIONBytesSet(outvio, ndone);
    TSVIOReenable(outvio); // intended to give zero bytes
  }

  if ( ! inavail ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,invio);
    return;
  }

  _writeHook(inreader,ndone,outavail);

  if ( outavail && outvio ) {
    // copy all bytes available in
    TSIOBufferCopy(TSVIOBufferGet(outvio), inreader, outavail, 0);
  }

  // keep it moving on ...
  TSIOBufferReaderConsume(inreader,outavail);
  TSVIONDoneSet(invio, ndone + outavail);
  forward_vio_event(TS_EVENT_VCONN_WRITE_READY, invio);
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

