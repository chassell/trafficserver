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
forward_vio_event(TSEvent event, TSVIO input_vio)
{
  if ( input_vio && TSVIOContGet(input_vio) && TSVIOBufferGet(input_vio)) {
    TSContCall(TSVIOContGet(input_vio), event, input_vio);
  }
}


class BlockTeeXform 
{
 public:
  BlockTeeXform(TSHttpTxn txn, TSVConn teeVconn, int64_t xformLen)
     : _sharedOutput(TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K) ), 
       _teeReader( TSIOBufferReaderAlloc(this->_sharedOutput.get())),
       _xformVConn(txn,TS_HTTP_RESPONSE_TRANSFORM_HOOK,this->_sharedOutput.get(), xformLen)
  {
     _teeOutputVIO = TSVConnWrite(teeVconn, _xformVConn, _teeReader.get(), INT64_MAX);

     // get to method via callback
     _xformVConn = [this](TSEvent evt, TSVConn vconn) { this->handleEvent(evt,vconn); };
  }

  void 
  handleEvent(TSEvent event, TSVConn invconn);

  void
  handleWrite(TSVIO invio, TSVIO outvio);

  TSIOBuffer_t             _sharedOutput;
  TSIOBufferReader_t       _teeReader;
  TSVIO                    _teeOutputVIO;
  APIXformCont             _xformVConn;
};

void BlockTeeXform::handleEvent(TSEvent event, TSVConn invconn)
{
   auto outvconn = TSTransformOutputVConnGet(invconn);

   if ( TSVConnClosedGet(invconn) || ! outvconn ) {
     return;
   }

   auto input_vio = TSVConnWriteVIOGet(invconn); // cannot be null

   switch (event) 
   {
     case TS_EVENT_VCONN_WRITE_COMPLETE:
       TSVConnShutdown(outvconn, 0, 1);
       break;                                                 //// RETURN
     case TS_EVENT_ERROR:
       forward_vio_event(TS_EVENT_ERROR,input_vio);
       break;                                                 //// RETURN
     case TS_EVENT_VCONN_WRITE_READY:
       handleWrite(input_vio, TSVConnWriteVIOGet(outvconn));
       break; // okay .. so continue on..
     default:
       break;                                                 //// RETURN
   }
}

void BlockTeeXform::handleWrite(TSVIO input_vio, TSVIO output_vio)
{
  // only if safe...
  if ( ! TSVIOBufferGet(input_vio) ) {
    return;
  }

  TSIOBufferReader inreader = TSVIOReaderGet(input_vio);
  int64_t inavail = TSIOBufferReaderAvail(inreader);

  // limit at end of input
  inavail = std::min(inavail+0, TSVIONTodoGet(input_vio));

  auto teeavail = TSIOBufferReaderAvail(_teeReader.get()); // check Tee
  auto outavail = TSIOBufferReaderAvail(TSVIOReaderGet(output_vio)); // check Out
  
  // update from downstream values
  teeavail = std::min(inavail+0, TSVIONTodoGet(_teeOutputVIO));
  outavail = std::min(inavail+0, TSVIONTodoGet(output_vio));

  // upstream is complete?
  if ( ! teeavail && ! outavail ) {
    forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,input_vio);
    TSVIOReenable(_teeOutputVIO); // intended to give zero bytes
    TSVIOReenable(output_vio); // intended to give zero bytes
    return;
  }

  // copy all available in
  TSIOBufferCopy(_sharedOutput.get(), inreader, inavail, 0);
  TSVIONDoneSet(input_vio, TSVIONDoneGet(input_vio) + inavail);
  TSIOBufferReaderConsume(inreader,inavail);

  if ( teeavail ) {
    TSVIOReenable(_teeOutputVIO);
  }

  if ( outavail ) {
    TSVIOReenable(output_vio);
  }

  // some data absorbed?
  // XXX: flow control if downstream is limited!
  forward_vio_event(TS_EVENT_VCONN_WRITE_READY, input_vio);
}

/*
void
firstTransformationPluginRead(vconn)
{
  _input_vio = TSVConnWriteVIOGet(vconn);
  _input_reader = TSVIOReaderGet(_input_vio);

  // after first write (ideally)
  _output_vconn = TSTransformOutputVConnGet(vconn);
  _output_buffer = TSIOBufferSizedCreate(TS_IOBUFFER_SIZE_INDEX_32K);
  _output_reader = TSIOBufferReaderAlloc(_output_buffer);
  _output_vio = TSVConnWrite(output_vconn, vconn, _output_reader, INT64_MAX); // all da bytes

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
      throw_vio_event(TS_EVENT_ERROR,input_vio);
      break;
    default:
      if (TSVIONBytesGet(_input_vio) <= TSVIONDoneGet(_input_vio)) {
        throw_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE,input_vio);
        break; // now done with reads
      }

      // cannot read anything?
      avail = TSIOBufferReaderAvail(_input_reader);
      if ( ! avail ) {
        break;
      }

      //  to_read = TSIOBufferCopy(_buffer, _input_reader, std::min(avail,TSVIONTodoGet(_input_vio)), 0);
      //  readChars(_buffer); // read what's in buffer now...
      copied = TSIOBufferCopy(_output_buffer, _input_reader, std::min(avail,TSVIONTodoGet(_input_vio)), 0);
      TSIOBufferReaderConsume(_input_reader, copied); // not to re-read
      TSVIONDoneSet(_input_vio, TSVIONDoneGet(_input_vio) + copied); // decrement todo

      if ( TSVIONTodoGet(_input_vio) <= 0 ) {
        throw_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE);
        break; // now done with reads
      }

      TSVIOReenable(_input_vio);
      throw_vio_event(TS_EVENT_VCONN_WRITE_READY,input_vio);
      break;
  }
}

size_t
close_output()
{
  ink_assert(_output_vio);

  // one sign of a close exists?  not a race?
  if ( TSVConnClosedGet(_vconn) || TSVConnClosedGet(_vconn) ) {
    // LOG_ERROR("TransformationPlugin=%p tshttptxn=%p unable to reenable output_vio=%p connection was closed=%d.", this,
    return _bytes_written; // nothing to do
  }
 

  // reset where we are...
  TSVIONBytesSet(_output_vio, _bytes_written);
  TSVIOReenable(_output_vio); // Wake up the downstream vio
  return _bytes_written;
}

shutdown_empty_downstream()
{
  // make sure a write occurs to signal the end
  TSVIONDoneSet(_output_vio, 0);
  TSVIOReenable(_output_vio); // Wake up the downstream vio
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
    TSVIOReenable(_output_vio); // Wake up the downstream vio
  }
}
*/

