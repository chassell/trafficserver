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


class BlockSinkXform 
{
 public:
  BlockSinkXform(TSHttpTxn txn, TSVConn vconn)
     : _input_vio( TSVConnWriteVIOGet(vconn) ),
       _input_reader( TSVIOReaderGet(_input_vio) ),
       _xformHook(*this,&initHook,nullptr),
       _xformWrite(*this,&BlockSinkXform::writeEvent,txn)
  {
//    TSTS_HTTP_RESPONSE_CLIENT_HOOK
  }

  int64_t initHook(TSEvent event, 

  int64_t writeEvent(TSEvent event, TSVIO input, int64_t off, int64_t size);

  TSVIO            const _input_vio;
  TSIOBufferReader const _input_reader;
  APICont          const _xformHook;
  APICont          const _xformWrite;
};

int64_t(TSEvent,TSVIO,int64_t,int64_t)
  _userCB = decltype(_userCB)([func,cont](TSEvent event, void *) 
     {
       if ( TSVConnClosedGet(cont) ) {
         (obj.*funcp)(TS_EVENT_VCONN_EOS,nullptr,-1,-1); // bytes "written" and bytes actually consumed
         return;
       }

       auto input_vio = static_cast<TSVIO>( TSVConnWriteVIOGet(cont) );

       auto r = 0;
       auto ndone = TSVIONDoneGet(input_vio);

       if ( event != TS_EVENT_VCONN_READ_READY && event != TS_EVENT_IMMEDIATE ) 
       {
         (obj.*funcp)(event,input_vio,ndone,-1); // bytes "written" and bytes actually consumed
         return;
       }

       if ( ! TSVIOBufferGet(input_vio) ) {
         return;
       }

       auto inreader = TSIOBufferReader(TSVIOReaderGet(input_vio));

       // total amt left and amt available

       auto avail = TSIOBufferReaderAvail(inreader);
       avail = std::min(avail+0, TSVIONTodoGet(input_vio));

       r = (obj.*funcp)(event,input_vio,ndone,avail); // bytes "written" and bytes actually consumed
       if ( r <= 0 || r > avail ) {
         return;
       }

       TSIOBufferReaderConsume(inreader,r); // consume data
       TSVIONDoneSet(input_vio, ndone + r ); // inc offset

       auto evt = ( TSVIONTodoGet(input_vio) > 0 ? TS_EVENT_VCONN_WRITE_READY
                                                 : TS_EVENT_VCONN_WRITE_COMPLETE );
       TSContCall(TSVIOContGet(input_vio), evt, input_vio);
     });

int64_t writeEvent(TSEvent event, TSVIO input, int64_t off, int64_t size);


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

void
BlockSinkXform::throw_event(event)
{
  if ( _input_vio || ! TSVIOContGet(_input_vio) || ! TSVIOBufferGet(_input_vio)) {
    return;
  }

  if ( ! _input_complete && _input_complete++ ) {
    return;
  }
  TSContCall(TSVIOContGet(_input_vio), static_cast<TSEvent>(), _input_vio);
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
      upstream_event(TS_EVENT_ERROR);
      break;
    default:
      if (TSVIONBytesGet(_input_vio) <= TSVIONDoneGet(_input_vio)) {
        upstream_event(TS_EVENT_VCONN_WRITE_COMPLETE);
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
        upstream_event(TS_EVENT_VCONN_WRITE_COMPLETE);
        break; // now done with reads
      }

      TSVIOReenable(_input_vio);

      upstream_event(TS_EVENT_VCONN_WRITE_READY);
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

