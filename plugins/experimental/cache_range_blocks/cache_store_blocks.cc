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
#include "atscppapi/Mutex.h"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt)
   : TransactionPlugin(ctxt.txn()), 
     BlockTeeXform(ctxt.atsTxn(), [this](TSIOBufferReader r,int64_t pos,int64_t len) { return this->handleInput(r,pos,len); }, 
                   ctxt.rangeLen()),
     _ctxt(ctxt), 
     _vcsToWrite(ctxt.rangeLen()/ctxt.blockSize() + 1),
     _writeReader( TSIOBufferReaderAlloc(this->outputBuffer()) ), // second reader for output buffer
     _writeEvents(*this, &BlockStoreXform::handleWrite, nullptr)
{
  ctxt._keysInRange.resize( _vcsToWrite.size() );

  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  DEBUG_LOG("xform-store: xhook:%p _ctxt:%p len=%lu",this,&_ctxt,_ctxt.assetLen());

  // [will override the server response for headers]
  //
  auto &keys = _ctxt._keysInRange;
  auto mutex = TSMutexCreate();

  auto pos = _ctxt._beginByte  - ( _ctxt._beginByte % _ctxt.blockSize() );
  auto blkNum = pos / _ctxt.blockSize();

  for( auto i = 0U ; i < keys.size() ; ++i,++blkNum ) {
	if ( ! is_base64_bit_set(_ctxt.b64BlkBitset(),blkNum) ) 
    {
      keys[i] = std::move(APICacheKey(_ctxt.clientUrl(),blkNum * _ctxt.blockSize()));
      auto contp = APICont::create_temp_tscont(mutex,_vcsToWrite[i]);
      TSCacheWrite(contp,keys[i]); // find room to store each key...
    }
  }

  DEBUG_LOG("xform-store: len=%lu",_ctxt._assetLen);
  txn.resume(); // wait for response
}

void
BlockStoreXform::handleSendRequestHeaders(Transaction &txn) {
  DEBUG_LOG("srvr-req: len=%lu",_ctxt.assetLen());
  _ctxt.clean_server_request(txn); // request full blocks if possible
  txn.resume();
}

void
BlockStoreXform::handleReadResponseHeaders(Transaction &txn)
{
  DEBUG_LOG("srvr-resp: len=%lu",_ctxt.assetLen());
   _ctxt.clean_server_response(txn);
   txn.resume();
}

int64_t BlockStoreXform::next_valid_vconn(TSVConn &vconn, int64_t pos, int64_t len)
{
  using namespace std::chrono;
  using std::future_status;

  vconn = nullptr;

  // zero if match to boundary
  int64_t dist = _ctxt.blockSize()-1 - ((pos + _ctxt.blockSize()-1) % _ctxt.blockSize());

  // within this range to start a new one?
  if ( dist >= len ) {
    DEBUG_LOG("invalid block pos:%ld len=%ld",pos,len);
    return -1;
  }

  // dist is less than a block

  auto i = (pos + dist) / _ctxt.blockSize();

  if ( ! _vcsToWrite[i].valid() ) {
    DEBUG_LOG("invalid VConn future pos:%ld len=%ld",pos,len);
    return -1;
  }
  if ( _vcsToWrite[i].wait_for(seconds::zero()) != future_status::ready ) {
    DEBUG_LOG("unset VConn future pos:%ld len=%ld",pos,len);
    return -1;
  }
  vconn = _vcsToWrite[i].get();
  DEBUG_LOG("ready VConn future pos:%ld len=%ld",pos,len);
  return dist;
}

//
// called from writeHook
//      -- after outputBuffer() was filled
int64_t BlockStoreXform::handleInput(TSIOBufferReader inrdr, int64_t pos, int64_t len)
{
  _xformReader = inrdr; // every time

  TSVConn nxtWrite = nullptr;
  auto dist = next_valid_vconn(nxtWrite,pos,len);

  if ( ! _currWrite && ! nxtWrite ) {
    DEBUG_LOG("xform input ignored pos:%ld len=%ld",pos,len);
    // no data needed
    TSIOBufferReaderConsume(_writeReader.get(), len); // skip reader past
    return len;
  }

  atscppapi::ScopedContinuationLock lock(_writeEvents);

  // data will be written to one...

  auto lenCurr = ( _currWrite ? std::min(len+0,TSVIONTodoGet(_currWrite)) : dist );
  auto lenNext = len - lenCurr;

  TSIOBufferCopy(outputBuffer(), inrdr, lenCurr, 0);

  if ( _currWrite ) {
    DEBUG_LOG("xform copy-reenable pos:%ld len=%ld (lenCurr=%ld)",pos,len,lenCurr);
    TSVIOReenable(_currWrite); // flush change down
  }

  if ( _currWrite && lenCurr && lenNext ) {
    DEBUG_LOG("xform close-reenable pos:%ld len=%ld (lenNext=%ld)",pos,len,lenNext);
    TSVIOReenable(_currWrite); // flush end-of-block down
  }

  TSIOBufferCopy(outputBuffer(), inrdr, lenNext, 0);

  if ( nxtWrite ) {
    DEBUG_LOG("xform new write pos:%ld len=%ld (lenNext=%ld)",pos,len,lenNext);
    _currWrite = TSVConnWrite(nxtWrite, _writeEvents, _writeReader.get(), _ctxt.blockSize());
  }

  DEBUG_LOG("xform copied write pos:%ld len=%ld (lenNext=%ld)",pos,len,lenNext);
  return len;
}

void BlockStoreXform::handleWrite(TSEvent evt, void*edata, std::nullptr_t)
{
  DEBUG_LOG("write event evt:%d",evt);
  TSVIO writeVIO = static_cast<TSVIO>(edata);
  atscppapi::ScopedContinuationLock lock(_writeEvents);

  switch (evt) {
    case TS_EVENT_VCONN_WRITE_READY:
       break;
    case TS_EVENT_VCONN_WRITE_COMPLETE:
    {
       if ( writeVIO ) {
         TSVConnClose(TSVIOVConnGet(writeVIO));
       }
       break;
    }
    default:
       break;
  }
}

BlockStoreXform::~BlockStoreXform() 
{
  for( auto &&i : _vcsToWrite ) {
    TSVConnClose(i.get());
  }
}
