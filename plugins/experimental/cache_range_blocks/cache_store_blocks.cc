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

#include <atscppapi/HttpStatus.h>
#include <atscppapi/Mutex.h>

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt)
   : TransactionPlugin(ctxt.txn()), 
     BlockTeeXform(ctxt.txn(), [this](TSIOBufferReader r,int64_t inpos,int64_t len) { return this->handleInput(r,inpos,len); }, 
                               ctxt._beginByte % ctxt.blockSize(), 
                               ctxt.rangeLen()),
     _ctxt(ctxt), 
     _vcsToWrite(ctxt.rangeLen()/ctxt.blockSize() + 1),
     _writeEvents(*this, &BlockStoreXform::handleWrite, nullptr)
{
  // definitely need a remote write
  TSHttpTxnCacheLookupStatusSet(ctxt.atsTxn(), TS_CACHE_LOOKUP_MISS);

  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(),0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(),0);

  ctxt._keysInRange.resize( _vcsToWrite.size() );

  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  DEBUG_LOG("store: xhook:%p _ctxt:%p len=%lu",this,&_ctxt,_ctxt.assetLen());

  // [will override the server response for headers]
  //
  auto &keys = _ctxt._keysInRange;
  auto mutex = TSMutexCreate();
  auto blkNum = _ctxt._beginByte / _ctxt.blockSize();

  for( auto i = 0U ; i < keys.size() ; ++i,++blkNum ) {
	if ( ! is_base64_bit_set(_ctxt.b64BlkBitset(),blkNum) ) 
    {
      keys[i] = std::move(APICacheKey(_ctxt.clientUrl(), _ctxt._etagStr, blkNum*_ctxt.blockSize()));
      auto contp = APICont::create_temp_tscont(mutex,_vcsToWrite[i]);
      TSCacheWrite(contp,keys[i]); // find room to store each key...
    }
  }

  DEBUG_LOG("store: len=%lu",_ctxt._assetLen);
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

int64_t BlockStoreXform::next_valid_vconn(TSVConn &vconn, int64_t inpos, int64_t len)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  using namespace std::chrono;
  using std::future_status;

  vconn = nullptr;

  // find distance to next start point (w/zero possible)
  int64_t dist = ( ! inpos ? 0 : blksz-1 - ((inpos-1) % blksz) );

  // no boundary within distance?
  if ( dist >= len ) {
    DEBUG_LOG("invalid data-block pos:%ld len=%ld",inpos,len);
    return -1;
  }

  // dist with length includes a block boundary

  auto absbase = _ctxt._beginByte / blksz; //
  // index of block in server download
  auto blk = inpos / blksz;

  // skip available blocks
  for ( ; is_base64_bit_set(_ctxt.b64BlkBitset(),absbase + blk) ; ++blk ) {
    dist += blksz; // jump forward a full block
  }

  // can't find a ready write waiting?
  if ( ! _vcsToWrite[blk].valid() ) {
    DEBUG_LOG("invalid VConn future dist:%ld pos:%ld len=%ld",dist,inpos,len);
    return -1;
  }

  // can't find a ready write waiting?
  if ( _vcsToWrite[blk].wait_for(seconds::zero()) != future_status::ready ) {
    DEBUG_LOG("unset VConn future dist:%ld pos:%ld len=%ld",dist,inpos,len);
    return -1;
  }

  // found one ready to go...
  vconn = _vcsToWrite[blk].get();
  DEBUG_LOG("ready VConn future dist:%ld pos:%ld len=%ld",dist,inpos,len);
  return dist;
}

//
// called from writeHook
//      -- after outputBuffer() was filled
int64_t BlockStoreXform::handleInput(TSIOBufferReader outrdr, int64_t inpos, int64_t len)
{
  // input has closed down?
  if ( ! outrdr ) {
    DEBUG_LOG("store **** final call");
    return 0;
  }
  // position is distance within body *without* len...

  TSVConn currBlock = nullptr;
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());

  // amount *includes* len
  auto oavail = TSIOBufferReaderAvail(outrdr) - len;
  auto odone = inpos - oavail;

  // check unneeded bytes before next block
  auto nextDist = next_valid_vconn(currBlock, odone, oavail + len);

  // don't have enough to reach full block boundary?
  if ( nextDist < 0 ) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(outrdr,oavail + len);
    DEBUG_LOG("store **** skip current block pos:%ld%+ld%+ld",odone,oavail,len);
    return len;
  }

  // no need to save these bytes?
  TSIOBufferReaderConsume(outrdr,nextDist);
  oavail -= nextDist;
  odone += nextDist;

  if ( nextDist ) {
    DEBUG_LOG("store **** skipping to buffered pos:%ld%+ld%+ld",odone,oavail,len);
  }

  // pos of buffer-empty is precisely on a block boundary

  if ( oavail + len < blksz ) {
    DEBUG_LOG("store ++++ buffering more dist pos:%ld%+ld%+ld -> %+ld",odone,oavail,len,std::min(len+0,blksz-oavail));
    return std::min(len+0,blksz-oavail); // limit amt left by distance to a filled block
    //////// RETURN
  }

  // should send a WRITE_COMPLETE rather quickly
  TSVConnWrite(currBlock, _writeEvents, outrdr, blksz);

  DEBUG_LOG("store ++++ performed write pos:%ld%+ld%+ld",odone,oavail,len);
  return len;
}

void BlockStoreXform::handleWrite(TSEvent event, void*edata, std::nullptr_t)
{
  TSVIO writeVIO = static_cast<TSVIO>(edata);

  if ( ! writeVIO ) {
    DEBUG_LOG("empty edata event:%d",event);
    return;
  }

  if ( event != TS_EVENT_VCONN_WRITE_COMPLETE ) {
    DEBUG_LOG("cache-write event:%d",event);
    return;
  }

  DEBUG_LOG("write complete event:%d",event);
  TSVConnClose(TSVIOVConnGet(writeVIO));

  TSVIOReenable(inputVIO()); // ??
}

BlockStoreXform::~BlockStoreXform() 
{
}
