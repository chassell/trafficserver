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
     BlockTeeXform(ctxt.txn(), [this](TSIOBufferReader r,int64_t pos,int64_t len) { return this->handleInput(r,pos,len); }, 
                               ctxt._beginByte, ctxt.rangeLen()),
     _ctxt(ctxt), 
     _vcsToWrite(ctxt.rangeLen()/ctxt.blockSize() + 1),
     _writeEvents(*this, &BlockStoreXform::handleWrite, nullptr)
{
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

int64_t BlockStoreXform::next_valid_vconn(TSVConn &vconn, int64_t pos, int64_t len)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  using namespace std::chrono;
  using std::future_status;

  vconn = nullptr;

  // find distance to next start point
  int64_t dist = ( ! pos ? 0 : blksz-1 - ((pos-1) % blksz) );

  // no boundary within distance?
  if ( dist >= len ) {
    DEBUG_LOG("invalid block pos:%ld len=%ld",pos,len);
    return -1;
  }

  // dist with length includes a block boundary

  // index of base block in set
  auto baseblk = _ctxt._beginByte / blksz;
  // index of block in server download
  auto blk = (pos + blksz-1) / blksz;

  // skip available blocks
  for ( ; is_base64_bit_set(_ctxt.b64BlkBitset(),blk + baseblk) ; ++blk ) {
    dist += blksz; // jump forward a full block
  }

  // can't find a ready write waiting?
  if ( ! _vcsToWrite[blk].valid() ) {
    DEBUG_LOG("invalid VConn future dist:%ld pos:%ld len=%ld",dist,pos,len);
    return -1;
  }

  // can't find a ready write waiting?
  if ( _vcsToWrite[blk].wait_for(seconds::zero()) != future_status::ready ) {
    DEBUG_LOG("unset VConn future dist:%ld pos:%ld len=%ld",dist,pos,len);
    return -1;
  }

  // found one ready to go...
  vconn = _vcsToWrite[blk].get();
  DEBUG_LOG("ready VConn future dist:%ld pos:%ld len=%ld",dist,pos,len);
  return dist;
}

//
// called from writeHook
//      -- after outputBuffer() was filled
int64_t BlockStoreXform::handleInput(TSIOBufferReader outrdr, int64_t pos, int64_t len)
{
  // position is distance within body *without* len...

  TSVConn currBlock = nullptr;
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());

  // amount *includes* len
  auto bready = TSIOBufferReaderAvail(outrdr) - len;
  auto bpos = pos - bready;

  // check unneeded bytes before next block
  auto nextDist = next_valid_vconn(currBlock, bpos, bready + len);

  // don't have enough to reach full block boundary?
  if ( nextDist < 0 ) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(outrdr,bready + len);
    DEBUG_LOG("store **** skip current block pos:%ld%+ld%+ld",bpos,bready,len);
    return len;
  }

  // no need to save these bytes?
  TSIOBufferReaderConsume(outrdr,nextDist);
  bready -= nextDist;
  bpos += nextDist;

  if ( nextDist ) {
    DEBUG_LOG("store **** skipping to buffered pos:%ld%+ld%+ld",bpos,bready,len);
  }

  // pos of buffer-empty is precisely on a block boundary

  if ( bready + len < blksz ) {
    DEBUG_LOG("store ++++ buffering more dist pos:%ld%+ld%+ld -> %+ld",bpos,bready,len,std::min(len+0,blksz-bready));
    return std::min(len+0,blksz-bready); // limit amt left by distance to a filled block
    //////// RETURN
  }

  atscppapi::ScopedContinuationLock lock(_writeEvents);

  // should send a WRITE_COMPLETE rather quickly
  TSVConnWrite(currBlock, _writeEvents, outrdr, blksz);

  DEBUG_LOG("store ++++ performed write pos:%ld%+ld%+ld",bpos,bready,len);
  return len;
}



void BlockStoreXform::handleWrite(TSEvent evt, void*edata, std::nullptr_t)
{
  DEBUG_LOG("write event evt:%d",evt);
  TSVIO writeVIO = static_cast<TSVIO>(edata);

  atscppapi::ScopedContinuationLock lock(_writeEvents);

  if ( ! writeVIO ) {
    return;
  }

  switch (evt) {
    case TS_EVENT_VCONN_WRITE_COMPLETE:
       TSVConnClose(TSVIOVConnGet(writeVIO));
       break;
    case TS_EVENT_VCONN_WRITE_READY:
       // should be started already
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
