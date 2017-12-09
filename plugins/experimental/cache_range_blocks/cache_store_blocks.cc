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

#include "ts/InkErrno.h"
#include <atscppapi/HttpStatus.h>
#include <atscppapi/Mutex.h>

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)


void
BlockSetAccess::start_cache_miss(int64_t firstBlk, int64_t endBlk)
{
  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_MISS);

  TSHttpTxnUntransformedRespCache(atsTxn(), 0);
  TSHttpTxnTransformedRespCache(atsTxn(), 0);

  DEBUG_LOG("cache-resp-wr: len=%#lx set=%s", assetLen(), b64BlkBitset().c_str());

  _keysInRange.clear();
  _keysInRange.resize(endBlk - firstBlk);

  // react to genuine misses above...
  for (auto i = 0U; i < endBlk - firstBlk; ++i) {
    if (!is_base64_bit_set(_b64BlkBitset, firstBlk + i)) {
      _keysInRange[i]    = std::move(APICacheKey(clientUrl(), _etagStr, (firstBlk + i) * _blkSize));
    }
  }

  _storeXform = std::make_unique<BlockStoreXform>(*this,endBlk - firstBlk);
  _storeXform->handleReadCacheLookupComplete(txn()); // may have txn.resume()
}


BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt, int blockCount)
  : TransactionPlugin(ctxt.txn()),
    BlockTeeXform(ctxt.txn(), 
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len) { return this->handleBodyRead(r, inpos, len); },
                  ctxt._beginByte % ctxt.blockSize(), 
                  ctxt.rangeLen() + ctxt.blockSize() - ctxt._endByte % ctxt.blockSize() ), // padded length...
    _ctxt(ctxt),
    _vcsToWrite(blockCount),
    _writeEvents(*this, &BlockStoreXform::handleBlockWrite, nullptr, TSContMutexGet(*this))
{
  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);  // add block-range and clean up
  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  DEBUG_LOG("store: xhook:%p _ctxt:%p len=%#lx", this, &_ctxt, _ctxt.assetLen());

  // [will override the server response for headers]
  //
  auto &keys  = _ctxt._keysInRange;
  auto mutex  = TSContMutexGet(*this);
  auto blkNum = _ctxt._beginByte / _ctxt.blockSize();

  for (auto i = 0U; i < keys.size(); ++i, ++blkNum) {
    if (keys[i]) {
      auto contp = APICont::create_temp_tscont(mutex, _vcsToWrite[i]);
      TSCacheWrite(contp, keys[i]); // find room to store each key...
    }
  }

  DEBUG_LOG("store: len=%#lx", _ctxt._assetLen);
  txn.resume(); // wait for response
}


int64_t
BlockStoreXform::next_valid_vconn(TSVConn &vconn, int64_t inpos, int64_t len)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  using namespace std::chrono;
  using std::future_status;

  vconn = nullptr;

  // find distance to next start point (w/zero possible)
  int64_t dist = (!inpos ? 0 : blksz - 1 - ((inpos - 1) % blksz));

  // dist with length includes a block boundary

  auto absbase = _ctxt._beginByte / blksz;             // fall back to prev.
  auto absend  = (_ctxt._endByte + blksz - 1) / blksz; // forward to next block
  // index of block in server download
  auto blk     = (inpos + dist) / blksz;
  int64_t blks = _vcsToWrite.size();

  // skip available blocks
  for (; absbase + blk < absend && blk < blks && is_base64_bit_set(_ctxt.b64BlkBitset(), absbase + blk); ++blk) {
    dist += blksz; // jump forward a full block
  }

  if (blk >= blks) {
    DEBUG_LOG("beyond final block final block start dist:- blk%#lx,%#lx (tot %ld/%ld)", absbase + blk, absend, absend - absbase,
              blks);
    return -1;
  }

  if (absbase + blk == absend) {
    DEBUG_LOG("at final block start dist:- blk%#lx,%#lx (tot %ld/%ld)", absbase + blk, absend, absend - absbase, blks);
    dist = 0;
  }

  // no boundary within distance?
  if (dist >= len) {
    DEBUG_LOG("skip required pos:%#lx -> %#lx len=%#lx", inpos, inpos + dist, len);
    return -1;
  }

  // can't find a ready write waiting?
  if (!_vcsToWrite[blk].valid()) {
    DEBUG_LOG("invalid VConn future dist:%#lx pos:%#lx len=%#lx", dist, inpos, len);
    return -1;
  }

  // can't find a ready write waiting?
  if (_vcsToWrite[blk].wait_for(seconds::zero()) != future_status::ready) {
    DEBUG_LOG("failed VConn future dist:%#lx pos:%#lx len=%#lx", dist, inpos, len);
    return -1;
  }

  // found one ready to go...
  vconn = _vcsToWrite[blk].get();
  DEBUG_LOG("ready VConn future dist:%#lx pos:%#lx len=%#lx", dist, inpos, len);
  return dist;
}

//
// called from writeHook
//      -- after outputBuffer() was filled
int64_t
BlockStoreXform::handleBodyRead(TSIOBufferReader teerdr, int64_t inpos, int64_t len)
{
  // input has closed down?
  if (!teerdr) {
    DEBUG_LOG("store **** final call");
    return 0;
  }

  // prevent multiple starts of a storage
  atscppapi::ScopedContinuationLock lock(*this);

  // position is distance within body *without* len...

  TSVConn currBlock = nullptr;
  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());

  // amount "ready" for block is read from our reader
  auto navail = TSIOBufferReaderAvail(teerdr); // new amount

  auto oavail = navail - len; // amount that hook had accepted
  auto buffpos  = inpos - oavail; // last consumed/written pos ...
  auto endpos = inpos + TSVIONTodoGet(inputVIO()); // final byte pos

  // check any unneeded bytes before next block
  auto skipDist = next_valid_vconn(currBlock, buffpos, navail);

  // don't have enough to reach full block boundary?
  if (skipDist < 0) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(teerdr, oavail + len);
    DEBUG_LOG("store **** skip current block pos:%#lx+%#lx+%#lx", buffpos, oavail, len);
    return len;
    //////// RETURN
  }

  if (skipDist) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(teerdr, skipDist);
    oavail -= skipDist;
    navail -= skipDist;
    buffpos += skipDist;

    DEBUG_LOG("store **** skipping to buffered pos:%#lx+%#lx+%#lx --> skip %#lx", buffpos, oavail, len, skipDist);
  }

  auto wrlen = std::min(endpos - buffpos,blksz); // len needed for a block write...

  if (navail < wrlen ) {
    DEBUG_LOG("store ++++ buffering more dist pos:%#lx+%#lx+%#lx -> +%#lx", buffpos, oavail, len, buffpos + navail);
    return len; // limit amt left by distance to a filled block
    //////// RETURN
  }

  auto &vcFuture = _vcsToWrite[buffpos / blksz]; // should be correct now...

  ink_assert(vcFuture.get() == currBlock);

  vcFuture = std::shared_future<TSVConn>(); // writing it now .. so zero it out

  // should send a WRITE_COMPLETE rather quickly
  _cacheWrVIO = TSVConnWrite(currBlock, _writeEvents, teerdr, wrlen);

  DEBUG_LOG("store ++++ performed write [%p] pos:%#lx+%#lx / +%#lx+%#lx", _cacheWrVIO, buffpos, wrlen, oavail, len);
  return len;
}

void
BlockStoreXform::handleBlockWrite(TSEvent event, void *edata, std::nullptr_t)
{
  TSVIO blkWriteVIO = static_cast<TSVIO>(edata);

  if (!blkWriteVIO) {
    DEBUG_LOG("empty edata event e#%d", event);
    return;
  }

  switch (event) {
  case TS_EVENT_VCONN_WRITE_READY:
    // didn't detect enough bytes in buffer to complete?
    if (TSIOBufferReaderAvail(teeReader())) {
      DEBUG_LOG("cache-write flush e#%d -> %ld? %ld?", event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
      TSVIOReenable(blkWriteVIO);
    } else {
      DEBUG_LOG("cache-write nothing to flush: e#%d -> %ld? %ld?", event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
    }
    break;
  case TS_EVENT_VCONN_WRITE_COMPLETE:
    if (!TSVIONTodoGet(inputVIO())) {
      DEBUG_LOG("cache-write final reenables: e#%d", event);
    } else {
      DEBUG_LOG("cache-write block: e#%d -> %ld? %ld?", event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
    }
    TSVConnClose(TSVIOVConnGet(blkWriteVIO));
    break;
  default:
    DEBUG_LOG("cache-write event: e#%d", event);
    break;
  }
}

BlockStoreXform::~BlockStoreXform()
{
  using namespace std::chrono;
  using std::future_status;

  atscppapi::ScopedContinuationLock lock(*this);

  for( auto &&p : _vcsToWrite ) {
    auto i = &p - &_vcsToWrite.front();
    if ( ! p.valid() ) {
      DEBUG_LOG("ignore invalid cache-write: #%ld",i);
    } else if ( p.wait_for(seconds::zero()) == future_status::ready ) {
      DEBUG_LOG("toss late/delayed cache-write: #%ld",i);
    } else {
      auto ptrErr = reinterpret_cast<intptr_t>(p.get());
      if ( ptrErr > 0 || ptrErr < -INK_START_ERRNO - 1000) {
        TSVConnClose(p.get());
        DEBUG_LOG("closed successful cache-write: #%ld %p",i,p.get());
      } else {
        DEBUG_LOG("pass failed cache-write: #%ld %p",i,p.get());
      }
    } 
    p = std::shared_future<TSVConn>(); // make invalid
  }
}
