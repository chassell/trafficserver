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

  DEBUG_LOG("cache-resp-wr: len=%#lx [blk:%ldK] set=%s", assetLen(), blockSize()/1024, b64BlkBitset().c_str());

  _keysInRange.clear();
  _keysInRange.resize(endBlk - firstBlk);

  // react to genuine misses above...
  for (auto i = 0U; i < endBlk - firstBlk; ++i) {
    if (!is_base64_bit_set(_b64BlkBitset, firstBlk + i)) {
      _keysInRange[i]    = std::move(ATSCacheKey(clientUrl(), _etagStr, (firstBlk + i) * _blkSize));
    }
  }

  _storeXform = std::make_unique<BlockStoreXform>(*this,endBlk - firstBlk);
  _storeXform->handleReadCacheLookupComplete(txn()); // may have txn.resume()
}

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt, int blockCount)
  : TransactionPlugin(ctxt.txn()),
    BlockTeeXform(ctxt.txn(), 
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len) { return this->handleBodyRead(r, inpos, len); },
                  ctxt.rangeLen(),
                  ctxt._beginByte % ctxt.blockSize()),
    _ctxt(ctxt),
    // must handle late late returns for TSCacheWrite
    _vcsToWriteP( new WriteVCs_t(blockCount), []( WriteVCs_t *ptr ){ delete ptr; } ),
    _vcsToWrite(*_vcsToWriteP),
    _writeEvents(*this, &BlockStoreXform::handleBlockWrite, nullptr, *this)
{
  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);  // add block-range and clean up
  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
}

BlockStoreXform::~BlockStoreXform()
{
  DEBUG_LOG("destruct start");
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  using namespace std::chrono;
  using std::future_status;

  DEBUG_LOG("store: xhook:%p _ctxt:%p len=%#lx [blk:%ldK]", this, &_ctxt, _ctxt.assetLen(), _ctxt.blockSize()/1024);

  // [will override the server response for headers]
  //
  auto &keys  = _ctxt._keysInRange;
  auto blkNum = _ctxt._beginByte / _ctxt.blockSize();

  for (auto i = 0U; i < keys.size(); ++i, ++blkNum) 
  {
    if ( ! keys[i] || is_base64_bit_set(_ctxt.b64BlkBitset(), blkNum) ) {
      continue;
    }

    auto contp = ATSCont::create_temp_tscont(*this, _vcsToWrite[i], _vcsToWriteP);
    TSCacheWrite(contp, keys[i]); // find room to store each key...
  }

  DEBUG_LOG("store: len=%#lx [blk:%ldK, %#lx bytes]", _ctxt._assetLen, _ctxt.blockSize()/1024, _ctxt.blockSize());
  txn.resume(); // wait for response
}


int64_t
BlockStoreXform::next_valid_vconn(TSVConn &vconn, int64_t inpos, int64_t len)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  using namespace std::chrono;
  using std::future_status;

  vconn = nullptr;

  auto absBlk = _ctxt._beginByte / blksz;
  auto absEnd = ( _ctxt._endByte + blksz-1 ) / blksz;

  // find distance to next start point (w/zero possible)

  auto fwdDist = ((inpos + blksz - 1) % blksz) + 1; // between 1 and blksz
  int64_t revDist = blksz - fwdDist;              // between zero and blksz-1
  auto nxtPos = inpos + revDist;

  // skip available or unstorable blocks
  for ( ; nxtPos <= inpos + len ; nxtPos += blksz ) {
    auto blk     = nxtPos / blksz;

    if (blk >= static_cast<int64_t>(_vcsToWrite.size())) {
      DEBUG_LOG("beyond final block final block start revDist:- blk%#lx,%#lx n=%ld)", absBlk + blk, absEnd, absEnd - absBlk);
      return -1;
    }
   
    vconn = _vcsToWrite[blk].get();
    // can't find a ready write waiting?
    if (!vconn) {
      DEBUG_LOG("non-valid VConn future nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", nxtPos, inpos, len, _ctxt.blockSize()/1024);
      continue;
    }

    DEBUG_LOG("ready VConn future nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", nxtPos, inpos, len, _ctxt.blockSize()/1024);
    return nxtPos - inpos;
  }

  // no boundary within distance?
  DEBUG_LOG("skip required pos:%#lx -> %#lx len=%#lx [blk:%ldK]", inpos, nxtPos, len, _ctxt.blockSize()/1024);
  return -1;
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

  auto oavail = navail - len; // amount that hook had accepted before...
  auto buffpos  = inpos - oavail; // last consumed/written pos ...
  auto endpos = inpos + TSVIONTodoGet(inputVIO()); // final byte pos

  // check any unneeded bytes before next block
  auto skipDist = next_valid_vconn(currBlock, buffpos, navail);

  // don't have enough to reach full block boundary?
  if (skipDist < 0) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(teerdr, navail);
    DEBUG_LOG("store **** skip current block pos:%#lx+%#lx+%#lx", buffpos, oavail, len);
    return len;
    //////// RETURN
  }

  ink_assert(currBlock);

  if (skipDist) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(teerdr, skipDist);
    // recheck..
    navail = TSIOBufferReaderAvail(teerdr);
    // align forward..
    buffpos += skipDist;

    DEBUG_LOG("store **** skipping to buffered pos:%#lx+%#lx --> skipped %#lx", buffpos, navail, skipDist);
  }

  // at block boundary now...

  auto wrlen = std::min(endpos - buffpos,blksz); // len needed for a block write...

  if (navail < wrlen ) {
    DEBUG_LOG("store ++++ buffering more dist pos:%#lx+%#lx [+%#lx]", buffpos, navail, wrlen);
    return len; // limit amt left by distance to a filled block
    //////// RETURN
  }

  auto &vcFuture = _vcsToWrite[buffpos / blksz]; // should be correct now...

  ink_assert(vcFuture.get() == currBlock);

//  vcFuture = std::shared_future<TSVConn>(); // writing it now .. so zero block out

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
    if (! TSIOBufferReaderAvail(teeReader())) {
      break; // surprising!
    }
    DEBUG_LOG("cache-write flush e#%d -> %ld? %ld?", event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
    TSVIOReenable(blkWriteVIO);
    break;
  case TS_EVENT_VCONN_WRITE_COMPLETE:
    break;
  default:
    DEBUG_LOG("cache-write event: e#%d", event);
    break;
  }
}

void
close_all_vcs(BlockStoreXform::WriteVCs_t &vect)
{
  using namespace std::chrono;
  using std::future_status;

  DEBUG_LOG("vcs destruct started");
}
