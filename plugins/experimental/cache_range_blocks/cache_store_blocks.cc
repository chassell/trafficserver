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

#include <algorithm>

void
BlockSetAccess::start_cache_miss(int64_t firstBlk, int64_t endBlk)
{
  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_MISS);

  TSHttpTxnUntransformedRespCache(atsTxn(), 0);
  TSHttpTxnTransformedRespCache(atsTxn(), 0);

  DEBUG_LOG("cache-resp-wr: len=%#lx [blk:%ldK] p=..%s.. u=..%s..", assetLen(), blockSize()/1024, 
        b64BlkPresentSubstr().c_str(), b64BlkUsableSubstr().c_str());

  _keysInRange.clear();
  _keysInRange.resize(endBlk - firstBlk);

  // react to genuine misses above...
  for (auto i = 0U; i < endBlk - firstBlk; ++i) {
    if (!is_base64_bit_set(_b64BlkPresent, firstBlk + i)) {
      DEBUG_LOG("attempt store from bitset: + 1<<%ld %s", firstBlk + i, b64BlkPresentSubstr().c_str());
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
    _vcsToWrite(*_vcsToWriteP)
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
    if ( ! keys[i] || is_base64_bit_set(_ctxt.b64BlkPresent(), blkNum) ) {
      continue;
    }

    auto contp = ATSCont::create_temp_tscont(*this, _vcsToWrite[i], _vcsToWriteP);
    TSCacheWrite(contp, keys[i]); // find room to store each key...
  }

  DEBUG_LOG("store: len=%#lx [blk:%ldK, %#lx bytes]", _ctxt._assetLen, _ctxt.blockSize()/1024, _ctxt.blockSize());
  txn.resume(); // wait for response
}


TSVConn
BlockStoreXform::next_valid_vconn(int64_t inpos, int64_t len, int64_t &skip)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  using namespace std::chrono;
  using std::future_status;

  skip = -1;

  // find distance to next start point (w/zero possible)

  // amt past boundary (maybe full block)
  auto fwdDist = ((inpos + blksz - 1) % blksz) + 1; // between 1 and blksz
  auto nxtPos = inpos - fwdDist + blksz; // (can be same as inpos)
  auto lastPos = static_cast<int64_t>(_vcsToWrite.size()) * blksz;

  // find nxtPos within this new span ... and before end
  for ( auto blk = nxtPos / blksz 
          ; nxtPos <= inpos + len && nxtPos < lastPos 
             ; (nxtPos += blksz),++blk ) 
  {
    auto vconn = _vcsToWrite[blk].get();

    // write is ready?
    if (vconn) {
      DEBUG_LOG("ready VConn future nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", nxtPos, inpos, len, blksz/1024);
      skip = nxtPos - inpos; // report if a skip is needed...
      return vconn;
    }

    // can't find a ready write waiting?
    DEBUG_LOG("non-valid VConn future nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", nxtPos, inpos, len, blksz/1024);
  }

  // no boundary within distance?
  DEBUG_LOG("skip required pos:%#lx -> %#lx len=%#lx [blk:%ldK]", inpos, nxtPos, len, blksz/1024);
  return nullptr;
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

  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());

  // amount "ready" for block is read from our reader
  auto navail = TSIOBufferReaderAvail(teerdr); // new amount
  auto donepos = inpos - (navail - len); // totally completed bytes
  auto endpos = inpos + TSVIONTodoGet(inputVIO()); // expected final input byte

  // check any unneeded bytes before next block
  int64_t skipDist = 0;
  auto currBlock = next_valid_vconn(donepos, navail, skipDist);

  // no block to write?
  if (! currBlock) {
    // no need to save these bytes?
    TSIOBufferReaderConsume(teerdr, navail);
    DEBUG_LOG("store **** skip current block pos:%#lx+%#lx+%#lx", donepos, navail - len, len);
    return len;
    //////// RETURN
  }

  if (skipDist) {
    TSIOBufferReaderConsume(teerdr, skipDist); // skipped

    navail = TSIOBufferReaderAvail(teerdr); // recheck..
    donepos += skipDist; // more completed bytes 

    DEBUG_LOG("store **** skipping to buffered pos:%#lx+%#lx --> skipped %#lx", donepos, navail, skipDist);
  }

  // reader has reached block boundary now...

  auto wrlen = std::min(endpos - donepos,blksz); // len needed for a block write...

  if ( navail < wrlen ) {
    DEBUG_LOG("store **** reading pos:%#lx+%#lx+%#lx < %#lx", donepos, navail - len, len, wrlen);
    return len;
    //////// RETURN
  }

  // reader has more than the needed bytes for a complete write

  auto blk = donepos / blksz;

  auto absBeg = _ctxt._beginByte / blksz;
  // auto absEnd = ( _ctxt._endByte + blksz-1 ) / blksz;

  auto &vcFuture = _vcsToWrite[blk]; // should be correct now...

  ink_assert(vcFuture.get() == currBlock);

  vcFuture.reset(); // will skip old bytes next time...

  // make a totally async set of callbacks to write out new block
  auto blockCont = TSContCreate(&BlockStoreXform::handleBlockWrite,TSContMutexGet(*this));
  intptr_t data = ThreadTxnID::get() | ((absBeg + blk) << 50);
  TSContDataSet(blockCont,reinterpret_cast<void*>(data));

  cloneAndSkip(blksz); // substitute new buffer ...

  TSVConnWrite(currBlock, blockCont, teerdr, wrlen); // copy older buffer bytes out

  DEBUG_LOG("store ++++ beginning block w/write 1<<%ld write @%#lx+%#lx+%#lx [final @%#lx]", absBeg + blk, donepos, navail, len, donepos + wrlen);
  return len;
}

int
BlockStoreXform::handleBlockWrite(TSCont cont, TSEvent event, void *edata)
{
  auto data = reinterpret_cast<intptr_t>(TSContDataGet(cont));
  auto blkid = data >> 50;

  ThreadTxnID txnid{static_cast<int>(data) & ~0};

  TSVIO blkWriteVIO = static_cast<TSVIO>(edata);
  TSIOBufferReader writeRdr = TSVIOReaderGet(blkWriteVIO);

  if (!blkWriteVIO) {
    DEBUG_LOG("empty edata event e#%d", event);
    return 0;
  }

  switch (event) {
  case TS_EVENT_VCONN_WRITE_READY:
    // didn't detect enough bytes in buffer to complete?
    if (! TSIOBufferReaderAvail(writeRdr)) {
      DEBUG_LOG("cache-write 1<<%ld flush empty e#%d -> %ld? %ld?", blkid, event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
      break; // surprising!
    }
    DEBUG_LOG("cache-write 1<<%ld flush e#%d -> %ld? %ld?", blkid, event, TSVIONDoneGet(blkWriteVIO), TSVIONBytesGet(blkWriteVIO));
    TSVIOReenable(blkWriteVIO);
    break;

  case TS_EVENT_VCONN_WRITE_COMPLETE:
  {
    TSIOBufferReader_t ordr( TSVIOReaderGet(blkWriteVIO) ); // dtor upon break
    TSIOBuffer_t obuff( TSVIOBufferGet(blkWriteVIO) ); // dtor upon break

    DEBUG_LOG("completed store to bitset 1<<%ld",blkid);

    auto vc = TSVIOVConnGet(blkWriteVIO);
    TSVConnClose(vc); // flush and store
    TSContDestroy(cont);
    break;
  }

  default:
    DEBUG_LOG("cache-write event: e#%d", event);
    break;
  }
  return 0;
}
