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
  if ( _etagStr.empty() ) {
    TSHttpTxnServerRespNoStoreSet(atsTxn(),0); // first assume we must store this..
    TSHttpTxnTransformedRespCache(atsTxn(), 1);
  } else {
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // first assume we cannot use this..
    TSHttpTxnTransformedRespCache(atsTxn(), 0); // don't save
  }

  DEBUG_LOG("cache-resp-wr: len=%#lx [blk:%ldK]", _assetLen, _blkSize/1024);

  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS);

  _storeXform = std::make_unique<BlockStoreXform>(*this,endBlk - firstBlk);
  _storeXform->start_write_futures(); // attempt to start writes now if possible...
}

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt, int blockCount)
  : BlockTeeXform(ctxt.txn(), 
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len) { return this->handleBodyRead(r, inpos, len); },
                  ctxt.rangeLen(),
                  ctxt._beginByte % ctxt.blockSize()),
    _ctxt(ctxt)
{
  TSIOBufferWaterMarkSet(teeBuffer(), ctxt.blockSize()); // perfect point to buffer to..
  DEBUG_LOG("tee-buffer block level reset to: %ld", ctxt.blockSize());
}

BlockStoreXform::~BlockStoreXform()
{
  DEBUG_LOG("destruct start");
}

void
BlockStoreXform::start_write_futures()
{
  auto &keys  = _ctxt.keysInRange();
  auto nxtBlk = _ctxt._beginByte / _ctxt.blockSize();

  if ( ! _vcsToWriteP ) {
    _vcsToWriteP.reset( new WriteVCs_t{}, []( WriteVCs_t *ptr ){ delete ptr; } );
    _vcsToWriteP->reserve(keys.size()); // no need to realloc
  }

  int limit = 5; // block-writes spawned at one time...

  for ( auto &&key : keys )
  {
    if ( ! key || ! key.valid() ) {
      DEBUG_LOG("store: 1<<%ld present / not storable", nxtBlk++);
      continue;
    }

    auto &vcFuture = at_vconn_future(key); // get future to match key

    if ( vcFuture ) {
      DEBUG_LOG("store: 1<<%ld started or ready", nxtBlk++);
      continue; // skip a waiting future
    }

    // empty future to prep
    auto contp = ATSCont::create_temp_tscont(*this, vcFuture, _vcsToWriteP);
    DEBUG_LOG("store: 1<<%ld to write", nxtBlk++);
    TSCacheWrite(contp, key); // find room to store each key...

    if ( ! --limit ) {
      break;
    }
  }

  DEBUG_LOG("store: len=%#lx [blk:%ldK, %#lx bytes]", _ctxt._assetLen, _ctxt.blockSize()/1024, _ctxt.blockSize());
}

void
BlockSetAccess::handleReadResponseHeaders(Transaction &txn) 
{
  auto &resp = txn.getServerResponse();
  auto &respHdrs = resp.getHeaders();

  if ( resp.getStatusCode() != HTTP_STATUS_PARTIAL_CONTENT ) {
    DEBUG_LOG("rejecting due to unusable status: %d",resp.getStatusCode());
    _storeXform.reset(); // cannot perform transform!
    txn.resume();
    return; // cannot use this for cache ...
  }

  auto contentEnc = respHdrs.values(CONTENT_ENCODING_TAG);

  if ( ! contentEnc.empty() && contentEnc != "identity" )
  {
    DEBUG_LOG("rejecting due to unusable encoding: %s",contentEnc.c_str());
    _storeXform.reset(); // cannot perform transform!
    txn.resume();
    return; // cannot use this for range block storage ...
  }

  ink_assert( _storeXform );

  auto srvrRange = respHdrs.value(CONTENT_RANGE_TAG); // not in clnt hdrs
  auto l = srvrRange.find('/');
  l = ( l != srvrRange.npos ? l + 1 : srvrRange.size() ); // zero-length c-string if no '/' found
  int64_t currAssetLen = std::atoll( srvrRange.c_str() + l );
  auto currETag = respHdrs.value(ETAG_TAG);

  if (currAssetLen == _assetLen && currETag == _etagStr ) {
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // we don't need to save this... assuredly..
    DEBUG_LOG("srvr-resp: len=%#lx (%ld-%ld) final=%s etag=%s", _assetLen, _beginByte, _endByte, srvrRange.c_str(), _etagStr.c_str());
    txn.resume(); // continue with expected transform
    return;
  }

  DEBUG_LOG("stub-reset: len=%#lx olen=%#lx (%ld-%ld) final=%s etag=%s", currAssetLen, _assetLen, _beginByte, _endByte, srvrRange.c_str(), currETag.c_str());
  // resetting the stub headers

  TSHttpTxnServerRespNoStoreSet(atsTxn(),0); // must assume now we need to save these headers!

  _assetLen = currAssetLen;
  _etagStr = currETag;

  reset_range_keys();
  _storeXform->reset_write_futures(); // restart all writes from empty...

  resp.setStatusCode(HTTP_STATUS_OK);
  respHdrs.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
  respHdrs.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please notice it!

  DEBUG_LOG("stub-hdrs:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
  txn.resume();
}


TSVConn
BlockStoreXform::next_valid_vconn(int64_t inpos, int64_t len, int64_t &skip)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());

  skip = -1;

  // find distance to next start point (w/zero possible)

  // amt past boundary (maybe full block)
  auto fwdDist = ((inpos + blksz - 1) % blksz) + 1; // between 1 and blksz
  auto nxtPos = inpos - fwdDist + blksz; // (can be same as inpos)
  auto lastPos = static_cast<int64_t>(_vcsToWriteP->size()) * blksz;
  auto &keys = _ctxt.keysInRange();
  auto blk = nxtPos / blksz;

  // find nxtPos within this new span ... and before end
  for ( ; nxtPos <= inpos + len && nxtPos < lastPos 
             ; (nxtPos += blksz),++blk ) 
  {
    auto vconn = at_vconn_future(keys[blk]).get();

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

  auto &keys = _ctxt.keysInRange();

  if ( ! inpos ) {
    auto n = _ctxt.blockSize() * keys.size();
    TSIOBufferWaterMarkSet(TSVIOBufferGet(xformInputVIO()), n); // grab full amount
    DEBUG_LOG("input block level reset to: %ld", n);
  }

  // prevent multiple starts of a storage
  atscppapi::ScopedContinuationLock lock(*this);

  // position is distance within body *without* len...

  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());

  {
    ssize_t vcsNeeded = keys.size() - _vcsToWriteP->size();
    ssize_t vcsReady = _vcsToWriteP->size() - inpos/blksz;

    if ( vcsReady < std::min(5L,vcsNeeded+0) ) {
      start_write_futures();
    }
  }

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

  auto absBeg = _ctxt._beginByte / blksz;
  // auto absEnd = ( _ctxt._endByte + blksz-1 ) / blksz;

  auto blk = donepos / blksz;
  auto &vcFuture = at_vconn_future(keys[blk]); // should be correct now...

  ink_assert(vcFuture.get() == currBlock);

  vcFuture.release(); // will skip old bytes next time...

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
