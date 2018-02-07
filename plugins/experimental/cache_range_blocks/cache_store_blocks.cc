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

#define WRITE_COUNT_LIMIT 8

void
BlockSetAccess::start_cache_miss()
{
  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_MISS);

  TSHttpTxnTransformedRespCache(atsTxn(),0);
  if ( _etagStr.empty() ) {
    DEBUG_LOG("cache-resp-wr: stub len=%#lx [blk:%ldK]", _assetLen, _blkSize/1024);
    TSHttpTxnServerRespNoStoreSet(atsTxn(),0); // first assume we must store this..
    TSHttpTxnUntransformedRespCache(atsTxn(),1);
  } else {
    DEBUG_LOG("cache-resp-wr: blocks len=%#lx [blk:%ldK]", _assetLen, _blkSize/1024);
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // first assume we cannot use this..
    TSHttpTxnUntransformedRespCache(atsTxn(),0);
  }

  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS);

  _storeXform = std::make_shared<BlockStoreXform>(*this,indexLen());
}

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt, int blockCount)
  : BlockTeeXform(ctxt.txn(), 
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len) { this->handleBodyRead(r, inpos, len); },
                  ctxt.contentLen(),
                  ctxt._beginByte % ctxt.blockSize()),
    _ctxt(ctxt)
{
  DEBUG_LOG("tee-buffer block level reset to: %ld", ctxt.blockSize());
}

BlockStoreXform::~BlockStoreXform()
{
  DEBUG_LOG("destruct start");
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
  auto srvrAssetLen = std::atol( srvrRange.c_str() + l);

  if ( srvrAssetLen <= 0 ) 
  {
    DEBUG_LOG("rejecting due to unparsable asset length: %s",srvrRange.c_str());
    _storeXform.reset(); // cannot perform transform!
    txn.resume();
    return; // cannot use this for range block storage ...
  }

  // orig client requested bytes-from-end-of-stream
  if ( _beginByte < 0 ) {
    _beginByte = srvrAssetLen + _beginByte;
    _endByte = srvrAssetLen;
  }

  // orig client requested end-of-stream
  if ( ! _endByte ) {
    _endByte = srvrAssetLen;
  }

  auto currETag = respHdrs.value(ETAG_TAG);

  if (srvrAssetLen == _assetLen && currETag == _etagStr ) {
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // we don't need to save this... assuredly..
    DEBUG_LOG("srvr-resp: len=%#lx (%ld-%ld) final=%s etag=%s", _assetLen, _beginByte, _endByte, srvrRange.c_str(), _etagStr.c_str());
    txn.resume(); // continue with expected transform
    return;
  }

  DEBUG_LOG("stub-reset: len=%#lx olen=%#lx (%ld-%ld) final=%s etag=%s", srvrAssetLen, _assetLen, _beginByte, _endByte, srvrRange.c_str(), currETag.c_str());
  // resetting the stub headers

  TSHttpTxnServerRespNoStoreSet(atsTxn(),0); // must assume now we need to save these headers!

  _assetLen = srvrAssetLen;
  _etagStr = currETag;

  reset_range_keys();

  resp.setStatusCode(HTTP_STATUS_OK);
  respHdrs.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
  respHdrs.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please notice it!

  DEBUG_LOG("stub-hdrs:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
  txn.resume();
}


TSCacheKey
BlockStoreXform::next_valid_vconn(int64_t inpos, int64_t len, int64_t &skip)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  auto firstBlk = _ctxt.firstIndex();

  skip = -1;

  // find distance to next start point (w/zero possible)

  // amt past boundary (maybe full block)
  auto fwdDist = ((inpos + blksz - 1) % blksz) + 1; // between 1 and blksz
  auto nxtPos = inpos - fwdDist + blksz; // (can be same as inpos)
  auto &keys = _ctxt.keysInRange();
  auto blk = nxtPos / blksz;

  // find nxtPos within this new span ... and before end
  for ( ; nxtPos <= inpos + len ; (nxtPos += blksz),++blk ) 
  {
    if ( ! keys[blk] ) {
      // can't find a ready write waiting?
      DEBUG_LOG("unstored VConn future 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", blk + firstBlk, nxtPos, inpos, len, blksz/1024);
      continue;
    }
 
    return keys[blk].get();
  }

  // no boundary within distance?
  DEBUG_LOG("skip required pos:%#lx -> %#lx len=%#lx [blk:%ldK]", inpos, nxtPos, len, blksz/1024);
  return nullptr;
}


//
// called from writeHook
//      -- after outputBuffer() was filled
void
BlockStoreXform::handleBodyRead(TSIOBufferReader teerdr, int64_t donepos, int64_t readypos)
{
  // input has closed down?
  if (!teerdr) {
    DEBUG_LOG("store **** final call");
    return;
  }

  auto firstBlk = _ctxt.firstIndex();

  // prevent multiple starts of a storage
  // atscppapi::ScopedContinuationLock lock(*this);

  // position is distance within body *without* len...

  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());
  // amount "ready" for block is read from our reader
  auto endpos = TSVIONBytesGet(inputVIO()); // expected final input byte

  // too much load to add a new write
  while ( donepos < readypos ) 
  {
    if ( write_count() >= WRITE_COUNT_LIMIT && _blockVIOUntil == TS_EVENT_CACHE_CLOSE ) {
      return; // silent...
    }

    if ( write_count() >= WRITE_COUNT_LIMIT ) {
      _blockVIOUntil = TS_EVENT_CACHE_CLOSE; // set blocked-cause flag
      break;
    }

    _blockVIOUntil = TS_EVENT_NONE; // can proceed

    teerdr = teeReader(); // reset if changed
    auto navail = TSIOBufferReaderAvail(teeReader()); // recheck..
    donepos = readypos - navail; // 
    auto blk = donepos / blksz;

    if ( ! navail || donepos >= endpos ) {
      return; // complete
    }

    // check any unneeded bytes before next block
    int64_t skipDist = 0;
    auto currBlock = next_valid_vconn(donepos, navail, skipDist);

    // new blocking problem?
    if (! currBlock && _blockVIOUntil ) {
      DEBUG_LOG("store **** stop at current block 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
      return;
    }

    // no block to write?
    if (! currBlock) {
      // no need to save these bytes?
      TSIOBufferReaderConsume(teerdr, navail);
      DEBUG_LOG("store **** skip current block 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
      return;
      //////// RETURN
    }

    if (skipDist) {
      TSIOBufferReaderConsume(teerdr, skipDist); // skipped

      navail = TSIOBufferReaderAvail(teerdr); // recheck..
      donepos += skipDist; // more completed bytes 
      blk = donepos / blksz;

      DEBUG_LOG("store **** skipping to buffered pos:%#lx+%#lx --> skipped %#lx", donepos, navail, skipDist);
    }

    // reader has reached block boundary now...

    auto wrlen = std::min(endpos - donepos,blksz); // len needed for a block write...

    if ( navail < wrlen ) {
      DEBUG_LOG("store **** reading in pos:%#lx+%#lx [<= +%#lx]", donepos, navail, wrlen);
      return;
      //////// RETURN
    }

    // reader has more than the needed bytes for a complete write
//    auto &vcFuture = at_vconn_future(keys[blk]); // should be correct now...
//
//    ink_assert(vcFuture.get() == currBlock);

    // ATSCacheKey key(_ctxt.clientUrl(), _ctxt._etagStr, (firstBlk + blk) * _ctxt.blockSize());

    // give ownership of current teereader / teebuffer
    auto callDatap = std::make_unique<BlockWriteInfo>(*this, teeBuffer(), teeReader(), static_cast<int>(blk));
    // make a totally async set of callbacks to write out new block
    cloneAndSkip(blksz); // substitute new buffer ...

    auto &callData = *callDatap;
    auto blockCont = std::make_unique<ATSCont>(&BlockStoreXform::handleBlockWrite, std::move(callDatap)); // separate mutexes!
    TSCacheWrite(blockCont->get(), callData._key.get()); // find room to store each key...

    DEBUG_LOG("store ++++ beginning block w/write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
             _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
  }

  DEBUG_LOG("store **** waiting at current block 1<<%ld pos:%#lx+%#lx", firstBlk + donepos/blksz, donepos, readypos - donepos);
}

TSEvent
BlockStoreXform::handleBlockWrite(TSCont cont, TSEvent event, void *edata, const std::unique_ptr<BlockWriteInfo> &blockp)
{
  BlockWriteInfo &block = *blockp;
  ThreadTxnID txnid(block._txnid);

  if (!edata) {
    DEBUG_LOG("empty edata event e#%d", event);
    return TS_EVENT_ERROR; // done
  }

  TSVIO vio = static_cast<TSVIO>(edata);
  TSVConn vconn = static_cast<TSVConn>(edata);
  intptr_t verr = -reinterpret_cast<intptr_t>(edata);

  switch (event) {
  case TS_EVENT_CACHE_OPEN_WRITE_FAILED:
    DEBUG_LOG("failed read check [%s] to 1<<%d nwrites:%ld", InkStrerror(verr), block._blkid, block._writeXform->write_count()-1);
    return TS_EVENT_NONE; // call dtor of ATSCont

  case TS_EVENT_CACHE_OPEN_WRITE:
    DEBUG_LOG("completed open read with 1<<%d nwrites:%ld",block._blkid, block._writeXform->write_count()-1);
    block._vconn = ATSVConnFuture(vconn);
    TSVConnWrite(vconn, cont, block._rdr.get(), TSIOBufferReaderAvail(block._rdr.get())); // copy older buffer bytes out
    break;

  case TS_EVENT_VCONN_WRITE_READY:
    {
      // didn't detect enough bytes in buffer to complete?
      TSIOBufferReader writeRdr = TSVIOReaderGet(vio);
      if (! TSIOBufferReaderAvail(writeRdr)) {
        DEBUG_LOG("cache-write 1<<%d flush empty e#%d -> %ld? %ld?", block._blkid, event, TSVIONDoneGet(vio), TSVIONBytesGet(vio));
        break; // surprising!
      }
      DEBUG_LOG("cache-write 1<<%d flush e#%d -> %ld? %ld?", block._blkid, event, TSVIONDoneGet(vio), TSVIONBytesGet(vio));
      TSVIOReenable(vio);
      break;
    }

  case TS_EVENT_VCONN_WRITE_COMPLETE:
    {
      BlockStoreXform &store = *block._writeXform; // doesn't change
      int wrcnt = store.write_count();
      auto vconnv = TSVIOVConnGet(vio);

      // attempt to close VConn here
      
      auto xfmutex = TSContMutexGet(store);
      TSEvent blockEvt = block._writeXform->_blockVIOUntil; // need un-blocking?

      // quick grab..
      if ( xfmutex && TSMutexLockTry(xfmutex) == TS_SUCCESS )
      {
        if ( block._writeXform->_blockVIOUntil == TS_EVENT_CACHE_CLOSE ) {
          block._writeXform->_blockVIOUntil = TS_EVENT_NONE;
        }
        TSMutexUnlock(xfmutex);
      }

      if ( blockEvt == TS_EVENT_CACHE_CLOSE ) {
        DEBUG_LOG("completed store with wakeup to 1<<%d nwrites:%d",block._blkid,wrcnt);
        TSVConnClose(vconnv); // flush and store
        TSContSchedule(store, 0, TS_THREAD_POOL_DEFAULT);
      } else {
        DEBUG_LOG("completed store to 1<<%d nwrites:%d",block._blkid,wrcnt);
        TSVConnClose(vconnv); // flush and store
      }
      return TS_EVENT_NONE; // call dtor of ATSCont
    }

  default:
    DEBUG_LOG("unknown event: e#%d", event);
    break;
  }
  return TS_EVENT_CONTINUE; // re-use ATSCont with data block
}
