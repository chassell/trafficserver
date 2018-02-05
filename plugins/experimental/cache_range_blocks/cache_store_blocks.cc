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
  _storeXform->start_write_futures(0); // attempt to start writes now if possible...
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
BlockStoreXform::start_write_futures(int blk)
{
  ssize_t vcsLeft = _ctxt.indexLen() - _vcsReady;
  ssize_t vcsWaiting = _vcsReady - blk; // 

  // if not enough vconns ready...
  if ( vcsWaiting >= std::min(WRITE_COUNT_LIMIT*2L,vcsLeft+0) ) {
    return;
  }

  DEBUG_LOG("init writes: nwrites:%ld buff=%d+ nleft:%ld", write_count(), blk, vcsLeft);

  auto &keys  = _ctxt.keysInRange();
  auto nxtBlk = _ctxt._beginByte / _ctxt.blockSize();

  if ( ! _vcsToWriteP ) {
    _vcsToWriteP.reset( new WriteVCs_t{}, []( WriteVCs_t *ptr ){ delete ptr; } );
    _vcsToWriteP->reserve(_ctxt.indexLen()); // prevent need to realloc
  }

  // start at last confirmed
  nxtBlk += vconn_count();

  int limit = 5; // block-writes spawned at one time...

  for ( auto i = keys.begin() + vconn_count() ; i != keys.end() ; ++i, ++nxtBlk )
  {
    if ( write_count() >= WRITE_COUNT_LIMIT ) {
      _blockVIOUntil = TS_EVENT_CACHE_CLOSE; // post blocked-cause flag
      break;
    }

    _blockVIOUntil = TS_EVENT_NONE; // no blocking currently

    auto &key = *i; // examine all the not-future'd keys
    if ( ! key || ! key.valid() ) {
      DEBUG_LOG("store: 1<<%ld present / not storable", nxtBlk);
      continue;
    }

    auto &vcFuture = at_vconn_future(key); // get future to match key
    auto vcErr = vcFuture.error();

    if ( ! vcErr || vcErr == ESOCK_TIMEOUT ) {
      if ( limit < 5 ) {
        DEBUG_LOG("store: 1<<%ld started or ready", nxtBlk);
      }
      continue; // skip a waiting future
    }

    // empty future to prep
    auto contp = ATSCont::create_temp_tscont(*this, vcFuture, _vcsToWriteP);
    DEBUG_LOG("store: 1<<%ld to write nwrites:%ld", nxtBlk, write_count());
    TSCacheWrite(contp, key); // find room to store each key...

    if ( ! --limit ) {
      break;
    }
  }

  DEBUG_LOG("store: nwrites:%ld inds=(0-%ld) len=%#lx [blk:%ldK, %#lx bytes]", write_count(), 
              vconn_future_count()-1, _ctxt._assetLen, _ctxt.blockSize()/1024, _ctxt.blockSize());
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
    if ( blk >= vconn_future_count() ) {
      _blockVIOUntil = TS_EVENT_CACHE_OPEN_WRITE; // set blocked-cause flag
      DEBUG_LOG("unavailable VConn future 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", blk + firstBlk, nxtPos, inpos, len, blksz/1024);
      return nullptr;
    }

    _blockVIOUntil = TS_EVENT_NONE; // can proceed

    if ( ! keys[blk] ) {
      // can't find a ready write waiting?
      DEBUG_LOG("unstored VConn future 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", blk + firstBlk, nxtPos, inpos, len, blksz/1024);
      continue;
    }
 
    auto &vcFuture = at_vconn_future(keys[blk]);
    auto vconn = vcFuture.get();

    // write is ready?
    if (vconn) {
      DEBUG_LOG("ready VConn future 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", blk + firstBlk, nxtPos, inpos, len, blksz/1024);
      skip = nxtPos - inpos; // report if a skip is needed...
      return vconn;
    }

    // can't find a ready write waiting?
    DEBUG_LOG("failed VConn future [%s] 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", InkStrerror(vcFuture.error()), blk + firstBlk, nxtPos, inpos, len, blksz/1024);

    vcFuture.release();
    _vcsReady = blk; // back up set of ready blocks
    _blockVIOUntil = TS_EVENT_CACHE_OPEN_WRITE; // set blocked-cause flag
    return nullptr; // as if it was unavailable
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

  auto &keys = _ctxt._keysInRange;
  auto firstBlk = _ctxt.firstIndex();

  // prevent multiple starts of a storage
  // atscppapi::ScopedContinuationLock lock(*this);

  // position is distance within body *without* len...

  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());
  // amount "ready" for block is read from our reader
  auto endpos = TSVIONBytesGet(inputVIO()); // expected final input byte
  auto nblk = readypos / blksz;

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

    // add more if needed...
    start_write_futures(blk);

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
    auto &vcFuture = at_vconn_future(keys[blk]); // should be correct now...

    ink_assert(vcFuture.get() == currBlock);

    keys[blk].reset(); // no key any more either

    // give ownership of current teereader / teebuffer
    ATSCacheKey key(_ctxt.clientUrl(), _ctxt._etagStr, (firstBlk + blk) * _ctxt.blockSize());
    BlockWriteData callData = { shared_from_this(), ThreadTxnID::get(), static_cast<int>(_ctxt.firstIndex() + blk), static_cast<int>(blk), key.release() };

    // make a totally async set of callbacks to write out new block
    auto blockCont = new ATSCont(&BlockStoreXform::handleBlockWrite, std::move(callData)); // separate mutexes!
    cloneAndSkip(blksz); // substitute new buffer ...

    TSVConnWrite(currBlock, blockCont->get(), teerdr, wrlen); // copy older buffer bytes out

    _vcsReady = blk+1; // next not ready block ..

    DEBUG_LOG("store ++++ beginning block w/write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
             _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
  }

  DEBUG_LOG("store **** waiting at current block 1<<%ld pos:%#lx+%#lx", firstBlk + donepos/blksz, donepos, readypos - donepos);
}

TSEvent
BlockStoreXform::handleBlockWrite(TSCont cont, TSEvent event, void *edata, const BlockWriteData &block)
{
  ThreadTxnID txnid(block._txnid);

  TSVIO vio = static_cast<TSVIO>(edata);

  if (!vio) {
    DEBUG_LOG("empty edata event e#%d", event);
    return TS_EVENT_ERROR; // done
  }

  switch (event) {
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
      TSIOBufferReader_t ordr( TSVIOReaderGet(vio) ); // dtor upon break
      TSIOBuffer_t obuff( TSVIOBufferGet(vio) ); // dtor upon break
      ATSCacheKey key( block._key );

      int wrcnt = block._writeXform.use_count()-1;
      auto vconnv = TSVIOVConnGet(vio);

      // attempt to close VConn here
      
      TSCont xformCont = *block._writeXform; // doesn't change
      auto xfmutex = TSContMutexGet(xformCont);
      TSEvent blockEvt = block._writeXform->_blockVIOUntil; // need un-blocking?
      TSVConn vconn = nullptr;

      // quick grab..
      if ( xfmutex && TSMutexLockTry(xfmutex) == TS_SUCCESS )
      {
        auto &vcsToWrite = *block._writeXform->_vcsToWriteP;
        vconn = vcsToWrite[block._ind].release(); // check old vconn
        if ( block._writeXform->_blockVIOUntil == TS_EVENT_CACHE_CLOSE ) {
          block._writeXform->_blockVIOUntil = TS_EVENT_NONE;
        }
        TSMutexUnlock(xfmutex);
      }

      if ( vconn == vconnv && blockEvt == TS_EVENT_CACHE_CLOSE ) {
        DEBUG_LOG("completed store with wakeup to 1<<%d nwrites:%d",block._blkid,wrcnt);
        TSVConnClose(vconnv); // flush and store
        TSContSchedule(xformCont, 0, TS_THREAD_POOL_DEFAULT);
      } else if ( vconn == vconnv ) {
        DEBUG_LOG("completed store to 1<<%d nwrites:%d",block._blkid,wrcnt);
        TSVConnClose(vconnv); // flush and store
      } else if ( blockEvt == TS_EVENT_CACHE_CLOSE ) {
        DEBUG_LOG("completed unclosed store with wakeup to 1<<%d nwrites:%d",block._blkid, wrcnt);
        TSContSchedule(xformCont, 0, TS_THREAD_POOL_DEFAULT);
      } else {
        DEBUG_LOG("completed unclosed store to 1<<%d nwrites:%d",block._blkid, wrcnt);
      }
      return TS_EVENT_NONE;
    }

#if 0
      TSCacheRead(cont, key.get()); // get a new read
      break;
    }

  case TS_EVENT_CACHE_OPEN_READ_FAILED:
    DEBUG_LOG("failed read check %p to 1<<%d nwrites:%ld",edata, block._blkid, block._writeXform->write_count()-1);
    return TS_EVENT_NONE;

  case TS_EVENT_CACHE_OPEN_READ:
    DEBUG_LOG("completed open read with 1<<%d nwrites:%ld",block._blkid, block._writeXform->write_count()-1);
    TSVConnRead(static_cast<TSVConn>(edata), cont, TSIOBufferCreate(), 1);
    break;
  case TS_EVENT_VCONN_READ_COMPLETE:
    DEBUG_LOG("completed read check with 1<<%d nwrites:%ld",block._blkid, block._writeXform->write_count()-1);
    TSVConnClose( TSVIOVConnGet(vio) ); // read-cache VC
    TSIOBufferDestroy( TSVIOBufferGet(vio) ); // temporary buffer
    return TS_EVENT_NONE;
#endif

  default:
    DEBUG_LOG("unknown event: e#%d", event);
    break;
  }
  return TS_EVENT_CONTINUE;
}
