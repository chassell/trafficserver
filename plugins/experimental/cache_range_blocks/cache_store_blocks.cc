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

#define WRITE_COUNT_LIMIT 4

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
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len, int64_t added) { this->handleBodyRead(r, inpos, len, added); },
                  ctxt.contentLen(),
                  ctxt._beginByte % ctxt.blockSize()),
    _ctxt(ctxt)
{
  reset_write_keys();
}

BlockStoreXform::~BlockStoreXform()
{
  DEBUG_LOG("destruct start: writes:%ld", write_count());
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
  _storeXform->reset_write_keys();

  if ( indexLen() > 100 ) {
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // don't store!
    TSHttpTxnUntransformedRespCache(atsTxn(),0); // don't store!
  }

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
  auto &keys = _keysToWrite;
  auto blk = nxtPos / blksz;

  // find nxtPos within this new span ... and before end
  for ( ; nxtPos <= inpos + len ; (nxtPos += blksz),++blk ) 
  {
    if ( ! keys[blk] ) {
      // can't find a ready write waiting?
      DEBUG_LOG("unstored VConn future 1<<%ld nxtPos:%#lx pos:%#lx len=%#lx [blk:%ldK]", blk + firstBlk, nxtPos, inpos, len, blksz/1024);
      continue;
    }
 
    skip = nxtPos - inpos; // report if a skip is needed...
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
BlockStoreXform::handleBodyRead(TSIOBufferReader teerdr, int64_t donepos, int64_t readypos, int64_t added)
{
  // input has closed down?
  if (!teerdr) {
    DEBUG_LOG("store **** final call");
    return;
  }

  if ( donepos == readypos && ! added ) {
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
    teerdr = teeReader(); // reset if changed
    auto navail = TSIOBufferReaderAvail(teerdr); // recheck..
    donepos = readypos - navail; // 
    auto blk = static_cast<int>(donepos / blksz);

    if ( write_count() >= WRITE_COUNT_LIMIT ) {
      auto older = _blockVIOUntil.exchange(TS_EVENT_CACHE_CLOSE);
      if ( older == TS_EVENT_CACHE_CLOSE && ! added ) {
//        DEBUG_LOG("store **** stop for wakeup 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
        return; // silent...
      }
      break;
    }

    _blockVIOUntil = TS_EVENT_NONE; // need more input only...
    if ( ! navail || donepos >= endpos ) {
//      DEBUG_LOG("store **** stop for end 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
      return; // complete
    }

    // check any unneeded bytes before next block
    int64_t skipDist = 0;
    auto currKey = next_valid_vconn(donepos, navail, skipDist);

    // new blocking problem?
    if (! currKey && _blockVIOUntil ) {
      DEBUG_LOG("store **** stop at current block 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
      return; // restart needed...
    }

    // no block to write?
    if (! currKey) {
      // no need to save these bytes?
      TSIOBufferReaderConsume(teerdr, navail);
      DEBUG_LOG("store **** skip current block 1<<%ld pos:%#lx+%#lx", firstBlk + blk, donepos, navail);
      return; // more input needed
      //////// RETURN
    }

    if (skipDist > 0) {
      TSIOBufferReaderConsume(teerdr, skipDist); // skipped

      navail = TSIOBufferReaderAvail(teerdr); // recheck..
      donepos += skipDist; // more completed bytes 
      blk = donepos / blksz;

      DEBUG_LOG("store **** skipping to buffered pos:%#lx+%#lx --> skipped %#lx", donepos, navail, skipDist);
    }

    // reader has reached block boundary now...

    auto wrlen = std::min(endpos - donepos,blksz); // len needed for a block write...

    if ( navail < wrlen && ! added ) {
      return;
      //////// RETURN
    }

    if ( navail < wrlen ) {
      DEBUG_LOG("store **** reading in pos:%#lx+%#lx [<= +%#lx]", donepos, navail, wrlen);
      return;
      //////// RETURN
    }

    // going to give ownership of current teereader / teebuffer
    auto callDatap = std::make_shared<BlockWriteInfo>(*this, teeBuffer(), teeReader(), blk);
    auto &callData = *callDatap;

    ink_assert( currKey == callData._key.get() );

    // direct new to allow self-delete
    auto blockCont = (new ATSCont(&BlockWriteInfo::handleBlockWriteCB, 
                                   callDatap->shared_from_this()))
                 ->get(); // get the TSCont

    auto wrcnt = write_count();

    // notify quickly if we need a new wakeup...
    if ( wrcnt >= WRITE_COUNT_LIMIT ) {
      DEBUG_LOG("store ++++ beginning [full] write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
      _blockVIOUntil.exchange(TS_EVENT_CACHE_CLOSE); // set flag 
    } else {
      DEBUG_LOG("store ++++ beginning write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
    }

    cloneAndSkip(blksz); // move on to new buffers...

    auto r = TSCacheWrite(blockCont, callData._key.get()); // find room to store each key...

    // Did write fail totally?
    auto err = callData._vconn.error();
    if ( err && err != ESOCK_TIMEOUT ) 
    {
      DEBUG_LOG("store ++++ failed write 1<<%ld [%s] nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, InkStrerror(callData._vconn.error()),
               write_count(), donepos, navail, donepos + wrlen);
      _blockVIOUntil.exchange(TS_EVENT_CACHE_CLOSE); // set flag 
      return; // wait for wakeup ...
    }

    if ( ! TSActionDone(r) ) {
      DEBUG_LOG("store ++++ incomplete write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
      _blockVIOUntil.exchange(TS_EVENT_CACHE_CLOSE); // set flag 
      return; // wait for wakeup ...
    }

    if ( callDatap.use_count() == 1 ) {
      DEBUG_LOG("store ++++ completed write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
    } else {
      DEBUG_LOG("store ++++ started write 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
    }

    if ( wrcnt >= WRITE_COUNT_LIMIT ) {
      return; // requested a wakeup already
    }
    // continue...
  }

  auto navail = TSIOBufferReaderAvail(teerdr); // recheck after swap
  if ( readypos >= endpos && navail ) {
    _blockVIOUntil = TS_EVENT_CACHE_CLOSE;
  }

  DEBUG_LOG("store **** nwrites:%ld waiting at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
}

BlockWriteInfo::BlockWriteInfo(BlockStoreXform &store, TSIOBuffer buff, TSIOBufferReader rdr, int blk) : 
      _writeXform(store.shared_from_this()), 
      _ind(blk),
      _key(store._keysToWrite[blk].release()),
      _buff(buff),
      _rdr(rdr),
      _writeRef(store._writeCheck),
      _blkid(store._ctxt.firstIndex() + blk)
{
  DEBUG_LOG("ctor 1<<%d xform active:%ld writes:%ld",  
      _blkid, _writeXform.use_count(), _writeXform->write_count());
}

BlockWriteInfo::~BlockWriteInfo() {
  DEBUG_LOG("dtor 1<<%d xform active:%ld writes:%ld",  
      _blkid, _writeXform.use_count(), _writeXform->write_count());
}

TSEvent
BlockWriteInfo::handleBlockWrite(TSCont cont, TSEvent event, void *edata)
{
  ThreadTxnID txnid(_txnid);

  if (!edata) {
    DEBUG_LOG("empty edata event e#%d", event);
    return TS_EVENT_ERROR; // done
  }

  switch (event) {
    case TS_EVENT_CACHE_OPEN_WRITE_FAILED:
    {
      auto verr = -reinterpret_cast<intptr_t>(edata);
      auto vconn = static_cast<TSVConn>(edata);
      _vconn = ATSVConnFuture(vconn); // replace with the error value
      // async result was not found in time..
      auto r = TSContSchedule(cont, 0, TS_THREAD_POOL_TASK); // XXX: deadlocks if not immediate!?!
      DEBUG_LOG("scheduled reopen write check [%s] to 1<<%d nwrites:%ld", InkStrerror(verr), _blkid, _writeXform->write_count());
      break;
    }

    case TS_EVENT_IMMEDIATE:
    {
      DEBUG_LOG("restarted open write with 1<<%d nwrites:%ld",_blkid, _writeXform->write_count());
      auto r = TSCacheWrite(cont, _key.get()); // find room to store each key...
      if ( ! TSActionDone(r) ) {
        DEBUG_LOG("waiting on open write with 1<<%d nwrites:%ld",_blkid, _writeXform->write_count());
      }
      break;
    }

    case TS_EVENT_CACHE_OPEN_WRITE:
    {
      TSVConn vconn = static_cast<TSVConn>(edata);
      DEBUG_LOG("completed open write with 1<<%d nwrites:%ld",_blkid, _writeXform->write_count());
      _vconn = ATSVConnFuture(vconn);
      auto wrlen = std::min(TSIOBufferReaderAvail(_rdr.get()),(1L<<20));
      TSVConnWrite(vconn, cont, _rdr.get(), wrlen); // copy older buffer bytes out
      break;
    }

    case TS_EVENT_VCONN_WRITE_READY:
    {
      TSVIO vio = static_cast<TSVIO>(edata);
      // didn't detect enough bytes in buffer to complete?
      TSIOBufferReader writeRdr = TSVIOReaderGet(vio);
      if (! TSIOBufferReaderAvail(writeRdr)) {
        DEBUG_LOG("cache-write 1<<%d flush empty e#%d -> %ld? %ld?", _blkid, event, TSVIONDoneGet(vio), TSVIONBytesGet(vio));
        break; // surprising!
      }
      DEBUG_LOG("cache-write 1<<%d flush e#%d -> %ld? %ld?", _blkid, event, TSVIONDoneGet(vio), TSVIONBytesGet(vio));
      TSVIOReenable(vio);
      break;
    }

    case TS_EVENT_VCONN_WRITE_COMPLETE:
    {
      TSVIO vio = static_cast<TSVIO>(edata);
      BlockStoreXform &store = *_writeXform; // doesn't change

      _writeRef.reset(); // remove flag of write

      int wrcnt = store.write_count(); // there can be only one zero!
      auto vconnv = TSVIOVConnGet(vio);

      // attempt to close VConn here
      auto &flagEvt = _writeXform->_blockVIOUntil;
      auto oflag = TS_EVENT_CONTINUE;

      if ( ! wrcnt ) {
        flagEvt.compare_exchange_strong(oflag,TS_EVENT_NONE); // need un-blocking?
      }

      if ( oflag == TS_EVENT_CACHE_CLOSE ) {
        DEBUG_LOG("completed store with wakeup to 1<<%d nwrites:%d",_blkid,wrcnt);
//        TSVConnClose(vconnv); // flush and store
        TSContSchedule(store, 0, TS_THREAD_POOL_TASK); // wakeup!
      } else if ( flagEvt == TS_EVENT_CACHE_CLOSE ) {
        DEBUG_LOG("completed non-final store to 1<<%d nwrites:%d",_blkid,wrcnt);
      } else {
        DEBUG_LOG("completed store to 1<<%d nwrites:%d",_blkid,wrcnt);
//        TSVConnClose(vconnv); // flush and store
      }
//      _vconn.release();
      return TS_EVENT_NONE; // call dtor of ATSCont
    }

    default:
      DEBUG_LOG("unknown event: e#%d", event);
      break;
  }
  return TS_EVENT_CONTINUE; // re-use ATSCont with data block
}
