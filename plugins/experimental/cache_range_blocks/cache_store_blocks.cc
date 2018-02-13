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
BlockStoreXform::next_valid_vconn(int64_t donepos, int64_t readypos, int64_t &skip)
{
  auto blksz = static_cast<int64_t>(_ctxt.blockSize());
  auto firstBlk = _ctxt.firstIndex();

  skip = -1;

  // find distance to next start point (w/zero possible)

  // amt past boundary (maybe full block)
  auto fwdDist = ((donepos + blksz - 1) % blksz) + 1; // between 1 and blksz
  auto keyPos = donepos - fwdDist + blksz; // (can be same as donepos)
  auto &keys = _keysToWrite;
  auto blk = keyPos / blksz;

  // find keyPos within this new span ... and before end
  for ( ; keyPos <= readypos ; (keyPos += blksz),++blk ) 
  {
    if ( keys[blk] ) {
      skip = keyPos - donepos; // report if a skip is needed...
      return keys[blk].get();
    }

    // skip this...
    DEBUG_LOG("unstored VConn future 1<<%ld pos:[%#lx-%#lx) no-key:%#lx", blk + firstBlk, donepos, readypos, keyPos);
  }

  // no boundary within distance?
  skip = readypos - donepos;
  DEBUG_LOG("discard current [%#lx-%#lx) [%#lx]", donepos, readypos, keyPos);
  return nullptr;
}


//
// called from writeHook
//      -- after outputBuffer() was filled
void
BlockStoreXform::handleBodyRead(TSIOBufferReader teerdr, int64_t odonepos, int64_t oreadypos, int64_t added)
{
  // input has closed down?
  if (!teerdr) {
    DEBUG_LOG("store **** final call");
    return;
  }

  auto teeRange = std::make_pair(odonepos,oreadypos);
  auto &donepos = teeRange.first;
  auto &readypos = teeRange.second;

  if ( ! donepos && added ) {
    auto newRef = shared_from_this();
    _wakeupCont = [newRef](TSEvent, void *) { newRef->teeReenable(); };
  }

  if ( donepos == readypos && ! added ) {
    return;
  }

  auto firstBlk = _ctxt.firstIndex();
  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());
  auto endpos = TSVIONBytesGet(inputVIO()); // expected final input byte
  auto newBlockEvent = _blockVIOUntil.load(); // atomic grab
  auto prevBlockEvent = newBlockEvent;

  // force clean if okay...
  if ( write_count() < WRITE_COUNT_LIMIT ) {
    _blockVIOUntil = TS_EVENT_NONE; // need more input only...
  }

  // update positions (aliases) on each loop
  for ( ; donepos < readypos && ! newBlockEvent ; teeRange = teeAvail() ) 
  {
    auto navail = readypos - donepos;
    auto blk = static_cast<int>(donepos / blksz);
    newBlockEvent = TS_EVENT_CACHE_CLOSE; // expect wakeup request first
    auto wrcnt = write_count();

    if ( wrcnt >= WRITE_COUNT_LIMIT ) {
      continue;                                           //// CONTINUE (need wakeup)
    }

    // check any unneeded bytes before next block
    int64_t skipDist = 0;
    auto currKey = next_valid_vconn(donepos, readypos, skipDist);

    if (skipDist > 0) {
      newBlockEvent = TS_EVENT_NONE;  // no wakeup needed just yet
      TSIOBufferReaderConsume(teerdr, skipDist); // skipped
      DEBUG_LOG("store **** skipping bytes @[%#lx-%#lx) --> skipped %#lx", donepos + skipDist, readypos, skipDist);
      continue;                                           //// CONTINUE (better aligned)
    }

    // aligned to block bound

    auto wrlen = std::min(endpos - donepos,blksz); // len needed for a block write...

    if ( navail < wrlen && ! added ) {
      DEBUG_LOG("store **** nothing read in pos:%#lx+%#lx [<= +%#lx]", donepos, navail, wrlen);
      return; // no progress                              /// RETURN (need input)
    }

    if ( navail < wrlen ) {
      DEBUG_LOG("store **** reading in pos:%#lx+%#lx [<= +%#lx]", donepos, navail, wrlen);
      return; // incomplete                               /// RETURN (need input)
    }

    // going to give ownership of current teereader / teebuffer
    ++wrcnt; // add write count
    auto takeKey( std::move(_keysToWrite[blk]) ); // clear the key...
    auto callDatap = std::make_shared<BlockWriteInfo>(*this, takeKey, blk); // takes key
    auto &callData = *callDatap;

    TSIOBufferCopy(callData._buff.get(), teerdr, wrlen, 0); // split off data for storage
    TSIOBufferReaderConsume(teerdr, wrlen);

    // direct new to allow self-delete
    auto blockCont = (new ATSCont(&BlockWriteInfo::handleBlockWriteCB, 
                                   callDatap->shared_from_this()))
                 ->get(); // get the TSCont

    // use new tee buffers with remainder of output

    // notify quickly if we need a new wakeup...
    if ( wrcnt >= WRITE_COUNT_LIMIT ) {
      prevBlockEvent = TS_EVENT_CACHE_CLOSE;
      newBlockEvent = TS_EVENT_CACHE_CLOSE;
      DEBUG_LOG("store ++++ beginning write with wakeup 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
      _blockVIOUntil.compare_exchange_strong(prevBlockEvent,newBlockEvent); // need un-blocking?
    } 

    auto r = TSCacheWrite(blockCont, currKey); // find room to store each key...

    // Did write fail totally?
    auto desc = "";
    auto err = callData._vconn.error();
    if ( err > ESOCK_TIMEOUT ) {
      desc = "failed";
    } else if ( ! TSActionDone(r) ) {
      desc = "delayed";
    } else if ( callDatap.use_count() > 1 ) {
      newBlockEvent = prevBlockEvent; // no wakeup
      desc = "started"; // didn't get WRITE_COMPLETE yet
    } else {
      newBlockEvent = prevBlockEvent; // no wakeup
      desc = "completed";
    }

    DEBUG_LOG("store ++++ %s write 1<<%ld [%s] nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               desc, _ctxt.firstIndex() + blk, InkStrerror(callData._vconn.error()),
               write_count(), donepos, navail, donepos + wrlen);
  }

  // at or past the end?
  if ( readypos >= endpos && donepos >= endpos ) {
    _wakeupCont = [](TSEvent, void *) { }; // last reference goes now
    return;                                          //// RETURN
  } 
  
  // if no input events left...
  if ( readypos >= endpos ) {
    newBlockEvent = TS_EVENT_CACHE_CLOSE; // always request wakeup
  }

  if ( prevBlockEvent != newBlockEvent ) {
    DEBUG_LOG("store **** nwrites:%ld wakeup-blocked at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
    _blockVIOUntil.compare_exchange_strong(prevBlockEvent,newBlockEvent); // need un-blocking?
  } else if ( newBlockEvent ) {
    DEBUG_LOG("store **** nwrites:%ld still blocked at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
  }
}

BlockWriteInfo::BlockWriteInfo(BlockStoreXform &store, ATSCacheKey &key, int blk) : 
      _writeXform{store.shared_from_this()}, 
      _ind(blk),
      _key(std::move(key)),
      _buff{TSIOBufferCreate()},
      _rdr{TSIOBufferReaderAlloc(_buff.get())},
      _writeRef{store._writeCheck},
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
  auto mutex = TSContMutexGet(cont);

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
      atscppapi::ScopedContinuationLock lock(cont);
      DEBUG_LOG("scheduled reopen write check [%s] to 1<<%d nwrites:%ld", InkStrerror(verr), _blkid, _writeXform->write_count());
      TSContSchedule(cont, 30, TS_THREAD_POOL_TASK);
      break;
    }

    case TS_EVENT_TIMEOUT:
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
        BlockStoreXform &store = *_writeXform; // doesn't change
        auto wakeupCont = store._wakeupCont.get();

        _writeRef.reset(); // remove flag of write

        int wrcnt = store.write_count(); // there can be only one zero!

        // attempt to close VConn here
        auto &flagEvt = _writeXform->_blockVIOUntil;

        TSVConnClose(_vconn.release());
        // replace with number to recognize (not null)
        _vconn = ATSVConnFuture(reinterpret_cast<TSVConn>(-ESTALE));

        // don't try but note 
        if ( wrcnt && flagEvt == TS_EVENT_CACHE_CLOSE ) {
          DEBUG_LOG("completed non-final store to 1<<%d nwrites:%d",_blkid,wrcnt);
          return TS_EVENT_NONE;
        }

        if ( wrcnt ) {
          DEBUG_LOG("completed store to 1<<%d nwrites:%d",_blkid,wrcnt);
          return TS_EVENT_NONE;
        }
        
        // ask to change CACHE_CLOSE to NONE .. or change only if CONTINUE (never) ...
        auto oflag = ( ! wrcnt ? TS_EVENT_CACHE_CLOSE : TS_EVENT_CONTINUE ); // flag 
        flagEvt.compare_exchange_strong(oflag,TS_EVENT_NONE); // need un-blocking?

        // did not try to grab... but would have succeeded
         
        if ( oflag != TS_EVENT_CACHE_CLOSE ) {
          DEBUG_LOG("completed final store to 1<<%d nwrites:0",_blkid);
          return TS_EVENT_NONE;
        }

        // (avoid deadlock)
        if ( TSMutexLockTry(mutex) ) {
          TSMutexUnlock(mutex); 
          TSMutexUnlock(mutex); // unlock enclosing (or noop)
        }

        atscppapi::ScopedContinuationLock lock(wakeupCont);
        DEBUG_LOG("completed store with wakeup to 1<<%d nwrites:%d",_blkid,wrcnt);
        TSContSchedule(wakeupCont, 0, TS_THREAD_POOL_TASK);

        return TS_EVENT_NONE; // call dtor of ATSCont
      }

    default:
      DEBUG_LOG("unknown event: e#%d", event);
      break;
  }
  return TS_EVENT_CONTINUE; // re-use ATSCont with data block
}
