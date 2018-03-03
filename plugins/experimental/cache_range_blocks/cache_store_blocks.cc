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
#define BLOCK_COUNT_MINIMUM 50
#define STUB_MAX_BYTES 1024

BlockStoreXform::Ptr_t
BlockStoreXform::start_cache_miss(BlockSetAccess &ctxt, TSHttpTxn txn, int64_t offset)
{
  TSHttpTxnCacheLookupStatusSet(txn, TS_CACHE_LOOKUP_MISS);
  ctxt.registerHook(BlockSetAccess::HOOK_READ_RESPONSE_HEADERS);

  return std::make_shared<BlockStoreXform>(ctxt, offset);
}

BlockStoreXform::BlockStoreXform(BlockSetAccess &ctxt, int offset)
  : BlockTeeXform(ctxt.txn(), 
                  [this](TSIOBufferReader r, int64_t inpos, int64_t len, int64_t added) { this->handleBodyRead(r, inpos, len, added); },
                  ctxt.contentLen(), 
                  offset), // offset may be *negative*...
    _ctxt(ctxt)
{
  DEBUG_LOG("construct start: offset %d", offset);
  reset_write_keys();
}

BlockStoreXform::~BlockStoreXform()
{
  DEBUG_LOG("destruct start: nwrites:%ld", write_count());
}

void
BlockSetAccess::handleReadResponseHeaders(Transaction &txn) 
{
  ThreadTxnID txnid{txn};

  auto &resp = txn.getServerResponse();
  auto &respHdrs = resp.getHeaders();

  auto srvrRange = respHdrs.value(CONTENT_RANGE_TAG); // not in clnt hdrs
  auto bodyLen = respHdrs.value(CONTENT_LENGTH_TAG); // not in clnt hdrs
  auto ilen = srvrRange.find('/');
  auto iend = srvrRange.rfind('-',ilen);
  auto ibeg = srvrRange.rfind(' ',iend);
  ilen = ( ilen != srvrRange.npos ? ilen + 1 : srvrRange.size() ); // zero-length c-string if no '/' found
  iend = ( iend != srvrRange.npos ? iend + 1 : srvrRange.size() ); // zero-length c-string if no '/' found
  ibeg = ( ibeg != srvrRange.npos ? ibeg + 1 : srvrRange.size() ); // zero-length c-string if no '/' found

  auto bodyBytes = std::atol( bodyLen.c_str() );

  _srvrBeginByte = std::atol( srvrRange.c_str() + ibeg);
  _srvrEndByte = std::atol( srvrRange.c_str() + iend) + 1;

  auto srvrAssetLen = std::atol( srvrRange.c_str() + ilen);

  // happens if not a 206...
  if ( srvrAssetLen <= 0 )
  {
    DEBUG_LOG("rejecting due to unusable length: %s",srvrRange.c_str());
    // _storeXform.reset(); // cannot perform transform!
    txn.resume();
    return; // no transform enabled
  }

  TSHttpTxnServerRespNoStoreSet(atsTxn(),1); // assume blocks only first...

  // orig client requested bytes-from-end-of-stream
  if ( _beginByte < 0 ) {
    _beginByte = srvrAssetLen + _beginByte;
    _endByte = srvrAssetLen;
  }

  // orig client requested end-of-stream
  if ( ! _endByte ) {
    _endByte = srvrAssetLen;
  }

  if ( _srvrBeginByte + bodyBytes != _srvrEndByte || _srvrBeginByte > _beginByte ) {
    DEBUG_LOG("rejecting due to unusable response: sbeg=%ld send=%ld cl=%ld beg=%ld range=%s / length=%s",
              _srvrBeginByte, _srvrEndByte, bodyBytes, _beginByte,
             srvrRange.c_str(),bodyLen.c_str());
    // _storeXform.reset(); // cannot perform transform!
    txn.resume();
    return; // no transform enabled
  }

  // server range is valid and should cover original request...

  if ( _endByte > _srvrEndByte ) 
  {
    _endByte = _srvrEndByte; // match position of end byte...
    DEBUG_LOG("confused due to differences: range=%s / length=%s",srvrRange.c_str(),bodyLen.c_str());
  }

  // adjusted so ranges overlap ...
  auto ofirstInd = firstIndex();

  // round to first of usable blocks...
  _firstInd = (_srvrBeginByte + _blkSize-1)/_blkSize;  // round up
  // round to first after usable blocks (w/end block always usable)
  _endInd = ( _srvrEndByte <  srvrAssetLen ? _srvrEndByte/_blkSize : endIndex() ); // round down (non-ending)

  // corrected estimate of all aspects 
  auto currETag = respHdrs.value(ETAG_TAG);

  if (srvrAssetLen != _assetLen || currETag != _etagStr) 
  {
    _assetLen = srvrAssetLen;
    _etagStr = currETag;

    _keysInRange.clear(); // discard old ones!

    // XXX: delete obsolete blocks if etag changed??
    // first ever response about this asset...

    DEBUG_LOG("stub-reset: len=%#lx olen=%#lx (%ld-%ld) final=%s etag=%s", srvrAssetLen, _assetLen, _beginByte, _endByte, srvrRange.c_str(), currETag.c_str());
    // new stub needed!

    _storeXform->new_asset_body(txn);
    txn.resume(); // continue with expected transform
    return; // no transform enabled
  }

  // was response not block-aligned?
  if ( ofirstInd != _firstInd )
  {
    // must reset list of blocks to match..
    reset_range_keys();
    _storeXform->reset_write_keys(); // all new
    DEBUG_LOG("srvr-resp: reset len=%#lx (%ld-%ld) final=%s etag=%s", _assetLen, _beginByte, _endByte, srvrRange.c_str(), _etagStr.c_str());
  } else {
    // normal case with valid stub file in place?
    DEBUG_LOG("srvr-resp: aligned len=%#lx (%ld-%ld) final=%s etag=%s", _assetLen, _beginByte, _endByte, srvrRange.c_str(), _etagStr.c_str());
  }

  // discover correct values...
  _storeXform->init_enabled_transform();
  txn.resume();
}


void
BlockStoreXform::new_asset_body(Transaction &txn) 
{
  auto &resp = txn.getServerResponse();
  auto &respHdrs = resp.getHeaders();
  auto atsTxn = _ctxt.atsTxn();

  auto contentEnc = respHdrs.values(CONTENT_ENCODING_TAG);
  auto storeBytes = _ctxt.contentLen();
  auto minBlockAsset = BLOCK_COUNT_MINIMUM * _ctxt.blockSize();
  auto maxBytes = _ctxt.assetLen();

  if ( ! contentEnc.empty() && contentEnc != "identity" )
  {
    DEBUG_LOG("rejecting due to unusable encoding: %s",contentEnc.c_str());
    return; // no transform enabled
  }

  // store whole small file?  
  if ( maxBytes <= minBlockAsset && storeBytes == maxBytes ) 
  {
    // respond as if a if-range passed the whole file ...
    resp.setStatusCode(HTTP_STATUS_OK);
    TSHttpTxnServerRespNoStoreSet(atsTxn,0); // must assume now we need to save these headers!
    respHdrs.erase(CONTENT_RANGE_TAG); // as if it wasn't a 206 ever...
    DEBUG_LOG("cache-store of whole 206:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
    return; // no transform enabled
  }

  // too small for use of blocks??
  if ( maxBytes <= minBlockAsset )
  {
    // 206 on through... but spawn a simple 'background' fetch...
    DEBUG_LOG("no-store of small 206:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
    auto pluginvc = spawn_sub_range(txn,0,0);
    ATSCont::create_temp_tscont(pluginvc, _subRangeVIO, nullptr); // create a VConnRead that is freed later
    DEBUG_LOG("no-store of small 206");
    return; // no transform enabled
  }

  // big files only ....

  // knowing the etag/size means it's time to reset...
  _ctxt.reset_range_keys();
  reset_write_keys();

  reset_output_length(storeBytes);

  TSHttpTxnUntransformedRespCache(atsTxn,0); // don't store untransformed!

  if ( storeBytes <= STUB_MAX_BYTES ) {
    TSHttpTxnServerRespNoStoreSet(atsTxn,0); // allow storage
    TSHttpTxnTransformedRespCache(atsTxn,1);   // store transformed!

    // respond as if a if-range failed...
    resp.setStatusCode(HTTP_STATUS_OK);

    respHdrs.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
    respHdrs.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please check for a stub!

    DEBUG_LOG("store block-storage stub:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
  } else {
    // 206 on through... but spawn a real stub request ... 
    DEBUG_LOG("stub-create recurse pre:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
    auto pluginvc = spawn_sub_range(txn,1,2); // stub gets one byte range ...
    ATSCont::create_temp_tscont(pluginvc, _subRangeVIO, nullptr); // create a VConnRead that is freed later
    DEBUG_LOG("stub-create recurse post");
  }

  init_enabled_transform();
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
  for ( ; blk < static_cast<int>(keys.size()) && keyPos <= readypos ; (keyPos += blksz),++blk ) 
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
BlockStoreXform::handleBodyRead(int64_t odonepos, int64_t oreadypos)
{
  ThreadTxnID txnid{_txnid};

  auto teeRange = std::make_pair(odonepos,oreadypos);
  auto &donepos = teeRange.first;
  auto &readypos = teeRange.second;

  if ( ! donepos && added ) {
    auto keepRef = shared_from_this();
    _wakeupCont = [keepRef](TSEvent, void *) { 
        atscppapi::ScopedContinuationLock lock(*keepRef);
        auto keepRef2 = keepRef;
        // mutex may be only part left..
        if ( keepRef && keepRef2 && keepRef.use_count() > 1 ) {
          keepRef->_blockVIOUntil = TS_EVENT_NONE; // allow ending/re-wakeup now...
          keepRef->teeReenable();
        }
    };
  }

  if ( donepos == readypos && ! added ) {
    return;
  }

  auto firstBlk = _ctxt.firstIndex();
  auto blksz        = static_cast<int64_t>(_ctxt.blockSize());
  auto endpos = TSVIONBytesGet(inputVIO()); // expected final input byte
  auto newBlockEvent = _blockVIOUntil.load(); // atomic grab
  auto prevBlockEvent = newBlockEvent;

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
    next_valid_vconn(donepos, readypos, skipDist);

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
    ATSCacheKey takeKey{ _keysToWrite[blk].release() }; // clear the key...
    auto blockWritePtr = std::make_shared<BlockWriteInfo>(*this, takeKey, blk); // takes key
    auto &blkWrite = *blockWritePtr;

    // direct new to allow self-delete
    auto blockCont = (new ATSCont(&BlockWriteInfo::handleBlockWriteCB, 
                                   blkWrite.shared_from_this()))
                 ->get(); // get the TSCont

    // activate the CacheVC with reenable
    TSVConnWrite(blkWrite, blockCont, TSIOBufferReaderClone(teerdr), wrlen);

    TSIOBufferReaderConsume(teerdr, wrlen);

    // use new tee buffers with remainder of output

    // notify quickly if we need a new wakeup...
    if ( wrcnt >= WRITE_COUNT_LIMIT ) {
      prevBlockEvent = TS_EVENT_CACHE_CLOSE;
      newBlockEvent = TS_EVENT_CACHE_CLOSE;
      DEBUG_LOG("store ++++ beginning write with wakeup 1<<%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               _ctxt.firstIndex() + blk, write_count(), donepos, navail, donepos + wrlen);
      _blockVIOUntil.compare_exchange_strong(prevBlockEvent,newBlockEvent); // need un-blocking?
    } 

    // Did write fail totally?
    auto desc = "";
//    if ( ! TSActionDone(r) ) {
//      desc = "delayed";
//    } else 
    if ( callDatap.use_count() > 1 ) {
      newBlockEvent = prevBlockEvent; // no wakeup
      desc = "started"; // didn't get WRITE_COMPLETE yet
    } else {
      newBlockEvent = prevBlockEvent; // no wakeup
      desc = "completed";
    }

//    DEBUG_LOG("store ++++ %s write 1<<%ld [%s] nwrites:%ld @%#lx+%#lx [final @%#lx]", 
    DEBUG_LOG("store ++++ %s write 1<<%ld active:%ld nwrites:%ld @%#lx+%#lx [final @%#lx]", 
               desc, 
               _ctxt.firstIndex() + blk, 
               callData.ref_count()-1,
               write_count(), donepos, navail, donepos + wrlen);
  }

  // if no input events left...
  if ( readypos >= endpos && donepos < endpos ) {
    newBlockEvent = TS_EVENT_CACHE_CLOSE; // always request wakeup
  }

  if ( prevBlockEvent != newBlockEvent ) {
    DEBUG_LOG("store **** nwrites:%ld wakeup-blocked at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
    _blockVIOUntil.compare_exchange_strong(prevBlockEvent,newBlockEvent); // need un-blocking?
    return; // new wakeup..
  }

  if ( newBlockEvent ) {
    DEBUG_LOG("store **** nwrites:%ld still wakeup-blocked at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
    return; // waiting wakeup
  } 
  
  if ( readypos < endpos || donepos < endpos ) {
    return; // tee-buffer is not empty...
  }

  DEBUG_LOG("store **** nwrites:%ld all blocks completed at current block 1<<%ld pos:%#lx+%#lx", write_count(), firstBlk + donepos/blksz, donepos, readypos - donepos);
  // no bytes left ...
  _wakeupCont = [](TSEvent, void *) { }; // last reference goes now
}

BlockWriteInfo::BlockWriteInfo(BlockStoreXform &store, ATSCacheKey &key, int blk) :
      ProxyVConn{ [this](TSCont cont) -> TSAction { return TSCacheWrite(cont,this->_key); } },
      _writeXform{store.shared_from_this()}, 
      _ind(blk),
      _key(std::move(key)),
      _blkid(store._ctxt.firstIndex() + blk)
{
  DEBUG_LOG("ctor 1<<%d xform active:%ld nwrites:%ld",  
      _blkid, _writeXform.use_count(), _writeXform->write_count());
  TSIOBufferWaterMarkSet(data(), (1<<20)); // single write only...
}

BlockWriteInfo::~BlockWriteInfo() {
  _writeRef.reset();
  DEBUG_LOG("dtor 1<<%d xform left active:%ld nwrites:%ld",  
      _blkid, _writeXform.use_count()-1, _writeXform->write_count());
}

TSEvent
BlockWriteInfo::handleBlockWrite(TSCont cont, TSEvent event, void *edata)
{
  ThreadTxnID txnid(_txnid);
  auto mutex = TSContMutexGet(cont);

  // *reset* value (destruct on old value!)
  BlockStoreXform &store = *_writeXform; // doesn't change
  auto wakeupCont = store._wakeupCont.get();

  _writeRef.reset(); // remove flag of write

  int wrcnt = store.write_count(); // there can be only one zero!

  // attempt to close VConn here
  auto &flagEvt = _writeXform->_blockVIOUntil;

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
  auto oflag = TS_EVENT_CACHE_CLOSE; // flag 
  flagEvt.compare_exchange_strong(oflag,TS_EVENT_VCONN_WRITE_READY); // prevent end ... but claim wakeup

  // did not try to grab... but would have succeeded
   
  if ( oflag == TS_EVENT_NONE ) {
    DEBUG_LOG("completed final store to 1<<%d nwrites:0",_blkid);
    return TS_EVENT_NONE;
  }

  if ( oflag != TS_EVENT_CACHE_CLOSE ) {
    DEBUG_LOG("completed final store to 1<<%d nwrites:0 (flag:%d)",_blkid,oflag);
    return TS_EVENT_NONE;
  }

  // wakeup is claimed by this call ...

  // (avoid deadlock with my own mutex!!)
  if ( TSMutexLockTry(mutex) ) {
    TSMutexUnlock(mutex); 
    TSMutexUnlock(mutex); // unlock enclosing (or just a noop)
  }

  atscppapi::ScopedContinuationLock lock(wakeupCont);
  DEBUG_LOG("completed store with wakeup to 1<<%d nwrites:%d",_blkid,wrcnt);
  TSContSchedule(wakeupCont, 0, TS_THREAD_POOL_TASK);

  return TS_EVENT_NONE; // call dtor of ATSCont
}
