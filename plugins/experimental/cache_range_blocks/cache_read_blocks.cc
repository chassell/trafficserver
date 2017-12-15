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

#include "utils_internal.h"
#include <atscppapi/HttpStatus.h>
#include <ts/experimental.h>
#include <ts/InkErrno.h>

#include <chrono>
#include <algorithm>

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

using namespace atscppapi;

void
BlockSetAccess::launch_block_tests()
{
  // stateless callback as deleter...
  using Deleter_t                = void (*)(BlockSetAccess *);
  static const Deleter_t deleter = [](BlockSetAccess *ptr) {
    ptr->handle_block_tests(); // recurse from last one
  };

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockSetAccess>(this, deleter);
  auto blkNum      = _beginByte / _blkSize;

  for (auto i = 0U; i < _keysInRange.size(); ++i, ++blkNum) {
    // prep for async reads that init into VConn-futures
    _keysInRange[i] = std::move(ATSCacheKey(clientUrl(), _etagStr, blkNum * _blkSize));
    auto contp      = ATSCont::create_temp_tscont(_txnCont, _vcsToRead[i], barrierLock);
    TSCacheRead(contp, _keysInRange[i]);
  }
}


// start read of all the blocks
void
BlockSetAccess::handle_block_tests()
{
  using namespace std::chrono;
  using std::future_status;

  //
  // mutex-protected check
  //

  if (_storeXform || _readXform) {
    return; // transform has already begun
  }

  auto nrdy     = 0U;
  auto firstBlk = _beginByte / _blkSize;
  auto endBlk = (_endByte + _blkSize-1) / _blkSize; // round up
  auto finalBlk = _assetLen / _blkSize; // round down..

  // scan *all* keys and vconns to check if ready
  for (auto n = 0U; n < _vcsToRead.size(); ++n) {
    base64_bit_clr(_b64BlkBitset, firstBlk + n);

    if ( ! _keysInRange[n] ) {
      continue; // known present..
    }

    auto vconn    = _vcsToRead[n].get();

    if ( ! vconn ) {
      DEBUG_LOG("read isn't ready: 1<<%ld", firstBlk + n);
      continue;
    }

    auto blkLen = ( firstBlk + n == finalBlk ? ((_assetLen-1) % blockSize())+1 : blockSize() );

    // block is ready and of right size?
    if ( TSVConnCacheObjectSizeGet(vconn) != blkLen ) {
      // clear bit
      DEBUG_LOG("read returned wrong size: #%#lx", firstBlk + n);
      continue;
    }

    _keysInRange[n].reset(); // no need for cache-key

    // successful!
    ++nrdy;
    base64_bit_set(_b64BlkBitset, firstBlk + n); // set a bit
    DEBUG_LOG("read successful bitset: + 1<<%ld %s", firstBlk + n, _b64BlkBitset.c_str());
  }

  // ready to read from cache...
  if ( nrdy == _keysInRange.size() ) {
    start_cache_hit(_beginByte);
    return;
  }

  auto n = _vcsToRead.size();

  for( auto &&p : _vcsToRead ) {
    if ( ! p.is_close_able() ) {
      auto i = &p - &_vcsToRead.front();
      DEBUG_LOG("late cache-read entry: #%ld",i);
      break;
    }
    --n;
  }

  // best to do now?
  if ( ! n ) {
    _vcsToRead.clear(); // deallocate and close now...
  }

  start_cache_miss(firstBlk, endBlk);
}

void
BlockSetAccess::start_cache_hit(int64_t rangeStart)
{
  _readXform = std::make_unique<BlockReadXform>(*this, rangeStart);
  _readXform->handleReadCacheLookupComplete(txn());  // may have txn.resume()
}

BlockReadXform::BlockReadXform(BlockSetAccess &ctxt, int64_t start)
  : ATSXformCont(ctxt.txn(), TS_HTTP_RESPONSE_TRANSFORM_HOOK, 0), // zero bytes length, zero bytes offset...
    _ctxt(ctxt),
    _startSkip(start % ctxt.blockSize()),
    _vconns(ctxt._vcsToRead),
    _readEvents(*this, &BlockReadXform::handleRead, nullptr, *this) // shared mutex
{
  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(), 0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(), 0);
}

BlockReadXform::~BlockReadXform() { }

void
BlockReadXform::handleReadCacheLookupComplete(Transaction &txn)
{
  launch_block_reads();
  _ctxt.set_cache_hit_bitset();
  txn.resume();
}

void
BlockReadXform::launch_block_reads()
{
  handleRead(TS_EVENT_VCONN_READ_COMPLETE, nullptr, nullptr);

  // handler after body is being written...
  auto blockBodyHandler = [this](TSEvent event, TSVIO vio, int64_t left) {
    auto avail = TSIOBufferReaderAvail(*_xformOutU);

    if ( avail || event != TS_EVENT_VCONN_WRITE_READY) {
<<<<<<< Updated upstream
      TSVIOReenable(*_xformOutU); // retried by the write op
      return 0L;
    }

    DEBUG_LOG("read write-vio flush-wait: %#lx+%#lx >= %#lx", TSVIONDoneGet(*_xformOutU), avail, _startSkip);
    _xformOutWaiting = event;
=======
      TSVIOReenable(outputVIO()); // handle complete as well
      return 0L;
    }

    DEBUG_LOG("read write-vio flush-wait: %#lx+%#lx >= %#lx", TSVIONDoneGet(outputVIO()), avail, _startSkip);
    _bodyCopyVIOWaiting = event;
>>>>>>> Stashed changes
    return 0L;
  };

  // initial handler ....
  set_body_handler([this,blockBodyHandler](TSEvent event, TSVIO vio, int64_t left) {
    auto avail = TSIOBufferReaderAvail(outputReader());

    _xformOutWaiting = event; // save the event...

<<<<<<< Updated upstream
    if ( _xformOutU || avail < _startSkip ) {
=======
    if ( avail < 

    if ( outputVIO() || avail < _startSkip ) {
      TSIOBufferReaderConsume(outputReader(), _startSkip);
>>>>>>> Stashed changes
      return 0L; // already requested callback...
    }

    // no VIO and enough is available...
<<<<<<< Updated upstream
    DEBUG_LOG("read -> body vio begin: %#lx: %#lx-%#lx [%p]", avail, _startSkip, _startSkip + _ctxt.rangeLen(), _bodyCopyVIO);
=======
    DEBUG_LOG("read -> body vio begin: %#lx: %#lx-%#lx [%p]", avail, _startSkip, _startSkip + _ctxt.rangeLen(), outputVIO());
>>>>>>> Stashed changes

    TSIOBufferReaderConsume(outputReader(), _startSkip);
    _xformOutU = ATSXformOutVConn::create_if_ready(*this,_ctxt.rangeLen());

    if ( _xformOutU ) {
      set_body_handler(blockBodyHandler); // NOTE: destructs active lambda
    }
    return 0L;
  });

  // do not resume until first block is read in
}

void
BlockReadXform::handleRead(TSEvent event, void *edata, std::nullptr_t)
{
  TSVIO vio = static_cast<TSVIO>(edata);

  switch ( event ) 
  {
    case TS_EVENT_VCONN_READ_READY: // failure!
      DEBUG_LOG("read ready was blocked: e#%d", event);
      return; /// RETURN

    default:
      DEBUG_LOG("read unkn event: e#%d", event);
      return; /// RETURN

    case TS_EVENT_VCONN_READ_COMPLETE:
<<<<<<< Updated upstream
      if ( vio && TSVIOVConnGet(vio) ) {
        DEBUG_LOG("waiting on close of cache-read: e#%d", event);
      }
=======
      DEBUG_LOG("close desired for cache-read: e#%d", event);
>>>>>>> Stashed changes
      break; /// ....
  }

  // first nonzero vconn ... or past end..
  auto nxt = std::find_if(_vconns.begin(), _vconns.end(), [](ATSVConnFuture &vc) { return vc.get() != nullptr; } ) - _vconns.begin();

  auto blkSize = _ctxt.blockSize();
  auto avail   = TSIOBufferReaderAvail(outputReader()); // last ready byte in buffer..
  auto posin   = nxt * blkSize; // last ready byte in buffer..
  auto endout  = _startSkip + _ctxt.rangeLen();

  // start the transform write?
  if ( ! _bodyCopyVIO && output() && avail >= _startSkip ) {
    DEBUG_LOG("write-body start: %#lx-%#lx endblk#%#lx", _startSkip, _ctxt.rangeLen(), nxt);
    // start the write up correctly now..
    _bodyCopyVIO = TSVConnWrite(output(), _readEvents, outputReader(), _ctxt.rangeLen());
    _bodyCopyVIOWaiting = TS_EVENT_VCONN_WRITE_READY;
  }
  
  if ( posin > endout ) {
    DEBUG_LOG("ignoring event.  completed all reads: e#%d",event);
    return; // no reads left to start!
  }

  if ( event == TS_EVENT_VCONN_WRITE_READY ) {
    DEBUG_LOG("explicit write-body start completed: %#lx-%#lx endblk#%#lx", _startSkip, _ctxt.rangeLen(), nxt);
    return; // cannot do more ...
  }

  auto nextRd    = nxt * blkSize; // next read-pos
  auto nextRdMax = std::min(nextRd + blkSize, endout);

  DEBUG_LOG("read start: %#lx-%#lx #%#lx", nextRd, nextRdMax, nxt);

  _cacheRdVIO = TSVConnRead(_vconns[nxt].get(), _readEvents, outputBuffer(), nextRdMax - nextRd);
  _vconns[nxt].reset();
}

void
BlockSetAccess::set_cache_hit_bitset()
{
  // HIT_FRESH is the case currently...

  Headers cachedHdr;
  TSMBuffer bufp = nullptr;
  TSMLoc offset = nullptr;

  // no need to free these 
  TSHttpTxnCachedRespModifiableGet(atsTxn(), &bufp, &offset);

  cachedHdr.reset(bufp,offset); 
  //  cachhdrs.erase(CONTENT_RANGE_TAG); // erase to remove concerns
  cachedHdr.erase(X_BLOCK_BITSET_TAG); // attempt to erase/rewrite field in headers
  cachedHdr.append(X_BLOCK_BITSET_TAG, b64BlkBitset()); // attempt to erase/rewrite field in headers

  auto r = TSHttpTxnUpdateCachedObject(atsTxn());

  // re-read everything ...
  cachedHdr.reset(bufp,offset);

  if ( r == TS_SUCCESS ) {
    DEBUG_LOG("updated bitset: %s", b64BlkBitset().c_str());
    DEBUG_LOG("updated cache-hdrs:\n-----\n%s\n------\n", cachedHdr.wireStr().c_str());
  } else if ( r == TS_ERROR ) {
    DEBUG_LOG("failed to update cache-hdrs:\n-----\n%s\n------\n", cachedHdr.wireStr().c_str());
  }
}

