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
  auto mutex       = TSContMutexGet(_txnCont); // single mutex for all reads
  auto blkNum      = _beginByte / _blkSize;

  for (auto i = 0U; i < _keysInRange.size(); ++i, ++blkNum) {
    // prep for async reads that init into VConn-futures
    _keysInRange[i] = std::move(APICacheKey(clientUrl(), _etagStr, blkNum * _blkSize));
    auto contp      = APICont::create_temp_tscont(mutex, _vcsToRead[i], barrierLock);
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

    if (_vcsToRead[n].wait_for(seconds::zero()) != future_status::ready) {
      DEBUG_LOG("read isn't ready: 1<<%ld", firstBlk + n);
      continue;
    }

    auto vconn    = _vcsToRead[n].get();
    auto vconnErr = -reinterpret_cast<intptr_t>(vconn); // block isn't ready
    // valid pointers don't look like this
    if (!vconnErr || (vconnErr >= CACHE_ERRNO && vconnErr < EHTTP_ERROR)) {
      // clear bit
      DEBUG_LOG("read returned non-pointer: 1<<%ld == %ld: %s", firstBlk + n, vconnErr, _b64BlkBitset.c_str());
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

  atscppapi::ScopedContinuationLock lock(_txnCont);

  for( auto &&p : _vcsToRead ) {
    auto i = &p - &_vcsToRead.front();
    if ( p.valid() && p.wait_for(seconds::zero()) == future_status::ready ) {
      auto ptrErr = reinterpret_cast<intptr_t>(p.get());
      if ( ptrErr < -INK_START_ERRNO - 1000 || ptrErr > 0 ) {
        TSVConnClose(p.get());
        DEBUG_LOG("closed successful cache-read: #%ld %p",i,p.get());
      } else {
        DEBUG_LOG("pass failed cache-read: #%ld %p",i,p.get());
      }
    } else {
      DEBUG_LOG("toss invalid cache-read: #%ld",i);
    }
    p = std::shared_future<TSVConn>(); // make invalid
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
  : APIXformCont(ctxt.txn(), TS_HTTP_RESPONSE_TRANSFORM_HOOK, 0), // zero bytes length, zero bytes offset...
    _ctxt(ctxt),
    _startSkip(start % ctxt.blockSize()),
    _readEvents(*this, &BlockReadXform::handleRead, nullptr, TSContMutexGet(*this)) // shared mutex
{
  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(), 0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(), 0);

  // make a stack-stored version ...
  decltype(ctxt._vcsToRead) readVCs;

  readVCs.swap(ctxt._vcsToRead); // grab away from BlockSetAccess
  std::transform(readVCs.begin(), readVCs.end(), std::back_inserter(_vconns),
                 [](decltype(readVCs.front()) &fut) { return fut.get(); });
}

BlockReadXform::~BlockReadXform()
{
  for( auto &&vc : _vconns ) {
    if ( vc ) {
      auto i = &vc - &_vconns.front();
      DEBUG_LOG("close successful cache-read: #%ld %p",i,vc);
      TSVConnClose(vc); 
    }
  }
}

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
    auto avail = TSIOBufferReaderAvail(outputReader());

    if ( avail || event != TS_EVENT_VCONN_WRITE_READY) {
      TSVIOReenable(_bodyCopyVIO); // retried by the write op
      return 0L;
    }

    DEBUG_LOG("read write-vio flush-wait: %#lx+%#lx >= %#lx", TSVIONDoneGet(_bodyCopyVIO), avail, _startSkip);
    _bodyCopyVIOWaiting = event;
    return 0L;
  };

  // initial handler ....
  set_body_handler([this,blockBodyHandler](TSEvent event, TSVIO vio, int64_t left) {
    auto avail = TSIOBufferReaderAvail(outputReader());

    _bodyCopyVIOWaiting = event; // save the event...

    if ( _bodyCopyVIO || avail < _startSkip ) {
      return 0L; // already requested callback...
    }

    // no VIO and enough is available...
    DEBUG_LOG("read -> body vio begin: %#lx: %#lx-%#lx [%p]", avail, _startSkip, _startSkip + _ctxt.rangeLen(), _bodyCopyVIO);
    handleRead(TS_EVENT_VCONN_WRITE_READY, nullptr, nullptr);

    set_body_handler(blockBodyHandler); // NOTE: destructs active lambda
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
      ink_assert( event != TS_EVENT_VCONN_READ_READY );
      return; /// RETURN

    default:
      DEBUG_LOG("read unkn event: e#%d", event);
      return; /// RETURN

    case TS_EVENT_VCONN_WRITE_COMPLETE:
      if ( vio == _bodyCopyVIO && outputVIO() ) {
        DEBUG_LOG("completion with outputvio: e#%d", event);
        TSVIONDoneSet(outputVIO(), TSVIONDoneGet(outputVIO()) );
        TSVIOReenable(outputVIO()); // deliver end to main transform...
        return;
      }

      if ( vio == _bodyCopyVIO ) {
        // must forward external events to main transform...
        DEBUG_LOG("completion with zeroed outputvio: e#%d", event);
        TSVConnClose(output());
        TSVConnShutdown(output(),0,1); 
        forward_vio_event(TS_EVENT_VCONN_WRITE_COMPLETE, inputVIO()); // to make sure..
        return;
      }

      DEBUG_LOG("completion of unkn vio: e#%d", event);
      return; /// RETURN

    case TS_EVENT_VCONN_WRITE_READY:
      if ( _bodyCopyVIO ) {
        DEBUG_LOG("write-ready event for reenable: e#%d", event);
        TSVIOReenable(_bodyCopyVIO);
        return; // 
      }
      DEBUG_LOG("read to begin body write: e#%d", event);
      break; /// ....
    case TS_EVENT_VCONN_READ_COMPLETE:
      if ( vio && TSVIOVConnGet(vio) ) {
        DEBUG_LOG("close of cache-read: e#%d", event);
        TSVConnClose(TSVIOVConnGet(vio)); 
      }
      break; /// ....
  }

  // first nonzero vconn ... or past end..
  auto nxt = std::find_if(_vconns.begin(), _vconns.end(), [](TSVConn vc) { return vc != nullptr; } ) - _vconns.begin();

  auto blkSize = _ctxt.blockSize();
  auto avail   = TSIOBufferReaderAvail(outputReader()); // last ready byte in buffer..
  auto posin   = nxt * blkSize; // last ready byte in buffer..
  auto endout  = _startSkip + _ctxt.rangeLen();

  if ( _bodyCopyVIO && _bodyCopyVIOWaiting ) {
    DEBUG_LOG("flush: @%ld",posin);
    TSVIOReenable(_bodyCopyVIO); // give it a retry now..
    _bodyCopyVIOWaiting = TS_EVENT_NONE; // back to zero
  }

  // start the transform write?
  if ( ! _bodyCopyVIO && output() && avail >= _startSkip ) {
    DEBUG_LOG("write-body start: %#lx-%#lx endblk#%#lx", _startSkip, _ctxt.rangeLen(), nxt);
    TSIOBufferReaderConsume(outputReader(), _startSkip);
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

  _cacheRdVIO = TSVConnRead(_vconns[nxt], _readEvents, outputBuffer(), nextRdMax - nextRd);
  _vconns[nxt] = nullptr;
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

