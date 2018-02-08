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

using namespace atscppapi;

void
BlockSetAccess::reset_range_keys()
{
  _keysInRange.clear();
  _keysInRange.reserve(indexLen());

  if ( _etagStr.empty() || _beginByte < 0 || _beginByte >= _endByte ) {
    DEBUG_LOG("keys are all empty: etag:%s bytes=[%ld-%ld)", _etagStr.c_str(), _beginByte, _endByte);
    _keysInRange.resize(indexLen());  // resize with defaults
    return;
  }

  for ( auto b = firstIndex() * _blkSize ; b < _endByte ; b += _blkSize) {
    _keysInRange.push_back(std::move(ATSCacheKey(clientUrl(), _etagStr, b)));
  }
}

void
BlockSetAccess::launch_block_tests()
{
  // stateless callback as deleter...
  using Deleter_t                = void (*)(BlockSetAccess *);
  static const Deleter_t deleter = [](BlockSetAccess *ptr) {
    ptr->handle_block_tests(); // recurse from last one
  };

  if ( _vcsToRead.empty() ) {
    _vcsToRead.reserve(indexLen());
  }

  auto &keys  = keysInRange();
  auto nxtBlk = _beginByte / blockSize();
  nxtBlk += _vcsToRead.size();

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockSetAccess>(this, deleter);

  int limit = 10; // block-reads spawned at one time...
 
  for ( auto i = keys.begin() + _vcsToRead.size() ; i != keys.end() ; ++i, ++nxtBlk )
  {
    auto &key = *i; // examine all the not-future'd keys
    _vcsToRead.emplace_back();
    // prep for async reads that init into VConn-futures
    auto contp      = ATSCont::create_temp_tscont(_mutexOnlyCont, _vcsToRead.back(), barrierLock);
    TSCacheRead(contp, key);

    if ( ! --limit ) {
      break;
    }
  }
}

// start read of all the blocks
void
BlockSetAccess::handle_block_tests()
{
  using namespace std::chrono;
  using std::future_status;

  //
  // mutex for single choice (read or write only) ...
  //

  if (_storeXform || _readXform) {
    return; // transform has already begun
  }

  //
  // checking once ...
  //

  auto nrdy     = 0U;
  auto skip     = 0U;
  auto finalBlk = _assetLen / _blkSize; // round down..

  //
  // scan *all* keys and vconns to check if ready
  //
  for( auto &&keyp : _keysInRange ) {
    auto i = &keyp - &_keysInRange.front();
    auto blkInd = firstIndex() + i;

    if ( i >= static_cast<ptrdiff_t>(_vcsToRead.size()) ) {
      break; // see if store is needed yet?
    }

    auto &rdFut = _vcsToRead[i];
    int error = rdFut.error();

    // key was reset with result visible..
    if ( ! keyp ) {
      ( error ? ++skip : ++nrdy );
      continue;
    }

    if ( error == ESOCK_TIMEOUT ) {
      DEBUG_LOG("read not ready: 1<<%ld %s %d", blkInd, InkStrerror(error),error);
      // leave skip [likely nonzero]
      // leave nrdy also
      continue;
    }

    if ( error == ECACHE_NO_DOC ) {
      ++skip; // allowing new store
      DEBUG_LOG("read not present: 1<<%ld %s %d", blkInd, InkStrerror(error),error);
      continue;
    }

    if ( error ) {
      ++skip;
      DEBUG_LOG("read not usable: 1<<%ld %s %d", blkInd, InkStrerror(error),error);
      keyp.reset(); // disallow new storage
      continue;
    } 

    auto vconn = rdFut.get();
    auto blkLen = TSVConnCacheObjectSizeGet(vconn);
    auto neededLen = ( blkInd == finalBlk ? ((_assetLen-1) % blockSize())+1 : blockSize() );

    // vconn/block is right size?
    if ( blkLen != neededLen ) {
      ++skip; // needs storage
      DEBUG_LOG("read returned wrong size: 1<<%ld n=%ld", blkInd, blkLen);
      continue;
    }

    // block writing to successful reads?
    keyp.reset(); // disallow new storage of successful read

    if ( skip ) {
      ++skip;
      DEBUG_LOG("read successful after skip : 1<<%ld vc:%p + ", blkInd, vconn);
      continue;
    }

    // all from the start that are successful
    ++nrdy;
    DEBUG_LOG("read successful present : 1<<%ld vc:%p + ", blkInd, vconn);
  }

  //
  // done scanning all keys
  //

  // ready to read from cache? 
  if ( nrdy == _keysInRange.size() ) {
    start_cache_hit();
    return;
  }

  if ( nrdy == _vcsToRead.size() ) {
    launch_block_tests(); // start more block tests
    return;
  }

  //
  // not all are ready and doing a write instead
  //

  // if all cache futures are accounted for...
  if ( skip + nrdy == _vcsToRead.size() ) {
    _vcsToRead.clear(); // close early
  }

  for ( auto i = nrdy ; i-- ; ) {
    _keysInRange[i].reset(); // clear the leading keys that don't need replacing...
  }

  start_cache_miss();
  _txn.resume();
}

void
BlockSetAccess::start_cache_hit()
{
  _readXform = std::make_unique<BlockReadXform>(*this, _beginByte % _blkSize); // skip this many
  _txn.resume();
}

BlockReadXform::BlockReadXform(BlockSetAccess &ctxt, int64_t toSkip)
  : ATSXformCont(ctxt.txn(), ctxt.contentLen(), toSkip) // zero bytes length, zero bytes offset...
{
  auto &vconns = ctxt._vcsToRead;
  auto len = std::min( TSVConnCacheObjectSizeGet(vconns[0].get()), toSkip + ctxt.contentLen() );
  auto lastCacheByte = toSkip + ctxt.contentLen();
  auto blkSize = ctxt.blockSize();

  reset_input_vio( TSVConnRead(vconns[0].release(), *this, outputBuffer(), len) );

  // initial handler ....
  set_body_handler([this,lastCacheByte,blkSize,&vconns](TSEvent event, TSVIO vio, int64_t left) {
    if ( event == TS_EVENT_VCONN_WRITE_READY && vio == outputVIO() ) {
      {
        auto inrdr = TSVIOReaderGet(inputVIO()); // log current buffer level
        DEBUG_LOG("output empty: %#lx (ready %#lx)", TSVIONDoneGet(vio), ( inrdr ? TSIOBufferReaderAvail(inrdr) : -1 ));
      }
      TSVIOReenable(inputVIO());
      return 0L;
    }

    if ( event == TS_EVENT_VCONN_READ_COMPLETE && vio == inputVIO() ) 
    {
      auto ovconn = TSVIOVConnGet(vio);
      TSVConnClose(ovconn); // complete close.. [CacheVC is not an InkContinuation]

      // first nonzero vconn ... or past end..
      auto nxt = std::find_if(vconns.begin(), vconns.end(), [](ATSVConnFuture &vc) { return vc.get() != nullptr; } ) - vconns.begin();

      if ( nxt >= static_cast<int64_t>(vconns.size()) ) {
        DEBUG_LOG("ignoring input-completion.  completed all reads: e#%d",event);
        return 0L; // no reads left to start!
      }

      auto nextRd    = nxt * blkSize; // next read-pos
      auto nextRdMax = std::min(nextRd + blkSize, lastCacheByte);

      DEBUG_LOG("read start: %#lx-%#lx #%#lx", nextRd, nextRdMax, nxt);

      auto len = TSVConnCacheObjectSizeGet(vconns[nxt].get());
      len = std::min(len, lastCacheByte - nextRd);

      reset_input_vio( TSVConnRead(vconns[nxt].release(), *this, outputBuffer(), nextRdMax - nextRd) );
      return 0L;
    }

    // detect ending conditions ...
    if ( event == TS_EVENT_VCONN_WRITE_COMPLETE && vio == outputVIO() ) {
      auto ndone = TSVIONDoneGet(xformInputVIO());
      DEBUG_LOG("output complete: input @%#lx",ndone);
      return 0L;
    }

    DEBUG_LOG("ignoring unhandled event. e#%d %p %ld %p",event,vio,left,inputVIO());
    return 0L;
  });
  // do not resume until first block is read in
  //
  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(), 0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(), 0);
}

BlockReadXform::~BlockReadXform() 
{
  DEBUG_LOG("destruct start");
}
