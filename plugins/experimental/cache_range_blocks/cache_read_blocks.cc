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
  _keysInRange.reserve(indexCnt());

  if ( _etagStr.empty() || _beginByte < 0 || _beginByte >= _endByte ) {
    DEBUG_LOG("keys are all empty: etag:%s bytes=[%ld-%ld)", _etagStr.c_str(), _beginByte, _endByte);
    _keysInRange.resize(indexCnt());  // resize with defaults
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

  if ( _cacheVIOs.empty() ) {
    _cacheVIOs.reserve(indexCnt());
  }

  auto &keys  = keysInRange();
  auto nxtBlk = firstIndex();
  nxtBlk += _cacheVIOs.size();

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockSetAccess>(this, deleter);

  int limit = 10; // block-reads spawned at one time...
 
  for ( auto i = keys.begin() + _cacheVIOs.size() ; i != keys.end() ; ++i, ++nxtBlk )
  {
    auto &key = *i; // examine all the not-future'd keys
    _cacheVIOs.emplace_back();
    // prep for async reads that init into VConn-futures
    auto contp      = ATSCont::create_temp_tscont(_mutexOnlyCont, _cacheVIOs.back(), barrierLock);
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

    if ( i >= static_cast<ptrdiff_t>(_cacheVIOs.size()) ) {
      break; // see if store is needed yet?
    }

    auto &rdFut = _cacheVIOs[i];
    int error = rdFut.error();

    // key was reset with result visible..
    if ( ! keyp ) {
      ( error ? ++skip : ++nrdy );
      continue;
    }

    if ( error == EAGAIN ) {
      DEBUG_LOG("read not ready: 1<<%ld [%d]", blkInd, error);
      // leave skip [likely nonzero]
      // leave nrdy also
      continue;
    }

    if ( error == ECACHE_NO_DOC ) {
      ++skip; // allowing new store
      DEBUG_LOG("read not present: 1<<%ld [%d]", blkInd, error);
      continue;
    }

    if ( error ) {
      ++skip;
      DEBUG_LOG("read not usable: 1<<%ld [%d]", blkInd, error);
      keyp.reset(); // disallow new storage
      continue;
    } 

    auto vio = rdFut.get();
    auto blkLen = TSVIONDoneGet(vio);
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
      DEBUG_LOG("read successful after skip : 1<<%ld vio:%p + ", blkInd, vio);
      continue;
    }

    // all from the start that are successful
    ++nrdy;
    DEBUG_LOG("read successful present : 1<<%ld vio:%p + ", blkInd, vio);
  }

  //
  // done scanning all keys
  //

  // ready to read from cache? 
  if ( nrdy == _keysInRange.size() ) {
    start_cache_hit();
    return;
  }

  if ( nrdy == _cacheVIOs.size() ) {
    launch_block_tests(); // start more block tests
    return;
  }

  //
  // not all are ready and doing a write instead
  //

  // if all cache futures are accounted for...
  if ( skip + nrdy == _cacheVIOs.size() ) {
    _cacheVIOs.clear(); // close early
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

void
BlockReadXform::on_empty_buffer()
{
  auto pos = bufferAvail();
  auto opos = outputAvail();
  DEBUG_LOG("buffer low: @%#lx-%#lx", pos.first, pos.second);

  // more required from input stream... 
  if ( TSVIONTodoGet(inputVIO()) ) {
    TSVIOReenable(inputVIO()); // just reenable [if blocked]
    return;
  }

  if ( ! TSVIONTodoGet(outputVIO()) ) {
    return; // extra?
  }

  // input read/write is at completion?  start new one...
  auto nxt = (pos.second + _blkSize-1)/_blkSize; // next *full* block..
  if ( nxt >= static_cast<int>(_keysToRead.size()) ) {
    DEBUG_LOG("empty and %#lx-%#lx incomplete.", opos.second, outputLen());
    return; // no reads left to start!
  }

  if ( nxt >= static_cast<int>(_cacheVIOs.size()) ) {
  }

  auto vio = _cacheVIOs[nxt].get();
  if ( ! vio ) {
    ink_assert(!"need a block that's missing");
  }

  // just begin with an input
  TSContCall(*this, TS_EVENT_IMMEDIATE, this);
}


BlockReadXform::BlockReadXform(BlockSetAccess &ctxt, int64_t offset)
  : ATSXformCont(ctxt.txn(), ctxt.contentLen(), offset), // may be zero/zero..
    _blkSize(ctxt.blockSize())
{
  _cacheVIOs.swap(ctxt._cacheVIOs);
  _keysToRead.swap(ctxt._keysInRange);

  // initial handler ....
  set_body_handler([this](TSEvent event, TSVIO vio, int64_t left) 
  {
      if ( event != TS_EVENT_VCONN_WRITE_READY ) {
        DEBUG_LOG("ignoring unhandled event. e#%d %p %ld %p",event,vio,left,inputVIO());
        return 0L;
      }

      if ( ! left && vio == outputVIO() ) {
        this->on_empty_buffer();
        return 0L;
      }

      TSVIOReenable(vio); // just reenable [if blocked]
      return 0L;
  });
  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(), 0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(), 0);
}

BlockReadXform::~BlockReadXform() 
{
  DEBUG_LOG("destruct start");
}
