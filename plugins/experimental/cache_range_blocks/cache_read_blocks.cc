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

BlockReadXform::Ptr_t 
BlockReadXform::try_cache_hit(BlockSetAccess &ctxt, int64_t offset)
{
  return std::make_shared<BlockReadXform>(ctxt, offset); // skip this many
}

BlockReadXform::BlockReadXform(BlockSetAccess &ctxt, int offset)
  : ATSXformCont(ctxt.txn(), ctxt.contentLen(), offset), // may be zero/zero..
    _ctxt(ctxt),
    _blkSize(ctxt.blockSize())
{
  DEBUG_LOG("construct start: offset %d", offset);

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

void
BlockReadXform::launch_block_tests()
{
  // stateless callback as deleter...
  using Deleter_t                = void (*)(BlockReadXform *);
  static const Deleter_t deleter = [](BlockReadXform *ptr) {
    ptr->read_block_tests(); // recurse from last one
  };

  if ( _cacheVIOs.empty() ) {
    _cacheVIOs.reserve(_ctxt.indexCnt());
  }

  auto &keys  = _ctxt.keysInRange();
  auto nxtBlk = _ctxt.firstIndex();
  nxtBlk += _cacheVIOs.size();

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockReadXform>(this, deleter);

  int limit = 10; // block-reads spawned at one time...
 
  for ( auto i = keys.begin() + _cacheVIOs.size() ; i != keys.end() ; ++i, ++nxtBlk )
  {
    auto &key = *i; // examine all the not-future'd keys
    _cacheVIOs.emplace_back();
    // prep for async reads that init into VConn-futures
    auto contp      = ATSCont::create_temp_tscont(*this, _cacheVIOs.back(), barrierLock);
    TSCacheRead(contp, key);

    if ( ! --limit ) {
      break;
    }
  }
}

// start read of all the blocks
void
BlockReadXform::read_block_tests()
{
  //
  // mutex for single choice (read or write only) ...
  //

  auto &keys  = _ctxt.keysInRange();
  auto blkSize = _ctxt.blockSize();
  auto assetLen = _ctxt.assetLen();
  auto nrdy     = 0U;
  auto skip     = 0U;
  auto finalBlk = assetLen / blkSize; // round down..
  auto neededLen = blkSize; 

  //
  // scan *all* keys and vconns to check if ready
  //
  for( auto &&rdFut : _cacheVIOs ) {
    auto i = &rdFut - &_cacheVIOs.front();
    auto blkInd = _ctxt.firstIndex() + i;

    int error = rdFut.error();
    auto key = keys[i].get(); // disallow any new storage 

    // key was reset and result is ready ...
    if ( ! key ) {
      ( error ? ++skip : ++nrdy );
      continue; // no need to analyze it...
    }

    if ( error == EAGAIN ) {
      DEBUG_LOG("read not ready: 1<<%ld [%d]", blkInd, error);
      // leave skip [likely nonzero]
      // leave nrdy also
      continue;
    }

    if ( error == ECACHE_NO_DOC ) {
      ++skip; // allow a cache-miss to store it ..
      DEBUG_LOG("read not present: 1<<%ld [%d]", blkInd, error);
      continue;
    }

    // no new storage with this key ...

    keys[i].release(); // disallow any new storage 

    if ( error ) {
      ++skip; // more messy?
      DEBUG_LOG("read not usable: 1<<%ld [%d]", blkInd, error);
      continue;
    } 

    // error is 0...

    auto vio = rdFut.get();

    auto len = TSVIONDoneGet(vio);

    if ( blkInd == finalBlk ) {
      neededLen = ((assetLen-1) % blkSize)+1;
    }

    // vconn/block is right size?
    if ( len != neededLen ) {
      ++skip; // allow a cache-miss to overwrite it...
      DEBUG_LOG("read returned wrong size: 1<<%ld n=%ld", blkInd, len);
      continue;
    }

    // successful whole read ...

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

  if ( outputVIO() ) {
    // these are live blocks?   ask to write them...
    TSContCall(*this, TS_EVENT_VCONN_WRITE_READY, outputVIO());
    return;
  } 

  // early only!

  // no early parts to use?
  if ( nrdy > 0 ) {
    init_enabled_transform();
  }
  _ctxt.cache_blk_hits(nrdy, _cacheVIOs.size() - nrdy);
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
  if ( nxt >= static_cast<int>(_ctxt.keysInRange().size()) ) {
    DEBUG_LOG("empty and %#lx-%#lx incomplete.", opos.second, outputLen());
    return; // no reads left to start!
  }

  if ( nxt >= static_cast<int>(_cacheVIOs.size()) ) {
    ink_assert(!"time for a recurse");
    return;
  }

  auto vio = _cacheVIOs[nxt].get();
  if ( vio ) {
    reset_input_vio(vio);
    return;
  }

  launch_block_tests();
}

