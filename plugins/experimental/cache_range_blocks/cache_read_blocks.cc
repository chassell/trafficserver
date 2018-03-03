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
  auto assetLen = _ctxt.assetLen();
  auto blkSize = _ctxt.blockSize();
  auto finalBlk = assetLen / blkSize; // round down..
  auto &keys  = _ctxt.keysInRange();
  auto nxtBlk = _ctxt.firstIndex();
  nxtBlk += _cacheVIOs.size();

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockReadXform>(this, [this](void *ptr) {
      read_block_tests(ptr); // notify on end/fail
    });

  int limit = 10; // block-reads spawned at one time...
 
  for ( auto i = keys.begin() + _cacheVIOs.size() ; i != keys.end() ; ++i, ++nxtBlk )
  {
    auto ref = barrierLock;
    auto &key = *i; // examine all the not-future'd keys
    TSCacheKey keyp = key;
    _cacheVIOs.emplace_back(std::move(ref));

    if ( nxtBlk == finalBlk ) {
      _cacheVIOs.back().add_outflow(bufferVC(),((assetLen-1) % blkSize)+1,0);
    } else {
      _cacheVIOs.back().add_outflow(bufferVC(),blkSize,0);
    }

    TSCacheRead(_cacheVIOs.back(),keyp); // begin the read and write ...
    if ( ! --limit ) {
      break;
    }
  }
}

// start read of all the blocks
void
BlockReadXform::read_block_tests(void *ptr)
{
  // simple test to see if we errored early...

  //
  // mutex for single choice (read or write only) ...
  //

  auto minfailed = ( ptr == this ? 0 : reinterpret_cast<intptr_t>(ptr) );
  auto &keys  = _ctxt.keysInRange();
  auto nrdy     = 0U;
  auto skip     = 0U;

  //
  // scan *all* keys and vconns to check if ready
  //
  for( auto &&pxyVC : _cacheVIOs ) {
    auto i = &pxyVC - &_cacheVIOs.front();
    auto blkInd = _ctxt.firstIndex() + i;

    int error = pxyVC.error();
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

    keys[i].reset(); // disallow any new storage there

    if ( error ) {
      ++skip; // more messy?
      DEBUG_LOG("read not usable: 1<<%ld [%d]", blkInd, error);
      continue;
    }

    // error is 0...

    // successful whole read for this block ...

    if ( skip ) {
      ++skip;
      DEBUG_LOG("read successful after skip : 1<<%ld vio:%p + ", blkInd, pxyVC.operator TSVConn());
      continue;
    }

    // all from the start that are successful
    ++nrdy;
    DEBUG_LOG("read successful present : 1<<%ld vio:%p + ", blkInd, pxyVC.operator TSVConn());
  }

  //
  // done scanning all keys
  //

  if ( ! outputVIO() && nrdy > 0 ) {
    _ctxt.cache_blk_hits(nrdy, _cacheVIOs.size() - nrdy);
  }

  if ( ! skip && ! minfailed ) {
    launch_block_tests(); // next set!
  }
}
