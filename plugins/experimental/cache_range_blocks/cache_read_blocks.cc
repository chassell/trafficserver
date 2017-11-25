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

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

using namespace atscppapi;

// start read of all the blocks
void
BlockSetAccess::handleBlockTests()
{
  using namespace std::chrono;
  using std::future_status;

  //
  // mutex-protected check
  // 

  if ( _storeXform || _readXform || _initXform ) {
    return; // transform has already begun
  }

  auto nrdy = 0U;
  auto firstBlk = _beginByte/_blkSize;

  // scan *all* keys and vconns to check if ready
  for( auto n = 0U ; n < _vcsToRead.size() ; ++n ) 
  {
    // clear bit
    base64_bit_clr(_b64BlkBitset,firstBlk + n);

    if ( _vcsToRead[n].wait_for(seconds::zero()) != future_status::ready ) {
      continue;
    }

    auto vconn = _vcsToRead[n].get();
    auto vconnErr = -reinterpret_cast<intptr_t>(vconn); // block isn't ready
    // valid pointers don't look like this
    if ( ! vconnErr || ( vconnErr >= CACHE_ERRNO && vconnErr < EHTTP_ERROR ) ) {
      continue;
    }

    // block is ready and of right size?
    if ( TSVConnCacheObjectSizeGet(vconn) != static_cast<int64_t>(blockSize()) ) {
      // TODO: old block
      continue;
    }
    
    // successful!
    ++nrdy;
    base64_bit_set(_b64BlkBitset,firstBlk + n); // set a bit
  }

  // ready to read from cache...
  if ( nrdy < _keysInRange.size() ) {
    DEBUG_LOG("cache-resp-wr: len=%lu set=%s",assetLen(),b64BlkBitset().c_str());

    // intercept data for new or updated stub version
    _storeXform = std::make_unique<BlockStoreXform>(*this);
    _storeXform->handleReadCacheLookupComplete(_txn);
    return;
  }

  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_HIT_FRESH);

  auto &cachhdrs = _txn.updateCachedResponse().getHeaders();
  cachhdrs.erase(CONTENT_RANGE_TAG); // erase to remove concerns
  cachhdrs.set(X_BLOCK_BITSET_TAG,_b64BlkBitset); // attempt to erase/rewrite field in headers

  DEBUG_LOG("updated bitset: %s",_b64BlkBitset.c_str());
  DEBUG_LOG("updated cache-hdrs:\n%s\n------\n",cachhdrs.wireStr().c_str());

  // change the cached header we're about to send out
  TSHttpTxnUpdateCachedObject(atsTxn());

  // adds hook for transform...
  _readXform = std::make_unique<BlockReadXform>(*this);

  _txn.resume(); // blocks are looked up .. so we can continue...
}

BlockReadXform::BlockReadXform(BlockSetAccess &ctxt)
   : APIXformCont(ctxt.atsTxn(), TS_HTTP_RESPONSE_TRANSFORM_HOOK, ctxt.rangeLen())
{
  std::transform( ctxt._vcsToRead.begin(), ctxt._vcsToRead.end(), std::back_inserter(_vconns), 
                    [](decltype(ctxt._vcsToRead.front()) &fut) { return fut.get(); }
                );
  auto &xform = static_cast<APIXformCont&>(*this);
  xform = [&ctxt,&xform](TSEvent event,TSVIO vio) {
     // vio can be Xform-Output-VIO [write] --> reenable read-VIO
     // vio can be CacheVC-VIO [read] --> reenable output-VIO

     // READ_COMPLETE --> start new VIO 
     // WRITE_COMPLETE --> shutdown all
  };
}

