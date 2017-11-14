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

#include "atscppapi/HttpStatus.h"
#include "ts/experimental.h"
#include "ts/InkErrno.h"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

// start read of all the blocks
void
BlockReadXform::handleReadCacheLookupComplete(Transaction &txn)
{
  auto &keys = _ctxt.keysInRange();

  // XXX: do in ctor or as static??
  if ( _vcsToRead.empty() || _vcsToRead.size() != keys.size() )
  {
    _vcsToRead.resize( keys.size() );
    // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
    auto barrierLock = std::shared_ptr<BlockReadXform>(this, [&txn](BlockReadXform *ptr) {
             ptr->handleReadCacheLookupComplete(txn);  // recurse from last one
          });
    for( auto i = 0U ; i < keys.size() ; ++i ) {
      // prep for async reads that init into VConn-futures
      // XXX: pass a use-for-all lambda-ref instead???
      auto contp = APICont::create_temp_tscont(_vcsToRead[i],barrierLock);
      TSCacheRead(contp,keys[i]);
    }
    return;
  }

  auto nrdy = 0U;
  auto firstBlk = _ctxt._beginByte/_ctxt._blkSize;

  // scan *all* keys and vconns to check if ready
  for( auto n = 0U ; n < _vcsToRead.size() ; ++n ) 
  {
    // clear bit
    base64_bit_clr(_ctxt._b64BlkBitset,firstBlk + n);

    if ( _vcsToRead[n].wait_for(std::chrono::seconds(0)) != std::future_status::ready ) {
      continue;
    }

    auto vconn = _vcsToRead[n].get();
    auto vconnErr = -reinterpret_cast<intptr_t>(vconn); // block isn't ready
    // valid pointers don't look like this
    if ( ! vconnErr || ( vconnErr >= CACHE_ERRNO && vconnErr < EHTTP_ERROR ) ) {
      continue;
    }

    // block is ready and of right size?
    if ( TSVConnCacheObjectSizeGet(vconn) != static_cast<int64_t>(_ctxt.blockSize()) ) {
      // TODO: old block
      continue;
    }
    
    // successful!
    ++nrdy;
    base64_bit_set(_ctxt._b64BlkBitset,firstBlk + n); // set a bit
  }

  // ready to read from cache...
  if ( nrdy < keys.size() ) {
    DEBUG_LOG("cache-resp-wr: len=%lu set=%s",_ctxt.assetLen(),_ctxt.b64BlkBitset().c_str());

    // intercept data for new or updated stub version
    auto &ctxt = _ctxt; // keep a safe stack ref
    ctxt._xform = std::unique_ptr<Plugin>(new BlockStoreXform(txn,_ctxt));  // DELETES THIS-OBJECT
    ctxt._xform->handleReadCacheLookupComplete(txn);
    return;
  }

  TSHttpTxnCacheLookupStatusSet(_ctxt.atsTxn(), TS_CACHE_LOOKUP_HIT_FRESH);

  auto &cachhdrs = txn.updateCachedResponse().getHeaders();
  cachhdrs.erase(CONTENT_RANGE_TAG); // erase to remove concerns
  cachhdrs.set(X_BLOCK_BITSET_TAG,_ctxt._b64BlkBitset); // attempt to erase/rewrite field in headers

  DEBUG_LOG("updated bitset: %s",_ctxt._b64BlkBitset.c_str());
  DEBUG_LOG("updated cache-hdrs:\n%s\n------\n",cachhdrs.wireStr().c_str());

  // change the cached header we're about to send out
  TSHttpTxnUpdateCachedObject(_ctxt.atsTxn());

  txn.resume(); // blocks are looked up .. so we can continue...
}
