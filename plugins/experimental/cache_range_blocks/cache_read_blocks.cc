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

  if (_storeXform || _readXform || _initXform) {
    return; // transform has already begun
  }

  auto nrdy     = 0U;
  auto firstBlk = _beginByte / _blkSize;

  // scan *all* keys and vconns to check if ready
  for (auto n = 0U; n < _vcsToRead.size(); ++n) {
    // clear bit
    base64_bit_clr(_b64BlkBitset, firstBlk + n);

    if (_vcsToRead[n].wait_for(seconds::zero()) != future_status::ready) {
      DEBUG_LOG("read isn't ready: #%#lx", firstBlk + n);
      continue;
    }

    auto vconn    = _vcsToRead[n].get();
    auto vconnErr = -reinterpret_cast<intptr_t>(vconn); // block isn't ready
    // valid pointers don't look like this
    if (!vconnErr || (vconnErr >= CACHE_ERRNO && vconnErr < EHTTP_ERROR)) {
      DEBUG_LOG("read returned non-pointer: #%#lx == %ld", firstBlk + n, vconnErr);
      continue;
    }

    // block is ready and of right size?
    if (TSVConnCacheObjectSizeGet(vconn) != static_cast<int64_t>(blockSize())) {
      DEBUG_LOG("read returned wrong size: #%#lx", firstBlk + n);
      continue;
    }

    _keysInRange[n].reset(); // no need for cache-key

    // successful!
    ++nrdy;
    base64_bit_set(_b64BlkBitset, firstBlk + n); // set a bit
    DEBUG_LOG("read successful bitset: %s", _b64BlkBitset.c_str());
  }

  // ready to read from cache...
  if (_vcsToRead.empty() || nrdy < _keysInRange.size()) {
    DEBUG_LOG("cache-resp-wr: len=%#lx set=%s", assetLen(), b64BlkBitset().c_str());

    // intercept data for new or updated stub version
    _storeXform = std::make_unique<BlockStoreXform>(*this);
    _storeXform->handleReadCacheLookupComplete(_txn);
    return;
  }

  //  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_HIT_FRESH);

  auto &cachhdrs = _txn.updateCachedResponse().getHeaders();
  //  cachhdrs.erase(CONTENT_RANGE_TAG); // erase to remove concerns
  cachhdrs.set(X_BLOCK_BITSET_TAG, _b64BlkBitset); // attempt to erase/rewrite field in headers

  DEBUG_LOG("updated bitset: %s", _b64BlkBitset.c_str());
  DEBUG_LOG("updated cache-hdrs:\n%s\n------\n", cachhdrs.wireStr().c_str());

  // change the cached header we're about to send out
  TSHttpTxnUpdateCachedObject(atsTxn());

  // adds hook for transform...
  _readXform = std::make_unique<BlockReadXform>(*this, _beginByte);
  // don't continue until first block is read ...
}

BlockReadXform::BlockReadXform(BlockSetAccess &ctxt, int64_t start)
  : APIXformCont(ctxt.txn(), TS_HTTP_RESPONSE_TRANSFORM_HOOK, 0),
    _ctxt(ctxt),
    _startByte(start % ctxt.blockSize()),
    _readEvents(*this, &BlockReadXform::handleRead, nullptr)
{
  auto blkSize  = ctxt.blockSize();
  auto &readVCs = ctxt._vcsToRead;

  TSHttpTxnUntransformedRespCache(ctxt.atsTxn(), 0);
  TSHttpTxnTransformedRespCache(ctxt.atsTxn(), 0);

  // move get()'s into _vconns vector
  std::transform(readVCs.begin(), readVCs.end(), std::back_inserter(_vconns),
                 [](decltype(readVCs.front()) &fut) { return fut.get(); });

  // start first read *only* to fill buffer for later
  TSVConnRead(_vconns[0], _readEvents, outputBuffer(), blkSize);
  _vconns[0] = nullptr;

  set_body_handler([this](TSEvent event, TSVIO vio, int64_t left) {
    auto avail = TSIOBufferReaderAvail(outputReader());
    auto ndone = (_bodyVIO ? TSVIONDoneGet(_bodyVIO) : -1);

    // write has buffered up?  flush down
    if (!_bodyVIO && avail >= _startByte) {
      // start full write
      DEBUG_LOG("read -> body vio begin: %#lx >= %#lx: n=%#lx", avail, _startByte, _ctxt.rangeLen());
      TSIOBufferReaderConsume(outputReader(), _startByte);
      _bodyVIO = TSVConnWrite(output(), *this, outputReader(), _ctxt.rangeLen());
      return 0L;
    }

    if (_bodyVIO && event == TS_EVENT_VCONN_WRITE_READY) {
      TSVIOReenable(_bodyVIO);
      DEBUG_LOG("read write-vio flush: %#lx+%#lx >= %#lx", ndone, avail, _startByte);
      return 0L;
    }

    if (_bodyVIO && event == TS_EVENT_VCONN_WRITE_COMPLETE) {
      DEBUG_LOG("flushing close: #%d pos:%#lx / %#lx", event, ndone, avail);
      TSVIOReenable(_bodyVIO);
      return 0L;
    }

    DEBUG_LOG("read unkn event: #%d pos:%#lx / %#lx", event, ndone, avail);
    return 0L;
  });

  // do not resume until first block is read in
}

void
BlockReadXform::handleRead(TSEvent event, void *, std::nullptr_t)
{
  // gauge total read already
  if (event != TS_EVENT_VCONN_READ_COMPLETE) {
    DEBUG_LOG("read event: #%d", event);
    return;
  }

  auto n = std::count(_vconns.begin(), _vconns.end(), nullptr);

  // only first read ended ...
  if (n == 1 && !_bodyVIO) {
    _ctxt.txn().resume(); // continue w/data
    DEBUG_LOG("read of first block complete");
  }

  auto blkSize = _ctxt.blockSize();
  auto pos     = _startByte; // position in output stream
  auto end     = _startByte + _ctxt.rangeLen();

  if (_bodyVIO) {
    pos += TSVIONDoneGet(_bodyVIO);
  }

  // get next to read
  auto blkNum    = (pos + blkSize - 1) / blkSize;
  int64_t blkMax = _vconns.size();

  while (!_vconns[blkNum] && blkNum < blkMax) {
    ++blkNum;
    pos += blkSize;
  }

  if (blkNum >= blkMax) {
    return; // no more thanks
  }

  auto currRead    = blkNum * blkSize;
  auto currReadMax = std::min((blkNum + 1) * blkSize, end);

  DEBUG_LOG("read start: %#lx-%#lx #%#lx", currRead, currReadMax, blkNum);
  TSVConnRead(_vconns[blkNum], _readEvents, outputBuffer(), currReadMax - currRead);
  _vconns[blkNum] = nullptr;
}
