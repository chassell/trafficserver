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
#include "util_types.h"

#include "ts/ink_memory.h"
#include "ts/ts.h"

#include <atscppapi/Url.h>
#include <atscppapi/Transaction.h>
#include <atscppapi/TransactionPlugin.h>
#include <atscppapi/TransformationPlugin.h>
#include <atscppapi/GlobalPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <future>

#pragma once

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define VARY_TAG "Vary"
#define ETAG_TAG "ETag"
#define IF_MODIFIED_SINCE_TAG "If-Modified-Since"
#define IF_NONE_MATCH_TAG "If-None-Match"
#define IF_RANGE_TAG "If-Range"
#define CONTENT_ENCODING_TAG "Content-Encoding"
#define CONTENT_LENGTH_TAG "Content-Length"
#define RANGE_TAG "Range"
#define CONTENT_RANGE_TAG "Content-Range"
#define ACCEPT_ENCODING_TAG "Accept-Encoding"
#define X_BLOCK_BITSET_TAG "X-Block-Bitset"

#define MIN_BLOCK_STORED 8192

using namespace atscppapi;

class BlockStoreXform;
class BlockReadXform;

class BlockSetAccess : public TransactionPlugin
{
  friend BlockStoreXform; // when it needs to change over
  friend BlockReadXform;  // when it needs to change over
  using Txn_t = Transaction;
public:
  static void start_if_range_present(Transaction &txn);

public:
  explicit BlockSetAccess(Transaction &txn);
  ~BlockSetAccess() override;

  Transaction & txn() const { return _txn; }
  TSHttpTxn atsTxn() const { return _atsTxn; }
  Headers & clientHdrs() { return _clntHdrs; }
  const Url & clientUrl() const { return _url; }
  const std::string & clientRangeStr() const { return _clntRangeStr; }
  const std::string & blockRangeStr() const { return _blkRangeStr; }

  const std::vector<ATSCacheKey> & keysInRange() const { return _keysInRange; }
  const std::string & b64BlkBitset() const { return _b64BlkBitset; }

  int64_t assetLen() const { return _assetLen; }
  int64_t rangeLen() const { return _endByte - _beginByte; }
  int64_t blockSize() const { return _blkSize; }
  
  void clean_client_request(); // allow secondary-accepting block-set match
  
  // clean up, increase range-request, avoid any 304/200 if client didn't request one
  void clean_server_request(Transaction &txn);

  void reset_cached_stub(Transaction &txn) {
    txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE, 1); // permit range in cached-request
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // handle reply for stub-file storage
  }

  void prepare_cached_stub(Transaction &txn);
  void clean_client_response(Transaction &txn);

public:
  // permit use of blocks if possible
  void handleReadRequestHeadersPostRemap(Transaction &txn) override;

  // detect a block map file here
  void handleReadCacheLookupComplete(Transaction &txn) override;

  // upon a new URL init .. add these only
  void handleSendRequestHeaders(Transaction &txn) override;
  void handleReadResponseHeaders(Transaction &txn) override;

  // restore the request as before...
  void handleSendResponseHeaders(Transaction &txn) override;
private:

  void launch_block_tests();
  void handle_block_tests();

private:
  Headers *get_stub_hdrs();

  int64_t select_needed_blocks();

  void start_cache_miss(int64_t firstBlk, int64_t endBlk);
  void start_cache_hit(int64_t rangeStart);
  void set_cache_hit_bitset();

private:
  Transaction &_txn;
  const TSHttpTxn _atsTxn = nullptr;
  Url &_url;
  Headers &_clntHdrs;
  std::string _clntRangeStr;
  std::string _blkRangeStr; // from clnt req for serv req

  std::string _b64BlkBitset; // if cached and found
  int64_t _assetLen = 0L;    // if cached and found
  int64_t _blkSize  = 0L;    // if cached and found
  std::string _etagStr;      // if cached and found

  int64_t _beginByte = -1L;
  int64_t _endByte   = -1L;

  ATSCont _mutexOnlyCont;

  std::vector<ATSCacheKey> _keysInRange;               // in order with index
  std::vector<ATSVConnFuture> _vcsToRead; // indexed as the keys

  // transform objects must be committed to, upon response

  std::unique_ptr<BlockReadXform> _readXform;   // state-object ptr [registers Transforms/Continuations]
  std::unique_ptr<BlockStoreXform> _storeXform; // state-object ptr [registers Transforms/Continuations]
};

/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockStoreXform : public TransactionPlugin, public BlockTeeXform
{
public:
  using WriteVCs_t = std::vector<ATSVConnFuture>;

public:
  BlockStoreXform(BlockSetAccess &ctxt, int blockCount);
  ~BlockStoreXform() override;

  // starting point if created from lookup hook
  void handleReadCacheLookupComplete(Transaction &txn) override; 

private:
  int64_t next_valid_vconn(TSVConn &vconn, int64_t pos, int64_t len);

  int64_t handleBodyRead(TSIOBufferReader r, int64_t pos, int64_t len);
  void handleBlockWrite(TSEvent, void *, std::nullptr_t);

private:
  BlockSetAccess &_ctxt;

  std::shared_ptr<WriteVCs_t> _vcsToWriteP; 
  WriteVCs_t &_vcsToWrite; // indexed as the keys

  ATSCont _writeEvents;

  int64_t _minBuffPos = 0L;
  TSVIO _cacheWrVIO = nullptr;
  TSEvent _cacheWrVIOWaiting = TS_EVENT_NONE; // event if cache-write is blocked
  TSEvent _blockVIOWaiting = TS_EVENT_NONE; // event if body-read is blocked
};

/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockReadXform : public ATSXformCont
{
  template <typename _Tp, typename... _Args>
  friend unique_ptr<_Tp> std::make_unique(_Args &&... __args); // when it needs to change over

public:
  // starting point if created from lookup hook
  void handleReadCacheLookupComplete(Transaction &txn);

  ~BlockReadXform() override;

private:
  BlockReadXform(BlockSetAccess &ctxt, int64_t start);

  void launch_block_reads(); // from constructor
private:
  BlockSetAccess &_ctxt;
  int64_t _startSkip;

  std::vector<ATSVConnFuture> &_vconns;
};

//}