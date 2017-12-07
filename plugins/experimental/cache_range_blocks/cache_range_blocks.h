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

class BlockInitXform;
class BlockStoreXform;
class BlockReadXform;

class BlockSetAccess : public TransactionPlugin
{
  friend BlockStoreXform; // when it needs to change over
  friend BlockReadXform;  // when it needs to change over
  using Txn_t = Transaction;

public:
  explicit BlockSetAccess(Transaction &txn);

  ~BlockSetAccess() override {}
  Transaction & txn() const { return _txn; }
  TSHttpTxn atsTxn() const { return _atsTxn; }
  Headers & clientHdrs() { return _clntHdrs; }
  const Url & clientUrl() const { return _url; }
  const std::string & clientRangeStr() const { return _clntRangeStr; }
  const std::string & blockRangeStr() const { return _blkRangeStr; }

  const std::vector<APICacheKey> & keysInRange() const { return _keysInRange; }
  const std::string & b64BlkBitset() const { return _b64BlkBitset; }

  int64_t assetLen() const { return _assetLen; }
  int64_t rangeLen() const { return _endByte - _beginByte; }
  int64_t blockSize() const { return _blkSize; }

  
  void clean_client_request(); // allow secondary-accepting block-set match
  
  // clean up, increase range-request, avoid any 304/200 if client didn't request one
  void clean_server_request(Transaction &txn);

  void reset_cached_stub(Transaction &txn) {
    txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE, 1); // permit range in cached-request
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);  // adjust range for later ...
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // handle reply for stub-file storage
  }

  void prepare_cached_stub(Transaction &txn);
  void clean_client_response(Transaction &txn);

  // permit use of blocks if possible
  void handleReadRequestHeadersPostRemap(Transaction &txn) override;

  // detect a block map file here
  void handleReadCacheLookupComplete(Transaction &txn) override;

  // upon a new URL init .. add these only
  void handleSendRequestHeaders(Transaction &txn) override;
  void handleReadResponseHeaders(Transaction &txn) override;

  // restore the request as before...
  void handleSendResponseHeaders(Transaction &txn) override;

  void handleBlockTests();

private:
  Headers *get_stub_hdrs();

  int64_t select_needed_blocks();

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

  std::vector<APICacheKey> _keysInRange;               // in order with index
  std::vector<std::shared_future<TSVConn>> _vcsToRead; // indexed as the keys

  // transform objects must be committed to, upon response

  std::unique_ptr<BlockReadXform> _readXform;   // state-object ptr
  std::unique_ptr<BlockStoreXform> _storeXform; // state-object ptr
};

/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockStoreXform : public TransactionPlugin, public BlockTeeXform
{
public:
  BlockStoreXform(BlockSetAccess &ctxt);
  ~BlockStoreXform() override;

  void handleReadCacheLookupComplete(Transaction &txn) override;
  void handleSendRequestHeaders(Transaction &txn) override;

  //////////////////////////////////////////
  //////////// in Response-Transformation phase
private:
  int64_t next_valid_vconn(TSVConn &vconn, int64_t pos, int64_t len);

  int64_t handleInput(TSIOBufferReader r, int64_t pos, int64_t len);
  void handleWrite(TSEvent, void *, std::nullptr_t);

  TSVConn next_valid_vconn(int64_t pos, int64_t len);

private:
  BlockSetAccess &_ctxt;
  std::vector<std::shared_future<TSVConn>> _vcsToWrite; // indexed as the keys
  APICont _writeEvents;
};

/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockReadXform : public APIXformCont
{
public:
  BlockReadXform(BlockSetAccess &ctxt, int64_t start);

private:
  void handleRead(TSEvent, void *, std::nullptr_t);

private:
  BlockSetAccess &_ctxt;
  int64_t _startByte;
  TSVIO _bodyVIO = nullptr;
  std::vector<TSVConn> _vconns;
  APICont _readEvents;
};

//}
