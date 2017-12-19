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

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define IF_MODIFIED_SINCE_TAG    "If-Modified-Since"
#define IF_NONE_MATCH_TAG        "If-None-Match"
#define CONTENT_ENCODING_TAG     "Content-Encoding"
#define CONTENT_LENGTH_TAG       "Content-Length"
#define RANGE_TAG                "Range"
#define CONTENT_RANGE_TAG        "Content-Range"
#define ACCEPT_ENCODING_TAG      "Accept-Encoding"
#define X_BLOCK_BITSET_TAG     "X-Block-Bitset"

#define MIN_BLOCK_STORED       8192

using namespace atscppapi;

class BlockStoreXform;
class BlockReadXform;

class BlockSetAccess : public TransactionPlugin
{
  friend BlockStoreXform; // when it needs to change over
  friend BlockReadXform; // when it needs to change over
  using Txn_t = Transaction;
public:
  explicit BlockSetAccess(Transaction &txn)
     : TransactionPlugin(txn),
       _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
       _url(txn.getClientRequest().getUrl()),
       _clntHdrs(txn.getClientRequest().getHeaders()),
       _clntRange(txn.getClientRequest().getHeaders().values(RANGE_TAG))
  {
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
  }

  ~BlockSetAccess() override {}

  Headers                     &clientHdrs() { return _clntHdrs; }
  const Url                   &clientUrl() const { return _url; }
  const std::string           &clientRange() const { return _clntRange; }
  const std::string           &blockRange() const { return _blkRange; }
  TSHttpTxn                   atsTxn() const { return _atsTxn; }
  const std::vector<APICacheKey> &keysInRange() const { return _keysInRange; }
  const std::string          &b64BlkBitset() const { return _b64BlkBitset; }
  uint64_t                    assetLen() const { return _assetLen; }

  uint64_t                    blockSize() const { return _blkSize; }

  void clean_client_request();
  void clean_server_request(Transaction &txn);
  void clean_server_response(Transaction &txn);
  void clean_client_response(Transaction &txn);

  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override;

  // detect a manifest stub file
  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    clean_client_response(txn);
    txn.resume();
  }

private:
  Headers *get_trunc_hdrs(Transaction &txn);

  uint64_t have_needed_blocks();

  const TSHttpTxn     _atsTxn = nullptr;
  Url                &_url;
  Headers            &_clntHdrs;
  std::string         _clntRange;

  uint64_t            _assetLen = 0UL; // if cached
  uint64_t            _blkSize = 0UL; // if cached
  std::string         _b64BlkBitset; // if cached
  std::string         _blkRange; // from clnt req for serv req

  int64_t             _beginByte = 0L; // negative if no blksize fit
  int64_t             _endByte = 0L; // negative if no blksize fit

  std::vector<APICacheKey>     _keysInRange; // in order with index

  std::unique_ptr<Plugin> _xform; // state-object ptr
};


class BlockInitXform : public TransactionPlugin
{
 public:
  BlockInitXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn), _ctxt(ctxt)
  {
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add user-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // remember length to create new entry
  }

  void
  handleSendRequestHeaders(Transaction &txn) override {
    _ctxt.clean_server_request(txn); // request full blocks if possible
    txn.resume();
  }

  // change to 200 and append stub-file headers...
  void
  handleReadResponseHeaders(Transaction &txn) override {
    _ctxt.clean_server_response(txn); // request full blocks if possible
    txn.resume();
  }

 private:
  BlockSetAccess                    &_ctxt;
};


class BlockStoreXform : public TransactionPlugin
{
 public:
  BlockStoreXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn), _ctxt(ctxt), 
        _vcsToWrite(ctxt.keysInRange().size())
  {
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockStoreXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendRequestHeaders(Transaction &txn) override;

  // change to 200 and append stub-file headers...
  void
  handleReadResponseHeaders(Transaction &txn) override;

//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                    &_ctxt;
  std::vector<std::shared_future<TSVConn>> _vcsToWrite; // indexed as the keys
};


class BlockReadXform : public TransactionPlugin
{
 public:
  BlockReadXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn),
       _ctxt(ctxt)
  {
  }

  ~BlockReadXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                    &_ctxt;
  std::vector<std::shared_future<TSVConn>> _vcsToRead; // indexed as the keys
};



class RangeDetect : public GlobalPlugin
{
public:
  void addHooks() { 
    GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_POST_REMAP); 
  }

  // add stub-allowing header if has a valid range
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override;

private:
  std::string _random;
};

//}

