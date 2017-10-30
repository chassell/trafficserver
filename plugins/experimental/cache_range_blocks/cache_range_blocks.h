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
#include "ts/ink_memory.h"
#include "ts/ink_align.h"
#include "ts/ts.h"

#include <atscppapi/GlobalPlugin.h>
#include <atscppapi/Transaction.h>
#include <atscppapi/TransactionPlugin.h>
#include <atscppapi/TransformationPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define CONTENT_ENCODING_TAG     "Content-Encoding"
#define CONTENT_LENGTH_TAG       "Content-Length"
#define RANGE_TAG                "Range"
#define CONTENT_RANGE_TAG        "Content-Range"
#define ACCEPT_ENCODING_TAG      "Accept-Encoding"
#define X_BLOCK_PRESENCE_TAG     "X-Block-Presence"

using namespace atscppapi;

//namespace
//{

class BlockStoreXform;
class BlockSendXform;

class FindTxnBlockPlugin : public TransactionPlugin
{
  using Txn_t = Transaction;
public:
  FindTxnBlockPlugin(Transaction &txn)
     : TransactionPlugin(txn),
       _clntHdrs(txn.getClientRequest().getHeaders()),
       _respRange( _clntHdrs.values(RANGE_TAG) )
  {
  }

  ~FindTxnBlockPlugin() override {}

  Headers &clientHdrs() { return _clntHdrs; }
  const std::string &clientRange() const { return _respRange; }
  const std::string &blockRange() const { return _blkRange; }

//////////////////////////////////////////
//////////// in the Transaction phases
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    // allow to try using "stub" instead of a MISS
    _clntHdrs.append(ACCEPT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL ";q=0.001");

    // use stub-file as fail-over-hit with possible miss if block is missing
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
    txn.resume();
  }

  void
  handleReadCacheLookupComplete(Transaction &txn);

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    auto &clntResp = getClientResponse().getHeaders();
    // uses 206 as response status
    txn.clntResp.set(CONTENT_RANGE_TAG, _respRange); // restore
    txn.clntResp.erase("Warning"); // erase added proxy-added warning

    // XXX erase only last field, with internal encoding
    clntResp.erase(CONTENT_ENCODING_TAG);
    txn.resume();
  }

private:
  Headers *get_stub_hdrs(Transaction &txn) 
  {
     if (txn.getCacheStatus() != Txn_t::CACHE_LOOKUP_HIT_FRESH ) {
       return &_clntHdrs;
     }

     auto &ccheHdrs = txn.getCachedRequest().getHeaders();
     auto i = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
     return ( i != std::string::npos ? &ccheHdrs : nullptr );
  }

  uint64_t have_avail_blocks(Headers &stubHdrs);

  Headers    &_clntHdrs;
  std::string _respRange;
  std::string _blkRange;

  int         _firstBlkSkip = 0; // negative if no blksize fit
  int         _lastBlkTrunc = 0; // negative if no blksize fit

  std::vector<TSCacheKey>  _keysInRange; // in order with index
  std::vector<TSVConn>     _vcsToRead; // each with an index
  std::vector<TSVConn>     _vcsToWrite;

  std::unique_ptr<BlockStoreXform> _storeXform;
  std::unique_ptr<BlockSendXform> _sendXform;
};




class BlockStoreXform : public TransformationPlugin
{
 public:
  BlockStoreXform(Transaction &txn, FindTxnBlockPlugin &ctxt)
     : TransformationPlugin(txn, REQUEST_TRANSFORMATION), 
       // new manifest from result
       _ctxt(ctxt)
  {
    // create new manifest file upon promise of data
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockStoreXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn)
  {
    auto txnhdl = static_cast<TSHttpTxn>(txn.getAtsHandle());
    // attempt to update storage with new headers
    TSHttpTxnCacheLookupStatusSet(txnhdl, TS_CACHE_LOOKUP_HIT_STALE);
    txn.resume();
  }

  void
  handleSendRequestHeaders(Transaction &txn)
  {
    auto &proxyReq = txn.getServerRequest().getHeaders();
    if ( ! _ctxt.blockRange().empty() ) {
      proxyReq.set(RANGE_TAG,_ctxt.blockRange()); // get a useful size of data
    }

    // XXX erase only last field, with internal encoding
    proxyReq.erase(ACCEPT_ENCODING_TAG);
  }

  // change to 200 and append stub-file headers...
  void
  handleReadResponseHeaders(Transaction &txn);
  
//////////////////////////////////////////
//////////// in Response-Transformation phase 

  // upstream data in
  void consume(const std::string &data) override
  {
    // produce() is how to pass onwards
    // pause() gives a future for unblocking call
  }

  // after last receive
  void handleInputComplete() override
  {
    // setOutputComplete() will close downstream
  }

private:
  FindTxnBlockPlugin &_ctxt;
};



class BlockSendXform : public TransformationPlugin
{
 public:
  BlockSendXform(Transaction &txn, FindTxnBlockPlugin &ctxt)
     : TransformationPlugin(txn, RESPONSE_TRANSFORMATION),
       _ctxt(ctxt)
  {
  }
  ~BlockSendXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
  }

//////////////////////////////////////////
//////////// in Response-Transformation phase 

  // upstream data in
  void consume(const std::string &data) override
  {
    // produce() is how to pass onwards
    // pause() gives a future for unblocking call
  }

  // after last receive
  void handleInputComplete() override
  {
    // setOutputComplete() will close downstream
  }
private:
  FindTxnBlockPlugin &_ctxt;
};



class GlobalHookPlugin : public GlobalPlugin
{
public:
  GlobalHookPlugin() { 
    GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_PRE_REMAP); 
  }

  // add stub-allowing header if has a valid range
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    auto &clntReq = txn.getClientRequest().getHeaders();
    if ( clntReq.count(RANGE_TAG) != 1 ) {
      return; // only use single-range requests
    }

    auto &txnPlugin = *new FindTxnBlockPlugin(txn); // plugin attach
    // changes client header and prep
    txnPlugin.handleReadRequestHeadersPostRemap(txn);
  }

private:
  std::string _random;
};
