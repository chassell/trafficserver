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

#define CONTENT_ENCODING_TAG     "Content-Encoding"
#define CONTENT_LENGTH_TAG       "Content-Length"
#define RANGE_TAG                "Range"
#define CONTENT_RANGE_TAG        "Content-Range"
#define ACCEPT_ENCODING_TAG      "Accept-Encoding"
#define X_BLOCK_BITSET_TAG     "X-Block-Bitset"

using namespace atscppapi;

// namespace {

using TSCacheKey_t = std::unique_ptr<std::remove_pointer<TSCacheKey>::type>;
using TSCont_t = std::unique_ptr<std::remove_pointer<TSCont>::type>;
using TSMutex_t = std::unique_ptr<std::remove_pointer<TSMutex>::type>;
using TSMBuffer_t = std::unique_ptr<std::remove_pointer<TSMBuffer>::type>;
using TSIOBuffer_t = std::unique_ptr<std::remove_pointer<TSIOBuffer>::type>;
using TSIOBufferReader_t = std::unique_ptr<std::remove_pointer<TSIOBufferReader>::type>;

namespace std {
// unique_ptr deletions
template <> inline void default_delete<TSCacheKey_t::element_type>::operator()(TSCacheKey key) const 
  { TSCacheKeyDestroy(key); }
template <> inline void default_delete<TSCont_t::element_type>::operator()(TSCont cont) const 
  { TSContDestroy(cont); }
template <> inline void default_delete<TSMutex_t::element_type>::operator()(TSMutex mutex) const 
  { TSMutexDestroy(mutex); }
template <> inline void default_delete<TSMBuffer_t::element_type>::operator()(TSMBuffer buff) const 
  { TSMBufferDestroy(buff); }
template <> inline void default_delete<TSIOBuffer_t::element_type>::operator()(TSIOBuffer buff) const 
  { TSIOBufferDestroy(buff); }
template <> inline void default_delete<TSIOBufferReader_t::element_type>::operator()(TSIOBufferReader reader) const 
  { TSIOBufferReaderFree(reader); }
}

// unique-ref object for a cache-read or cache-write request
struct APICacheKey : public TSCacheKey_t
{
  APICacheKey() = default; // nullptr by default

  operator TSCacheKey() const { return get(); }

  APICacheKey(const Url &url, uint64_t offset)
     : TSCacheKey_t(TSCacheKeyCreate()) 
  {
     auto str = url.getUrlString();
     str.append(reinterpret_cast<char*>(&offset),sizeof(offset)); // append *unique* position bytes
	 TSCacheKeyDigestSet(get(), str.data(), str.size() );
	 auto host = url.getHost();
	 TSCacheKeyHostNameSet(get(), host.data(), host.size()); 
  }
};

// object to request write/read into cache
struct APICont : public TSCont_t
{
  APICont() = default; // nullptr by default

  operator TSCont() { return get(); }

  template <typename T_DATA, typename T_REFCOUNTED>
  static TSCont create_temp_tscont(std::promise<T_DATA> &prom, T_REFCOUNTED counted)
  {
    std::function<void(TSEvent,void*)> stub;

    // make stub-contp to move into lambda's ownership
    std::unique_ptr<APICont> contp(new APICont(std::move(stub))); // empty usercb at first
    auto &cont = *contp; // hold scoped-ref

    (void) counted;

    cont._userCB = std::move( [&cont,&prom,counted](TSEvent evt, void *data) {
                std::unique_ptr<APICont> contp(&cont); // hold in fxn until done
                prom.set_value(static_cast<T_DATA>(data));
            });

    return *contp.release(); // now it owns itself (until callback)
  }

  // accepts TSHttpTxn handler functions
  template <class T_OBJ, typename T_DATA>
  APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,TSHttpTxn,T_DATA), T_DATA cbdata)
     : TSCont_t(TSContCreate(&APICont::handleEvent,TSMutexCreate())) 
  {
    // point back here
    TSContDataSet(get(),this);
    // memorize user data to forward on
    _userCB = decltype(_userCB)([&obj,funcp,cbdata](TSEvent event, void *evtdata) 
       {
        (obj.*funcp)(event,static_cast<TSHttpTxn>(evtdata),cbdata);
       });
  }

  // callback is "cb(TSEvent, TSVIO, ntodo)"
  // can get with TSVConnWrite(), TSVConnRead() :
  //		VC_EVENT_WRITE_READY / VC_EVENT_READ_READY
  //		VC_EVENT_WRITE_COMPLETE / VC_EVENT_READ_COMPLETE,
  //	[wr/rd] VC_EVENT_EOS, VC_EVENT_INACTIVITY_TIMEOUT, VC_EVENT_ACTIVE_TIMEOUT
  template <class T_OBJ>
  APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,TSVIO,int64_t), TSHttpTxn txnHndl)
     : TSCont_t(TSTransformCreate(&APICont::handleEvent,txnHndl))
  {
    // point back here
    TSContDataSet(get(),this);
    // memorize user data to forward on
    _userCB = decltype(_userCB)([&obj,funcp](TSEvent event, void *evtdata) 
       {
         auto vio = static_cast<TSVIO>(evtdata);
         (obj.*funcp)(event,vio,TSVIONTodoGet(vio));
       });
  }

private:
  static int handleEvent(TSCont cont, TSEvent event, void *data) {
    APICont *self = static_cast<APICont*>(TSContDataGet(cont));
    ink_assert(self->operator TSCont() == cont);
    self->_userCB(event,data);
    return 0;
  }

  APICont(std::function<void(TSEvent,void*)> &&fxn)
     : TSCont_t(TSContCreate(&APICont::handleEvent,nullptr)),
       _userCB(fxn)
  {
    // point back here
    TSContDataSet(get(),this);
  }

  // holds object and function pointer
  std::function<void(TSEvent,void*)> _userCB;
};

class BlockStoreXform;
class BlockReadXform;

class BlockSetAccess : public TransactionPlugin
{
  using Txn_t = Transaction;
public:
  BlockSetAccess(Transaction &txn)
     : TransactionPlugin(txn),
       _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
       _url(txn.getClientRequest().getUrl()),
       _clntHdrs(txn.getClientRequest().getHeaders()),
       _clntRange(txn.getClientRequest().getHeaders().values(RANGE_TAG))
  {
  }

  ~BlockSetAccess() override {}

  Headers                     &clientHdrs() { return _clntHdrs; }
  const Url                   &clientUrl() const { return _url; }
  const std::string           &clientRange() const { return _clntRange; }
  const std::string           &blockRange() const { return _blkRange; }
  TSHttpTxn                   atsTxn() const { return _atsTxn; }
  const std::vector<APICacheKey> &keysInRange() const { return _keysInRange; }

  uint64_t                    blockSize() const { return _blkSize; }

  void clean_server_request(Transaction &txn);
  void clean_server_response(Transaction &txn);

  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    // allow to try using "stub" instead of a MISS
    _clntHdrs.append(ACCEPT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL ";q=0.001");

    // use stub-file as fail-over-hit with possible miss if block is missing
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
    txn.resume();
  }

  // detect a manifest stub file
  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    auto &clntResp = txn.getClientResponse().getHeaders();

    // override block-style range
    if ( ! _respRange.empty() ) {
      clntResp.set(CONTENT_RANGE_TAG, _respRange); // restore
    }

    clntResp.erase("Warning"); // erase added proxy-added warning

    // TODO erase only last field, with internal encoding
    clntResp.erase(CONTENT_ENCODING_TAG);
    clntResp.erase(X_BLOCK_BITSET_TAG);
    txn.resume();
  }

private:
  Headers *get_stub_hdrs(Transaction &txn) 
  {
     // not even found?
     if (txn.getCacheStatus() != Txn_t::CACHE_LOOKUP_HIT_FRESH ) {
       return &_clntHdrs;
     }

     auto &ccheHdrs = txn.getCachedRequest().getHeaders();
     auto i = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
     return ( i != std::string::npos ? &ccheHdrs : nullptr );
  }

  uint64_t have_needed_blocks();

  const TSHttpTxn     _atsTxn = nullptr;
  Url                &_url;
  Headers            &_clntHdrs;
  std::string         _clntRange;

  uint64_t            _assetLen = 0ULL; // if cached
  uint64_t            _blkSize = 0ULL; // if cached
  std::string         _b64BlkBitset; // if cached
  std::string         _respRange; // from clnt req for resp
  std::string         _blkRange; // from clnt req for serv req

  int         _firstBlkSkip = 0; // negative if no blksize fit
  int         _lastBlkTrunc = 0; // negative if no blksize fit

  std::vector<APICacheKey>     _keysInRange; // in order with index

  std::unique_ptr<Plugin> _xform;
};


class BlockInitXform : public TransactionPlugin
{
 public:
  BlockInitXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn), _ctxt(ctxt)
  {
    TSHttpTxnUntransformedRespCache(_ctxt.atsTxn(), 0);
    TSHttpTxnTransformedRespCache(_ctxt.atsTxn(), 1);  // create mfest headers
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
     : TransactionPlugin(txn), _ctxt(ctxt), _vcsToWrite(ctxt.keysInRange().size())
  {
    TSHttpTxnUntransformedRespCache(_ctxt.atsTxn(), 0); 
    TSHttpTxnTransformedRespCache(_ctxt.atsTxn(), 1);  // update mfest headers
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockStoreXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendRequestHeaders(Transaction &txn) override {
    _ctxt.clean_server_request(txn); // request full blocks if possible
    txn.resume();
  }

  // change to 200 and append stub-file headers...
  void
  handleReadResponseHeaders(Transaction &txn) override;

//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                    &_ctxt;
  std::vector<std::promise<TSVConn>> _vcsToWrite; // indexed as keys
};



class BlockReadXform : public TransactionPlugin
{
 public:
  BlockReadXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn),
       _ctxt(ctxt)
  {
    // create new manifest file upon promise of data
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockReadXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
  }

//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                    &_ctxt;
  std::vector<std::promise<TSVConn>> _vcsToRead; // indexed as keys
};



class RangeDetect : public GlobalPlugin
{
public:
  RangeDetect() { 
    GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_POST_REMAP); 
  }

  // add stub-allowing header if has a valid range
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    auto &clntReq = txn.getClientRequest().getHeaders();
    if ( clntReq.count(RANGE_TAG) != 1 ) {
      return; // only use single-range requests
    }

    auto &txnPlugin = *new BlockSetAccess(txn); // plugin attach
    // changes client header and prep
    txnPlugin.handleReadRequestHeadersPostRemap(txn);
  }

private:
  std::string _random;
};

//}

