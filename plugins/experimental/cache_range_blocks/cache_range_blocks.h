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
#define X_BLOCK_PRESENCE_TAG     "X-Block-Presence"

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
  static TSCont create_future_cont(std::promise<T_DATA> &prom, T_REFCOUNTED counted)
  {
    std::function<void(TSEvent,void*)> stub;

    // make stub-contp to move into lambda's ownership
    std::unique_ptr<APICont> contp(new APICont(std::move(stub))); // empty usercb at first
    auto &cont = *contp; // hold scoped-ref

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
  APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,TSVIO,int64_t), Transaction &txn)
     : TSCont_t(TSTransformCreate(&APICont::handleEvent,txn.getAtsHandle()))
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
       _clntHdrs(txn.getClientRequest().getHeaders()),
       _url(txn.getClientRequest().getUrl()),
       _respRange( _clntHdrs.values(RANGE_TAG) )
  {
  }

  ~BlockSetAccess() override {}

  Headers                     &clientHdrs() { return _clntHdrs; }
  const std::string           &clientRange() const { return _respRange; }
  const std::string           &blockRange() const { return _blkRange; }
  const std::vector<APICacheKey> &keysInRange() const { return _keysInRange; }

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
    auto &clntResp = txn.getClientResponse().getHeaders();
    // uses 206 as response status
    clntResp.set(CONTENT_RANGE_TAG, _respRange); // restore
    clntResp.erase("Warning"); // erase added proxy-added warning

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

  uint64_t have_needed_blocks(Headers &stubHdrs);

  Headers    &_clntHdrs;
  Url        &_url;
  std::string _respRange;
  std::string _blkRange;

  int         _firstBlkSkip = 0; // negative if no blksize fit
  int         _lastBlkTrunc = 0; // negative if no blksize fit

  std::vector<APICacheKey>     _keysInRange; // in order with index

  std::unique_ptr<BlockStoreXform> _storeXform;
  std::unique_ptr<BlockReadXform> _sendXform;
};




class BlockStoreXform : public TransactionPlugin
{
 public:
  BlockStoreXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn), 
       _ctxt(ctxt)
  {
       // _writerReady(*this,&BlockStoreXform::handleVConnWriteReady)

    // create new manifest file upon promise of data
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockStoreXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn);

  void 
  handleVConnWriteReady(TSEvent event, TSVConn vc, int i);

  void
  handleSendRequestHeaders(Transaction &txn)
  {
    auto &proxyReq = txn.getServerRequest().getHeaders();
    if ( ! _ctxt.blockRange().empty() ) {
      proxyReq.set(RANGE_TAG,_ctxt.blockRange()); // get a useful size of data
    }

    // XXX erase only last field, with internal encoding
    proxyReq.erase(ACCEPT_ENCODING_TAG);
    txn.resume(); // just wait for result
  }

  // change to 200 and append stub-file headers...
  void
  handleReadResponseHeaders(Transaction &txn);
  
//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                          &_ctxt;
  std::vector<std::promise<TSVConn>> _vcsToWrite; // indexed as keys
};



class BlockReadXform : public TransactionPlugin
{
 public:
  BlockReadXform(Transaction &txn, BlockSetAccess &ctxt)
     : TransactionPlugin(txn),
       _ctxt(ctxt)
  {
       // _readersReady(*this,&BlockReadXform::handleVConnReadReady)

    // create new manifest file upon promise of data
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
  }

  ~BlockReadXform() override {}

  void
  handleReadCacheLookupComplete(Transaction &txn) override;

  void 
  handleVConnReadReady(TSEvent event, TSVConn vc);

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
  }

//////////////////////////////////////////
//////////// in Response-Transformation phase 

private:
  BlockSetAccess                          &_ctxt;
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

