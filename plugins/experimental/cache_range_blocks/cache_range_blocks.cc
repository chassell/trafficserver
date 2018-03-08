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
#include "util_types.h"

#include <atscppapi/HttpStatus.h>
#include <atscppapi/RemapPlugin.h>
#include <ts/experimental.h>
#include <ts/InkErrno.h>

#include <algorithm>

thread_local int ThreadTxnID::g_pluginTxnID = -1;

static int parse_range(std::string rangeFld, int64_t len, int64_t &start, int64_t &end);

void BlockSetAccess::start_if_range_present(Transaction &txn) 
{
  ThreadTxnID txnid{txn};

  int i = 0;

  TSCacheReady(&i);
  if ( ! i ) {
    return; // cannot handle new blocks 
  }

  auto &req = txn.getClientRequest();
  auto &hdrs = req.getHeaders();

  auto encodings = hdrs.values(ACCEPT_ENCODING_TAG);
  auto ranges = hdrs.count(RANGE_TAG);

  if ( ranges != 1 ) { 
    return;
  }

  if ( ! encodings.empty() && encodings.find("identity") == encodings.npos ) {
    return; // cannot apply blocks to non-identity encodings
  }

  new BlockSetAccess(txn); // registers itself
}

/////////////////////////////////////////////////
/////////////////////////////////////////////////
BlockSetAccess::BlockSetAccess(Transaction &txn)
  : TransactionPlugin(txn),
    _txn(txn),
    _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
    _url(txn.getClientRequest().getUrl()),
    _clntHdrs(txn.getClientRequest().getHeaders()),
    _clntRangeStr(txn.getClientRequest().getHeaders().values(RANGE_TAG))
{
  DEBUG_LOG("using range detected: %s", _clntRangeStr.c_str());
  TransactionPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_POST_REMAP);
  TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
  txn.addPlugin(this);                  // delete this when done
}

BlockSetAccess::~BlockSetAccess() 
{
  using namespace std::chrono;
  using std::future_status;

  ThreadTxnID txnid{_txn};

  DEBUG_LOG("top delete beginning");

  {
    atscppapi::ScopedContinuationLock lock(_mutexOnlyCont); // to close

    DEBUG_LOG("top delete mid 1");
    _storeXform.reset(); // clear first
    DEBUG_LOG("top delete mid 2");
    _readXform.reset(); // clear next
    DEBUG_LOG("top delete mid 3");
    _keysInRange.clear();
    DEBUG_LOG("top delete mid 4");
  }

  _mutexOnlyCont.reset();
  DEBUG_LOG("top delete end");
}

void
BlockSetAccess::handleReadRequestHeadersPostRemap(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  clean_client_request(); // permit match to a stub-file [disallowed by default]
  txn.resume();
}

void
BlockSetAccess::handleReadCacheLookupComplete(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  auto pstub = get_stub_hdrs(); // get (1) client-req ptr or (2) cached-stub ptr or nullptr

  // file was found in full?
  if (!pstub) {
    txn.resume();
    return; // main file made it through
  }

  if (pstub == &_clntHdrs) {
    DEBUG_LOG("stub-init request");
    txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE, 1); // just permit a range in cached HTTP request
  }

  // not the first request?
  if ( pstub != &_clntHdrs ) 
  {
    // no interest in cacheing a new file... so waive any storage of a reply
    TSHttpTxnServerRespNoStoreSet(_atsTxn,1);

    _etagStr = pstub->value(ETAG_TAG); // hold on for later

    auto srvrRange = pstub->value(CONTENT_RANGE_TAG); // not in clnt hdrs
    auto l = srvrRange.find('/');
    l = ( l != srvrRange.npos ? l : srvrRange.size() ); // zero-length c-string if no '/' found
    _assetLen = std::atol( srvrRange.c_str() + l );
  } else {
    _clntHdrs.erase(RANGE_TAG); // old range is stored already.. but remove it from cached stub
  }

  auto r = parse_range(_clntRangeStr, _assetLen, _beginByte, _endByte);

  if ( _assetLen && r <= 0 ) {
    txn.resume(); // the range is not serviceable! pass through 
    return;
  }

  // a store operation is possible ... but blocks to transform might be available 

  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // clean up headers from request
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS); // clean up headers from earlier

  txn.configIntSet(TS_CONFIG_HTTP_DEFAULT_BUFFER_SIZE,TS_IOBUFFER_SIZE_INDEX_1M); // 1M buffer-size by default
  txn.configIntSet(TS_CONFIG_HTTP_DEFAULT_BUFFER_WATER_MARK,1<<20);               // 1M buffer-wait by default
  txn.configIntSet(TS_CONFIG_NET_SOCK_RECV_BUFFER_SIZE_OUT,1<<20);                // 1M kernel TCP buffer

  auto firstBlk = _beginByte / _blkSize;
  auto endBlk = (_endByte + _blkSize-1) / _blkSize; // round up

  _keysInRange.resize(endBlk - firstBlk); // start empty...
  _vcsToRead.resize(endBlk - firstBlk);   // start empty...

  // store with inclusive end
  _blkRangeStr = "bytes=";
  _blkRangeStr += std::to_string(_blkSize * firstBlk) + "-" + std::to_string(_blkSize * endBlk - 1);

  // cached response could exist with correct etag?
  if ( _assetLen && ! _etagStr.empty() ) {
    // nothing handled until handle_block_tests is done...
    launch_block_tests(); // test if blocks are ready...
    return;
  }

  // no valid file found yet?

  start_cache_miss(firstBlk,endBlk);
  txn.resume();
}

// handled for init-only case
void
BlockSetAccess::handleSendRequestHeaders(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  clean_server_request(txn); // request full blocks if possible
  txn.resume();
}

void
BlockSetAccess::handleReadResponseHeaders(Transaction &txn)
{
  if ( _storeXform ) {
    ThreadTxnID txnid{txn};
    _storeXform->txnReadResponse();
  }
  txn.resume();
}

void
BlockStoreXform::txnReadResponse()
{
  auto blkSz = _ctxt.blockSize();
  auto &txn = _ctxt.txn();
  auto atsTxn = _ctxt.atsTxn();
  auto assetLen = _ctxt.assetLen();
  auto beginByte = _ctxt._beginByte;
  auto rangeLen = _ctxt.rangeLen();

  auto &resp = txn.getServerResponse();
  auto &respHdrs = resp.getHeaders();
  if (resp.getStatusCode() != HTTP_STATUS_PARTIAL_CONTENT) {
    DEBUG_LOG("rejecting due to unusable status: %d",resp.getStatusCode());
    TSHttpTxnServerRespNoStoreSet(atsTxn,1);
    txn.resume();
    return; // cannot use this for cache ...
  }

  auto contentEnc = respHdrs.values(CONTENT_ENCODING_TAG);

  if ( ! contentEnc.empty() && contentEnc != "identity" )
  {
    DEBUG_LOG("rejecting due to unusable encoding: %s",contentEnc.c_str());
    TSHttpTxnServerRespNoStoreSet(atsTxn,1);
    txn.resume();
    return; // cannot use this for range block storage ...
  }

  auto blkByte      = beginByte - ( beginByte % blkSz ); // strict location

  // we have to restart?
  if ( _ctxt._etagStr != respHdrs.value(ETAG_TAG) ) 
  {
    _ctxt._etagStr = respHdrs.value(ETAG_TAG);

    for( auto &&keyp : _ctxt._keysInRange ) {
      auto i = &keyp - &_ctxt._keysInRange.front();
      keyp = std::move(ATSCacheKey(_ctxt.clientUrl(), _ctxt._etagStr, blkByte));
      _vcsToWrite[i].release(); // no future set any more...
      blkByte += blkSz;
    }
  }

  auto srvrRange = respHdrs.value(CONTENT_RANGE_TAG); // not in clnt hdrs
  auto l = srvrRange.find('/');
  l = ( l != srvrRange.npos ? l : srvrRange.size() ); // zero-length c-string if no '/' found
  auto currAssetLen = std::atol( srvrRange.c_str() + l );

  DEBUG_LOG("srvr-resp: len=%#lx olen=%#lx (%ld-%ld) final=%s", currAssetLen, assetLen, beginByte, beginByte + rangeLen, srvrRange.c_str());

  if (currAssetLen != assetLen) {
    _ctxt._assetLen     = static_cast<uint64_t>(currAssetLen);
    DEBUG_LOG("srvr-bitset: blk=%#lx", blkSz);
  }

  resp.setStatusCode(HTTP_STATUS_OK);
  respHdrs.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
  respHdrs.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please notice it!

  DEBUG_LOG("stub-hdrs:\n-------\n%s\n------\n", respHdrs.wireStr().c_str());
  txn.resume();
}

void
BlockSetAccess::handleSendResponseHeaders(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  clean_client_response(txn);
  DEBUG_LOG("client response hdrs:\n----------------%s\n----------------\n", _txn.getClientResponse().getHeaders().wireStr().c_str());
  txn.resume();
}

/////////////////////////////////////////////////////////////

void
BlockSetAccess::clean_client_request()
{
  if ( _clntHdrs.values(ACCEPT_ENCODING_TAG).empty() ) {
    _clntHdrs.append(ACCEPT_ENCODING_TAG, "identity;q=1.0"); // defer to full version
  }

  _clntHdrs.append(ACCEPT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL ";q=0.001"); // but accept block-set too
  _clntHdrs.set(ACCEPT_ENCODING_TAG, _clntHdrs.values(ACCEPT_ENCODING_TAG)); // on one line...
}

void
BlockSetAccess::clean_server_request(Transaction &txn)
{
  auto &proxyReq = txn.getServerRequest().getHeaders();

  auto fields = proxyReq.values(ACCEPT_ENCODING_TAG);
  // is stub-encoding field found?
  if ( fields.find(CONTENT_ENCODING_INTERNAL, fields.rfind(',')) != fields.npos ) {
    fields = fields.substr(0,fields.rfind(',')); // erase after last comma
    proxyReq.set(ACCEPT_ENCODING_TAG,fields);
  }

  if ( ! _clntHdrs.count(IF_MODIFIED_SINCE_TAG) ) {
    proxyReq.erase(IF_MODIFIED_SINCE_TAG); // prevent 304 unless client wants it
  }
  if ( ! _clntHdrs.count(IF_NONE_MATCH_TAG) ) {
    proxyReq.erase(IF_NONE_MATCH_TAG);     // prevent 304 unless client wants it
  }
  if ( ! _clntHdrs.count(IF_RANGE_TAG) ) {
    proxyReq.erase(IF_RANGE_TAG);     // prevent full 200 unless client wants it
  }

  // replace with a block-based range if known
  if (!blockRangeStr().empty()) {
    proxyReq.set(RANGE_TAG, blockRangeStr());  // adjusted range string..
  } else {
    proxyReq.set(RANGE_TAG, clientRangeStr()); // restore as before..
  }
}

void
BlockSetAccess::prepare_cached_stub(Transaction &txn)
{
}

void
BlockSetAccess::clean_client_response(Transaction &txn)
{
  auto &clntRespStatus = txn.getClientResponse();
  auto &clntResp       = txn.getClientResponse().getHeaders();

  auto resp = clntRespStatus.getStatusCode();

  // only change 200-case back to 206
  if ( resp != HTTP_STATUS_OK && resp != HTTP_STATUS_PARTIAL_CONTENT ) {
    return; // cannot do more...
  }

  clntRespStatus.setStatusCode(HTTP_STATUS_PARTIAL_CONTENT);
//  clntResp.erase(X_BLOCK_BITSET_TAG);
  clntResp.erase(CONTENT_ENCODING_TAG);
  clntResp.erase(VARY_TAG);

  // override block-style range
  if (_assetLen && _beginByte >= 0 && _endByte > 0) {
    // change into client-based range and content length
    auto srvrRange =
      std::string() + "bytes " + std::to_string(_beginByte) + "-" + std::to_string(_endByte - 1) + "/" + std::to_string(_assetLen);
    clntResp.set(CONTENT_RANGE_TAG, srvrRange);
    clntResp.set(CONTENT_LENGTH_TAG, std::to_string(rangeLen()));
  } 
}

static int
parse_range(std::string rangeFld, int64_t len, int64_t &start, int64_t &end)
{
  // value of digit-string after last '='
  //    or 0 if no '='
  //    or negative value if no digits and '-' present
  start = std::atol(rangeFld.erase(0, rangeFld.rfind('=')).erase(0, 1).c_str());

  if (!start && (rangeFld.empty() || rangeFld.front() != '0')) {
    --start;
  }

  // negative value of digit-string after last '-'
  //    or 0 if no '=' or leading 0
  end = -std::atol(rangeFld.erase(0, rangeFld.rfind('-')).c_str());

  if (rangeFld.empty()) {
    return --end - start;
  }

  if (start >= 0 && !end) {
    end = len; // end is implied
  } else if (start < 0 && -start == end) {
    start = len + end; // bytes before end
    end   = len;       // end is implied
  } else {
    ++end; // change inclusive to exclusive
  }

  return end - start;
}

Headers *
BlockSetAccess::get_stub_hdrs()
{
  // not even found?
  if (_txn.getCacheStatus() < Txn_t::CACHE_LOOKUP_HIT_STALE) {
    DEBUG_LOG(" unusable cache status: %d", _txn.getCacheStatus());
    return &_clntHdrs;
  }

  DEBUG_LOG("cache-hdrs:\n----------------%s\n----------------\n", _txn.getCachedResponse().getHeaders().wireStr().c_str());

  auto &ccheHdrs = _txn.getCachedResponse().getHeaders();
  auto i         = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
  return (i == std::string::npos ? nullptr : &ccheHdrs);
}

////////////////////////////////////////////////
void default_remap(Url &clntUrl, const Url &from, const Url &to)
{
    // do default replacement..
    clntUrl.setScheme( to.getScheme() );
    clntUrl.setHost( to.getHost() );
    clntUrl.setPort( to.getPort() );

    // insert ahead... [otherwise leave path alone]
    if ( from.getPath().empty() && ! to.getPath().empty() ) {
      clntUrl.setPath( to.getPath() + clntUrl.getPath() );
    }
}

/////////////////////////////////////////////////////////////////////////
/// hooks to activate block access
/////////////////////////////////////////////////////////////////////////
class RemapRangeDetect : public RemapPlugin
{
public:
  using RemapPlugin::RemapPlugin; // same ctor

  // add stub-allowing header if has a valid range
  Result doRemap(const Url &from, const Url &to, Transaction &txn, bool &) override 
  {
    BlockSetAccess::start_if_range_present(txn);
    return RESULT_NO_REMAP;
  }
};

class RangeDetect : public GlobalPlugin
{
public:
  void addHooks() { GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_POST_REMAP); }

  // add stub-allowing header if has a valid range
  void handleReadRequestHeadersPostRemap(Transaction &txn) override 
  {
    BlockSetAccess::start_if_range_present(txn);
    txn.resume();
  }
};

//
// NOTE: needed to start automatic Transaction-obj handling / freeing!!
//
struct GlobalInit : public GlobalPlugin { } __global_init__;

TSReturnCode
TSRemapNewInstance(int, char *[], void **hndl, char *, int)
{
  new RemapRangeDetect(hndl); // for this remap line
  return TS_SUCCESS;
}

void
TSPluginInit(int, const char **)
{
  static std::shared_ptr<RangeDetect> pluginPtr;

  RegisterGlobalPlugin("CPP_Cache_Range_Block", "apache", "dev@trafficserver.apache.org");
  if (!pluginPtr) {
    pluginPtr = std::make_shared<RangeDetect>();
    pluginPtr->addHooks(); // hook for all transactions
  }
}
