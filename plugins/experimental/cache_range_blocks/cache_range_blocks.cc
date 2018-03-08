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

  // file was found?!
  if (!pstub) {
    txn.resume();
    return; // main file made it through
  }

  // simply clean up stub-response to be a normal header
  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // clean up headers from request
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS); // clean up headers from earlier

  if (pstub == &_clntHdrs) {
    DEBUG_LOG("stub-init only");
    txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE, 1); // permit range in cached-request
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // handle reply for stub-file storage
    txn.resume();
    return;
  }

  // no interest in cacheing the file... so waive any storage
  TSHttpTxnServerRespNoStoreSet(_atsTxn,1);

  txn.configIntSet(TS_CONFIG_HTTP_DEFAULT_BUFFER_SIZE,TS_IOBUFFER_SIZE_INDEX_1M); // 1M buffer-size by default
  txn.configIntSet(TS_CONFIG_HTTP_DEFAULT_BUFFER_WATER_MARK,1<<20);               // 1M buffer-wait by default
  txn.configIntSet(TS_CONFIG_NET_SOCK_RECV_BUFFER_SIZE_OUT,1<<20);                // 1M kernel TCP buffer

  _clntHdrs.erase(RANGE_TAG);

  _etagStr = pstub->value(ETAG_TAG); // hold on for later

  // see if valid stub headers
  auto srvrRange = pstub->value(CONTENT_RANGE_TAG); // not in clnt hdrs

  srvrRange.erase(0, srvrRange.find('/')).erase(0, 1);
  _assetLen = std::atol(srvrRange.c_str());

  // test if range is readable
  if (parse_range(_clntRangeStr, _assetLen, _beginByte, _endByte) > 0) {
//    _blkSize = INK_ALIGN((_assetLen >> 10) | 1, MIN_BLOCK_STORED);
//    _blkSize = std::min( _blkSize+0 , 1L<<21 );  // 2Meg max size
    _blkSize = 1L<<20;  // 1Meg standard size
    _b64BlkPresent = std::string((_assetLen + _blkSize*6 - 1) / (_blkSize * 6), 'A');
    _b64BlkUsable = _b64BlkPresent;
  }

  auto chk = std::find_if(_b64BlkPresent.begin(), _b64BlkPresent.end(), [](char c) { return !isalnum(c) && c != '+' && c != '/'; });

  // invalid stub file... (or client headers instead)
  if (!_assetLen || !_blkSize || _b64BlkPresent.empty() || chk != _b64BlkPresent.end()) {
    DEBUG_LOG("stub-failed: len=%#lx [blk:%ldK] set=%s", _assetLen, _blkSize/1024, _b64BlkPresent.c_str());
    txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE, 1); // permit range in cached-request
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // handle reply for stub-file storage
    txn.resume();
    return;
  }

  if (!select_needed_blocks()) {
    DEBUG_LOG("write is likely needed: len=%#lx [blk:%ldK] set=%s", _assetLen, _blkSize/1024, _b64BlkPresent.c_str());
  }

  // nothing handled until handle_block_tests is done...
  launch_block_tests();
}

// handled for init-only case
void
BlockSetAccess::handleSendRequestHeaders(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  clean_server_request(txn); // request full blocks if possible
  txn.resume();
}

// handled for init-only case
void
BlockSetAccess::handleReadResponseHeaders(Transaction &txn)
{
  ThreadTxnID txnid{_txn};
  auto &resp = txn.getServerResponse();
  auto &respHdrs = resp.getHeaders();
  if (resp.getStatusCode() != HTTP_STATUS_PARTIAL_CONTENT) {
    DEBUG_LOG("rejecting due to unusable status: %d",resp.getStatusCode());
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1);
    txn.resume();
    return; // cannot use this for cache ...
  }

  auto contentEnc = respHdrs.values(CONTENT_ENCODING_TAG);

  if ( ! contentEnc.empty() && contentEnc != "identity" )
  {
    DEBUG_LOG("rejecting due to unusable encoding: %s",contentEnc.c_str());
    TSHttpTxnServerRespNoStoreSet(atsTxn(),1);
    txn.resume();
    return; // cannot use this for range block storage ...
  }

  prepare_cached_stub(txn); // prepare to be stored
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
  auto &proxyRespStatus = txn.getServerResponse();
  auto &proxyResp       = txn.getServerResponse().getHeaders();

  // see if valid stub headers
  auto srvrRange     = proxyResp.value(CONTENT_RANGE_TAG); // not in clnt hdrs
  auto srvrRangeCopy = srvrRange;

  srvrRange.erase(0, srvrRange.find('/')).erase(0, 1);
  auto currAssetLen = std::atol(srvrRange.c_str());

  DEBUG_LOG("srvr-resp: len=%#lx olen=%#lx final=%s range=%s", currAssetLen, _assetLen, srvrRange.c_str(), srvrRangeCopy.c_str());

  if (currAssetLen != _assetLen) {
//    _blkSize      = INK_ALIGN((currAssetLen >> 10) | 1, MIN_BLOCK_STORED);
//    _blkSize = std::min( _blkSize+0 , 1L<<21 );  // 2Meg max size
    _blkSize = 1L<<20;  // 1Meg standard size
    _b64BlkPresent = std::string((currAssetLen + _blkSize*6 - 1) / (_blkSize * 6), 'A');
    _b64BlkUsable = _b64BlkPresent;
    _assetLen     = static_cast<uint64_t>(currAssetLen);
    DEBUG_LOG("srvr-bitset: blk=%#lx %s", _blkSize, _b64BlkPresent.c_str());
  }

  proxyRespStatus.setStatusCode(HTTP_STATUS_OK);
  proxyResp.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
  proxyResp.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please notice it!
//  proxyResp.set(X_BLOCK_BITSET_TAG, _b64BlkUsable); // keep as *last* field

  DEBUG_LOG("stub-hdrs:\n-------\n%s\n------\n", proxyResp.wireStr().c_str());
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


std::string
BlockSetAccess::b64BlkUsableSubstr() const
{
  size_t i = _beginByte/_blkSize/6;
  size_t j = _endByte/_blkSize/6;
  return ( _b64BlkUsable.size() >= j ? _b64BlkUsable.substr(i,j - i) : "" );
}

std::string
BlockSetAccess::b64BlkPresentSubstr() const
{
  size_t i = _beginByte/_blkSize/6;
  size_t j = _endByte/_blkSize/6;
  return ( _b64BlkPresent.size() >= j ? _b64BlkPresent.substr(i,j - i) : "" );
}

int64_t
BlockSetAccess::select_needed_blocks()
{
  int64_t start = _beginByte;
  int64_t end   = _endByte;

  auto startBlk = start / _blkSize;                // inclusive-start block
  auto endBlk   = (end + _blkSize - 1) / _blkSize; // exclusive-end block

  _keysInRange.resize(endBlk - startBlk); // start empty...
  _vcsToRead.resize(endBlk - startBlk);   // start empty...

  //  ink_assert( _contentLen == static_cast<int64_t>(( endBlk - startBlk )*_blkSize) - _beginByte - _endByte );

  // store with inclusive end
  _blkRangeStr = "bytes=";
  _blkRangeStr += std::to_string(_blkSize * startBlk) + "-" + std::to_string(_blkSize * endBlk - 1);

  auto i = startBlk;

  for (; i < endBlk && is_base64_bit_set(_b64BlkUsable, i); ++i) {
  }

  // any missed
  if (i < endBlk) {
    return 0; // don't have all of them
  }

  return end;
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
