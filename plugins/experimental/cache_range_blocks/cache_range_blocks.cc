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

#define PLUGIN_NAME "cache_range_blocks"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

static int parse_range(std::string rangeFld, int64_t len, int64_t &start, int64_t &end);

const int8_t base64_values[80] = {
  /* 0-4 */ /*0x2b: +*/ 62, /*0x2c,2d,0x2e:*/ ~0, ~0, ~0, /*0x2f: / */ 63,
  /* 5-14 */  /*0x30-0x39: 0-9*/ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 
  /* 15-21 */   ~0, ~0, ~0, ~0, ~0, ~0, ~0,
  /* 22-47 */ /*0x41-0x5a: A-Z*/ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
  /* 48-53 */   ~0, ~0, ~0, ~0, ~0, ~0,
  /* 54-79 */ /*0x61-0x6a: a-z*/ 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
}; 

const char base64_chars[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockInitXform : public TransactionPlugin
{
public:
  BlockInitXform(BlockSetAccess &ctxt) : TransactionPlugin(ctxt.txn()), _ctxt(ctxt)
  {
  }

  // change to 200 and append stub-file headers...
private:
  BlockSetAccess &_ctxt;
};


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

void
BlockSetAccess::handleReadRequestHeadersPostRemap(Transaction &txn)
{
  clean_client_request(); // add allowance for stub-file's encoding
  txn.resume();
}

void
BlockSetAccess::handleReadCacheLookupComplete(Transaction &txn)
{
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
    reset_cached_stub(txn);
    txn.resume();
    return;
  }

  _clntHdrs.erase(RANGE_TAG);

  _etagStr = pstub->value(ETAG_TAG); // hold on for later

  // see if valid stub headers
  auto srvrRange = pstub->value(CONTENT_RANGE_TAG); // not in clnt hdrs

  srvrRange.erase(0, srvrRange.find('/')).erase(0, 1);
  _assetLen = std::atol(srvrRange.c_str());

  // test if range is readable
  if (parse_range(_clntRangeStr, _assetLen, _beginByte, _endByte) >= 0) {
    _b64BlkBitset = pstub->value(X_BLOCK_BITSET_TAG);
  }

  auto chk = std::find_if(_b64BlkBitset.begin(), _b64BlkBitset.end(), [](char c) { return !isalnum(c) && c != '+' && c != '/'; });

  // invalid stub file... (or client headers instead)
  if (!_assetLen || _b64BlkBitset.empty() || chk != _b64BlkBitset.end()) {
    DEBUG_LOG("stub-failed: len=%#lx set=%s", _assetLen, _b64BlkBitset.c_str());
    reset_cached_stub(txn);
    txn.resume();
    return;
  }

  _blkSize = INK_ALIGN((_assetLen >> 10) | 1, MIN_BLOCK_STORED);

  if (select_needed_blocks()) {
    DEBUG_LOG("read guaranteed: len=%#lx set=%s", _assetLen, _b64BlkBitset.c_str());
  }

  /*
    // all blocks [and keys for them] are valid to try reading?
    if ( ! have_needed_blocks() || _keysInRange.empty() ) {
      DEBUG_LOG("cache-resp-wr: base:%p len=%#lx len=%#lx set=%s",this,assetLen(),_assetLen,_b64BlkBitset.c_str());

      // intercept data for new or updated stub version
      _storeXform = std::make_unique<BlockStoreXform>(*this);
      _storeXform->handleReadCacheLookupComplete(txn); // [default version]
      // resume implied
      return;
    }
  */

  // stateless callback as deleter...
  using Deleter_t                = void (*)(BlockSetAccess *);
  static const Deleter_t deleter = [](BlockSetAccess *ptr) {
    ptr->handleBlockTests(); // recurse from last one
  };

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockSetAccess>(this, deleter);
  auto mutex       = TSMutexCreate(); // one shared mutex across reads
  auto blkNum      = _beginByte / _blkSize;

  for (auto i = 0U; i < _keysInRange.size(); ++i, ++blkNum) {
    // prep for async reads that init into VConn-futures
    _keysInRange[i] = std::move(APICacheKey(clientUrl(), _etagStr, blkNum * _blkSize));
    auto contp      = APICont::create_temp_tscont(mutex, _vcsToRead[i], barrierLock);
    TSCacheRead(contp, _keysInRange[i]);
  }

  // *no* txn.resume()
}

// handled for init-only case
void
BlockSetAccess::handleSendRequestHeaders(Transaction &txn)
{
  clean_server_request(txn); // request full blocks if possible
  txn.resume();
}

// handled for init-only case
void
BlockSetAccess::handleReadResponseHeaders(Transaction &txn)
{
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

  prepare_cached_stub(txn); // request full blocks if possible
  txn.resume();
}

void
BlockSetAccess::handleSendResponseHeaders(Transaction &txn)
{
  clean_client_response(txn);
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
  fields = fields.substr(0,fields.rfind(',')); // erase after last comma
  proxyReq.set(ACCEPT_ENCODING_TAG,fields);

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
    _blkSize      = INK_ALIGN((currAssetLen >> 10) | 1, MIN_BLOCK_STORED);
    _b64BlkBitset = std::string((currAssetLen + _blkSize * 6 - 1) / (_blkSize * 6), 'A');
    _assetLen     = static_cast<uint64_t>(currAssetLen);
    DEBUG_LOG("srvr-bitset: blk=%#lx %s", _blkSize, _b64BlkBitset.c_str());
  }

  proxyRespStatus.setStatusCode(HTTP_STATUS_OK);
  proxyResp.set(CONTENT_ENCODING_TAG, CONTENT_ENCODING_INTERNAL); // promote matches
  proxyResp.append(VARY_TAG,ACCEPT_ENCODING_TAG); // please notice it!
  proxyResp.set(X_BLOCK_BITSET_TAG, _b64BlkBitset); // keep as *last* field

  DEBUG_LOG("stub-hdrs:\n-------\n%s\n------\n", proxyResp.wireStr().c_str());
}

void
BlockSetAccess::clean_client_response(Transaction &txn)
{
  auto &clntRespStatus = txn.getClientResponse();
  auto &clntResp       = txn.getClientResponse().getHeaders();

  // only change 200-case back to 206
  if (clntRespStatus.getStatusCode() != HTTP_STATUS_OK) {
    return; // cannot do more...
  }

  clntRespStatus.setStatusCode(HTTP_STATUS_PARTIAL_CONTENT);
  clntResp.erase(X_BLOCK_BITSET_TAG);
  clntResp.erase(CONTENT_ENCODING_TAG);

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
    return --end;
  }

  if (start >= 0 && !end) {
    end = len; // end is implied
  } else if (start < 0 && -start == end) {
    start = len + end; // bytes before end
    end   = len;       // end is implied
  } else {
    ++end; // change inclusive to exclusive
  }

  return end;
}

Headers *
BlockSetAccess::get_stub_hdrs()
{
  // not even found?
  if (_txn.getCacheStatus() < Txn_t::CACHE_LOOKUP_HIT_STALE) {
    DEBUG_LOG(" unusable cache status: %d", _txn.getCacheStatus());
    return &_clntHdrs;
  }

  DEBUG_LOG("cache-hdrs:\n%s\n------\n", _txn.getCachedResponse().getHeaders().wireStr().c_str());

  auto &ccheHdrs = _txn.getCachedResponse().getHeaders();
  auto i         = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
  return (i == std::string::npos ? nullptr : &ccheHdrs);
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

  for (; i < endBlk && is_base64_bit_set(_b64BlkBitset, i); ++i) {
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
class RemapRangeDetect : public RemapPlugin
{
public:
  using RemapPlugin::RemapPlugin; // same ctor

  // add stub-allowing header if has a valid range
  Result doRemap(const Url &from, const Url &to, Transaction &txn, bool &) override 
  {
    auto &req = txn.getClientRequest();
    auto &hdrs = req.getHeaders();

    if (hdrs.count(RANGE_TAG) == 1) {
      new BlockSetAccess(txn); // registers itself
    }
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
    // *must* avoid the stub file 
    txn.getClientRequest().getHeaders().append(VARY_TAG, CONTENT_ENCODING_INTERNAL); // need for correct

    auto &clntReq = txn.getClientRequest().getHeaders();
    if (clntReq.count(RANGE_TAG) == 1) {
      new BlockSetAccess(txn); // registers itself
    }
    txn.resume();
  }
};

struct GlobalInit : public GlobalPlugin
{
 public:
  GlobalInit() : GlobalPlugin(true) { }
} __global_init__;


TSReturnCode
TSRemapNewInstance(int, char *[], void **hndl, char *, int)
{
  new RemapRangeDetect(hndl); // registers itself
  return TS_SUCCESS;
}

void
TSPluginInit(int, const char **)
{
  static std::shared_ptr<RangeDetect> pluginPtr;

  RegisterGlobalPlugin("CPP_Cache_Range_Block", "apache", "dev@trafficserver.apache.org");
  if (!pluginPtr) {
    pluginPtr = std::make_shared<RangeDetect>();
    pluginPtr->addHooks();
  }
}
