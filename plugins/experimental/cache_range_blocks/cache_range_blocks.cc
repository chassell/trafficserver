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

#include "atscppapi/HttpStatus.h"
#include "ts/experimental.h"
#include "ts/InkErrno.h"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

static int parse_range(std::string rangeFld, int64_t len, int64_t &start, int64_t &end);

// namespace
// {

std::shared_ptr<RangeDetect> pluginPtr;

const int8_t base64_values[] = {
  /*0x2b: +*/ 62, /*0x2c,2d,0x2e:*/ ~0,~0,~0, /*0x2f: / */ 63,
  /*0x30-0x39: 0-9*/ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, ~0,~0,~0,~0,~0,~0,
  ~0, /*0x41-0x5a: A-Z*/ 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25, ~0,~0,~0,~0,~0,
  ~0, /*0x61-0x6a: a-z*/ 26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51
};

const char *const base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/////////////////////////////////////////
void
RangeDetect::handleReadRequestHeadersPostRemap(Transaction &txn)
{
  // must avoid the stub file *always* unless warranted
  txn.configIntSet(TS_CONFIG_HTTP_CACHE_IGNORE_ACCEPT_ENCODING_MISMATCH,0);

  auto &clntReq = txn.getClientRequest().getHeaders();
  if ( clntReq.count(RANGE_TAG) != 1 ) {
    return; // only use single-range requests
  }

  // allow a cache-write for this range request ... if needed
  txn.configIntSet(TS_CONFIG_HTTP_CACHE_RANGE_WRITE,1);

  auto &txnPlugin = *new BlockSetAccess(txn); // plugin attach
  txn.addPlugin(&txnPlugin); // delete this when done
  txnPlugin.handleReadRequestHeadersPostRemap(txn);
}

BlockSetAccess::BlockSetAccess(Transaction &txn)
   : TransactionPlugin(txn),
     _txn(txn),
     _atsTxn(static_cast<TSHttpTxn>(txn.getAtsHandle())),
     _url(txn.getClientRequest().getUrl()),
     _clntHdrs(txn.getClientRequest().getHeaders()),
     _clntRangeStr(txn.getClientRequest().getHeaders().values(RANGE_TAG))
{
  TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
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
  auto pstub = get_trunc_hdrs(txn); // get (1) client-req ptr or (2) cached-stub ptr or nullptr

  if ( ! pstub ) {
    /// TODO delete old stub file in case of revalidate!
    return; // main file made it through
  }

  if ( pstub == &_clntHdrs ) {
    DEBUG_LOG("cache-init: len=%lu set=%s",_assetLen,_b64BlkBitset.c_str());
    _initXform = std::make_unique<BlockInitXform>(txn,*this);
    _initXform->handleReadCacheLookupComplete(txn); // [default version]
    return;
  }

  // "stale" cache until all blocks proven ready
  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_HIT_STALE);

  // simply clean up stub-response to be a normal header
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);

  // see if valid stub headers
  auto srvrRange = pstub->value(CONTENT_RANGE_TAG); // not in clnt hdrs

  srvrRange.erase(0,srvrRange.find('/')).erase(0,1);
  _assetLen = std::atol(srvrRange.c_str());

  // test if range is readable
  if ( parse_range(_clntRangeStr, _assetLen, _beginByte, _endByte) >= 0 ) {
    _b64BlkBitset = pstub->value(X_BLOCK_BITSET_TAG);
  }
 
  auto chk = std::find_if(_b64BlkBitset.begin(),_b64BlkBitset.end(),[](char c) { return ! isalnum(c) && c != '+' && c !='/'; });

  // invalid stub file... (or client headers instead)
  if ( ! _assetLen || _b64BlkBitset.empty() || chk != _b64BlkBitset.end() ) {
    DEBUG_LOG("cache-stub-fail: len=%lu set=%s",_assetLen,_b64BlkBitset.c_str());
    _initXform = std::make_unique<BlockInitXform>(txn,*this);
    _initXform->handleReadCacheLookupComplete(txn); // [default version]
    return;
  }

  _blkSize = INK_ALIGN((_assetLen>>10)|1,MIN_BLOCK_STORED);

  // all blocks [and keys for them] are valid to try reading?
  if ( ! have_needed_blocks() || _keysInRange.empty() ) {
    DEBUG_LOG("cache-resp-wr: base:%p len=%lu len=%lu set=%s",this,assetLen(),_assetLen,_b64BlkBitset.c_str());

    // intercept data for new or updated stub version
    _storeXform = std::make_unique<BlockStoreXform>(txn,*this);
    _storeXform->handleReadCacheLookupComplete(txn); // [default version]
    return;
  }

  DEBUG_LOG("cache-resp-rd: len=%lu set=%s",_assetLen,_b64BlkBitset.c_str());
  _vcsToRead.resize( _keysInRange.size() );

  // stateless callback as deleter...
  using Deleter_t = void(*)(BlockSetAccess*);
  static const Deleter_t deleter = [](BlockSetAccess *ptr) {
     ptr->handleBlockTests();  // recurse from last one
  };

  // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
  auto barrierLock = std::shared_ptr<BlockSetAccess>(this, deleter);
  auto mutex = TSMutexCreate(); // one shared mutex across reads

  for( auto i = 0U ; i < _keysInRange.size() ; ++i ) {
    // prep for async reads that init into VConn-futures
    // XXX: pass a use-for-all lambda-ref instead???
    auto contp = APICont::create_temp_tscont(mutex, _vcsToRead[i], barrierLock);
    TSCacheRead(contp,_keysInRange[i]);
  }
}

/////////////////////////////////////////////////////////////

void BlockSetAccess::clean_client_request()
{
  _clntHdrs.append(ACCEPT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL ";q=0.001");
  _clntHdrs.append(ACCEPT_ENCODING_TAG,"*;q=1");
}

void BlockSetAccess::clean_server_request(Transaction &txn)
{
  auto &proxyReq = txn.getServerRequest().getHeaders();

  // TODO: erase only last fields
  proxyReq.erase(ACCEPT_ENCODING_TAG);
  proxyReq.erase(IF_MODIFIED_SINCE_TAG); // actually getting data
  proxyReq.erase(IF_NONE_MATCH_TAG); // actually getting data

  // replace with a block-based range if known
  if ( ! blockRangeStr().empty() ) {
     proxyReq.set(RANGE_TAG, blockRangeStr()); 
  }
}

void BlockSetAccess::clean_server_response(Transaction &txn)
{
  auto &proxyRespStatus = txn.getServerResponse();
  auto &proxyResp = txn.getServerResponse().getHeaders();

  if ( proxyRespStatus.getStatusCode() != HTTP_STATUS_PARTIAL_CONTENT ) {
    return; // cannot clean this up...
  }

  // see if valid stub headers
  auto srvrRange = proxyResp.value(CONTENT_RANGE_TAG); // not in clnt hdrs
  auto srvrRangeCopy = srvrRange;

  srvrRange.erase(0,srvrRange.find('/')).erase(0,1);
  auto currAssetLen = std::atol(srvrRange.c_str());

  DEBUG_LOG("srvr-resp: len=%ld olen=%lu final=%s range=%s",currAssetLen,_assetLen,srvrRange.c_str(),srvrRangeCopy.c_str());

  if ( static_cast<uint64_t>(currAssetLen) != _assetLen ) {
    _blkSize = INK_ALIGN((currAssetLen>>10)|1,MIN_BLOCK_STORED);
    _b64BlkBitset = std::string( (currAssetLen+_blkSize*6-1 )/(_blkSize*6), 'A');
    DEBUG_LOG("srvr-bitset: blk=%lu %s",_blkSize,_b64BlkBitset.c_str());
  }

  proxyRespStatus.setStatusCode(HTTP_STATUS_OK);
//  proxyResp.erase(CONTENT_RANGE_TAG); // erase to remove range worries
  proxyResp.set(CONTENT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL); // promote matches
  proxyResp.set(X_BLOCK_BITSET_TAG, _b64BlkBitset); // TODO: not good enough for huge files!!

  DEBUG_LOG("srvr-hdrs:\n%s\n------\n",proxyResp.wireStr().c_str());
}

void BlockSetAccess::clean_client_response(Transaction &txn)
{
  auto &clntRespStatus = txn.getClientResponse();
  auto &clntResp = txn.getClientResponse().getHeaders();

  // override block-style range
  if ( _assetLen && _endByte ) {
    clntResp.set(CONTENT_RANGE_TAG, std::to_string(_beginByte) + "-" + std::to_string(_endByte-1) + "/" + std::to_string(_assetLen));
  }

  // only change 200-case back to 206
  if ( clntRespStatus.getStatusCode() == HTTP_STATUS_OK ) { 
    clntRespStatus.setStatusCode( HTTP_STATUS_PARTIAL_CONTENT );
  }

  // clntResp.erase("Warning"); // erase added proxy-added warning

  // TODO erase only last field, with internal encoding
  clntResp.erase(CONTENT_ENCODING_TAG);
  clntResp.erase(X_BLOCK_BITSET_TAG);
}

static int 
parse_range(std::string rangeFld, int64_t len, int64_t &start, int64_t &end)
{
  // value of digit-string after last '=' 
  //    or 0 if no '=' 
  //    or negative value if no digits and '-' present
  start = std::atol( rangeFld.erase(0,rangeFld.rfind('=')).erase(0,1).c_str() );

  if ( ! start && ( rangeFld.empty() || rangeFld.front() != '0' ) ) {
    --start;
  }

  // negative value of digit-string after last '-'
  //    or 0 if no '=' or leading 0
  end = - std::atol( rangeFld.erase(0,rangeFld.rfind('-')).c_str());

  if ( rangeFld.empty() ) {
    return --end;
  }

  if ( start >= 0 && ! end ) {
    end = len; // end is implied
  } else if ( start < 0 && -start == end ) {
    start = len + end; // bytes before end
    end = len; // end is implied
  } else {
    ++end; // change inclusive to exclusive
  }

  return end;
}

Headers *
BlockSetAccess::get_trunc_hdrs(Transaction &txn) 
{
   // not even found?
   if (txn.getCacheStatus() < Txn_t::CACHE_LOOKUP_HIT_STALE ) {
     DEBUG_LOG(" unusable: %d",txn.getCacheStatus());
     return &_clntHdrs;
   }

   DEBUG_LOG("cache-hdrs:\n%s\n------\n",txn.getCachedResponse().getHeaders().wireStr().c_str());

   auto &ccheHdrs = txn.getCachedResponse().getHeaders();
   auto i = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
   return ( i == std::string::npos ? nullptr : &ccheHdrs );
}

uint64_t 
BlockSetAccess::have_needed_blocks()
{
  int64_t start = _beginByte;
  int64_t end = _endByte;

  auto startBlk = start /_blkSize; // inclusive-start block
  auto endBlk = (end+1+_blkSize-1)/_blkSize; // exclusive-end block

//  ink_assert( _contentLen == static_cast<int64_t>(( endBlk - startBlk )*_blkSize) - _beginByte - _endByte );

  // store with inclusive end
  _blkRangeStr = "bytes=";
  _blkRangeStr += std::to_string(_blkSize*startBlk) + "-" + std::to_string(_blkSize*endBlk-1); 

  auto misses = 0;

  for( auto i = startBlk ; i < endBlk ; ++i ) 
  {
    // fill keys only of set bits...
    if ( ! is_base64_bit_set(_b64BlkBitset,i) ) {
      ++misses;
    }
  }

  // any missed
  if ( misses ) {
   return 0; // don't have all of them
  }

  return end;
}

// }

// tsapi TSReturnCode TSBase64Decode(const char *str, size_t str_len, unsigned char *dst, size_t dst_size, size_t *length);
// tsapi TSReturnCode TSBase64Encode(const char *str, size_t str_len, char *dst, size_t dst_size, size_t *length);
void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
  if ( ! pluginPtr ) {
    pluginPtr = std::make_shared<RangeDetect>();
    pluginPtr->addHooks();
  }
}
