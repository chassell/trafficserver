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

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%s:%d] %s(): " fmt, __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

// namespace
// {

GlobalPlugin *plugin;
//std::shared_ptr<GlobalPlugin> pluginPtr;

constexpr int8_t base64_values[] = {
  /*0x2b: +*/ 62, /*0x2c,2d,0x2e:*/ ~0,~0,~0, /*0x2f: / */ 63,
  /*0x30-0x39: 0-9*/ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, ~0,~0,~0,~0,~0,~0,
  ~0, /*0x41-0x5a: A-Z*/ 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25, ~0,~0,~0,~0,~0,
  ~0, /*0x61-0x6a: a-z*/ 26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51
};

constexpr const char *base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static inline void base64_bit_clr(std::string &base64,unsigned i)
{
   auto &c = base64[i/6];
   c = base64_chars[~(1<<(i%6)) & base64_values[c -'+']];
}

static inline void base64_bit_set(std::string &base64,unsigned i)
{
   auto &c = base64[i/6];
   c = base64_chars[(1<<(i%6)) | base64_values[c -'+']];
}

static inline bool is_base64_bit_set(const std::string &base64,unsigned i)
{
   return (1<<(i%6)) & base64_values[base64[i/6]-'+'];
}

void BlockSetAccess::clean_client_request()
{
  // allow to try using "stub" instead of a MISS
  _clntHdrs.append(ACCEPT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL ";q=0.001");
  _clntHdrs.erase(RANGE_TAG); // prep for stub
}

void BlockSetAccess::clean_server_request(Transaction &txn)
{
  auto &proxyReq = txn.getServerRequest().getHeaders();

  // TODO erase only last field, with internal encoding
  proxyReq.erase(ACCEPT_ENCODING_TAG);
  proxyReq.erase(IF_MODIFIED_SINCE_TAG); // revalidate must be done locally
  proxyReq.erase(IF_NONE_MATCH_TAG); // revalidate must be done locally

  // request a block-based range if knowable
  if ( ! blockRange().empty() ) {
     proxyReq.set(RANGE_TAG, blockRange()); 
  } else {
     proxyReq.set(RANGE_TAG, clientRange());
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
  auto currAssetLen = std::atoll(srvrRange.c_str());

  DEBUG_LOG("srvr-resp: len=%lld final=%s range=%s",currAssetLen,srvrRange.c_str(),srvrRangeCopy.c_str());

  if ( currAssetLen != _assetLen ) {
    _blkSize = INK_ALIGN((currAssetLen>>10)|1,MIN_BLOCK_STORED);
    _b64BlkBitset = std::string( (currAssetLen+_blkSize*6-1 )/(_blkSize*6), 'A');
    DEBUG_LOG("srvr-bitset: blk=%lu %s",_blkSize,_b64BlkBitset.c_str());
  }

  proxyRespStatus.setStatusCode(HTTP_STATUS_OK);

  proxyResp.set(CONTENT_ENCODING_TAG,CONTENT_ENCODING_INTERNAL);
  proxyResp.set(X_BLOCK_BITSET_TAG, _b64BlkBitset); // TODO: not good enough for huge files!!

  DEBUG_LOG("srvr-hdrs:\n%s\n------\n",proxyResp.wireStr().c_str());
}

void BlockSetAccess::clean_client_response(Transaction &txn)
{
  auto &clntRespStatus = txn.getClientResponse();
  auto &clntResp = txn.getClientResponse().getHeaders();

  // override block-style range
  if ( ! _respRange.empty() ) {
    clntRespStatus.setStatusCode( HTTP_STATUS_PARTIAL_CONTENT );
    clntResp.set(CONTENT_RANGE_TAG, _respRange); // restore
  }

  clntResp.erase("Warning"); // erase added proxy-added warning

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
  start = std::atoll( rangeFld.erase(0,rangeFld.rfind('=')).erase(0,1).c_str() );

  if ( ! start && ( rangeFld.empty() || rangeFld.front() != '0' ) ) {
    --start;
  }

  // negative value of digit-string after last '-'
  //    or 0 if no '=' or leading 0
  end = - std::atoll( rangeFld.erase(0,rangeFld.rfind('-')).c_str());

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
BlockSetAccess::get_stub_hdrs(Transaction &txn) 
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
  int64_t start = 0;
  int64_t end = 0;

  if ( parse_range(_clntRange, _assetLen, start, end) < 0 ) {
    return 0; // range is unreadable
  }

  _respRange = std::to_string(start) + "-" + std::to_string(end-1) + "/" + std::to_string(_assetLen);

  auto startBlk = start /_blkSize; // inclusive-start block
  auto endBlk = (end+1+_blkSize-1)/_blkSize; // exclusive-end block

  _firstBlkSkip = start - startBlk*_blkSize; // may be zero
  _lastBlkTrunc = endBlk*_blkSize - end; // may be zero

//  ink_assert( _contentLen == static_cast<int64_t>(( endBlk - startBlk )*_blkSize) - _firstBlkSkip - _lastBlkTrunc );

  // store with inclusive end
  _blkRange = "bytes=";
  _blkRange += std::to_string(_blkSize*startBlk) + "-" + std::to_string(_blkSize*endBlk-1); 

  _keysInRange.resize( endBlk - startBlk );

  auto misses = 0;

  for( auto i = startBlk ; i < endBlk ; ++i ) 
  {
    // fill keys only of set bits...
    if ( ! is_base64_bit_set(_b64BlkBitset,i) ) {
      _keysInRange[i-startBlk] = APICacheKey(_url,i*_blkSize);
      ++misses;
    }
  }

  // any missed
  if ( misses ) {
   return 0; // don't have all of them
  }

  // write all of them
  for( auto i = startBlk ; i < endBlk ; ++i ) {
   _keysInRange[i-startBlk] = APICacheKey(_url,i*_blkSize);
  }

  return end;
}

void
BlockSetAccess::handleReadCacheLookupComplete(Transaction &txn)
{
  auto pstub = get_stub_hdrs(txn); // get (1) client-req ptr or (2) cached-stub ptr or nullptr

  if ( ! pstub ) {
    /// TODO delete old stub file in case of revalidate!
    return; // main file made it through
  }

  if ( pstub == &_clntHdrs ) {
    DEBUG_LOG("cache-init: len=%lu set=%s",_assetLen,_b64BlkBitset.c_str());
    _xform = std::unique_ptr<Plugin>(new BlockInitXform(txn,*this));
    _xform->handleReadCacheLookupComplete(txn); // [default version]
    return;
  }

  // "stale" cache until all blocks proven ready
  TSHttpTxnCacheLookupStatusSet(atsTxn(), TS_CACHE_LOOKUP_HIT_STALE);

  // simply clean up stub-response to be a normal header
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);

  // see if valid stub headers
  auto srvrRange = pstub->value(CONTENT_RANGE_TAG); // not in clnt hdrs

  srvrRange.erase(0,srvrRange.find('/')).erase(0,1);
  auto _assetLen = std::atoll(srvrRange.c_str());
 
  _b64BlkBitset = pstub->value(X_BLOCK_BITSET_TAG);
  auto chk = std::find_if(_b64BlkBitset.begin(),_b64BlkBitset.end(),[](char c) { return ! isalnum(c) && c != '+' && c !='/'; });

  // invalid stub file... (or client headers instead)
  if ( ! _assetLen || _b64BlkBitset.empty() || chk != _b64BlkBitset.end() ) {
    DEBUG_LOG("cache-stub-fail: len=%llu set=%s",_assetLen,_b64BlkBitset.c_str());
    _xform = std::unique_ptr<Plugin>(new BlockInitXform(txn,*this));
    _xform->handleReadCacheLookupComplete(txn); // [default version]
    return;
  }

  _blkSize = INK_ALIGN((_assetLen>>10)|1,MIN_BLOCK_STORED);

  DEBUG_LOG("cache-resp: len=%llu set=%s",_assetLen,_b64BlkBitset.c_str());

  // all blocks [and keys for them] are valid to try reading?
  if ( have_needed_blocks() && ! _keysInRange.empty() ) {
    _xform = std::unique_ptr<Plugin>(new BlockReadXform(txn,*this));
    // first test that blocks are ready and correctly sized
    _xform->handleReadCacheLookupComplete(txn);
    return;
  }

  // intercept data for new or updated stub version
  _xform = std::unique_ptr<Plugin>(new BlockStoreXform(txn,*this));
  _xform->handleReadCacheLookupComplete(txn);
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  // [will override the server response for headers]
  auto &cachhdrs = txn.updateCachedResponse().getHeaders();
  auto b64Bitset = _ctxt.b64BlkBitset();

  auto &keys = _ctxt.keysInRange();
  for( auto i = 0U ; i < keys.size() ; ++i ) {
    if ( keys[i] ) {
      // prep for async write that inits into VConn-futures 
      auto contp = APICont::create_temp_tscont(_vcsToWrite[i],nullptr);
      TSCacheWrite(contp,keys[i]); // find room to store...
      base64_bit_set(b64Bitset,i); // set a bit
    }
  }
  cachhdrs.set(X_BLOCK_BITSET_TAG,b64Bitset); // attempt to erase/rewrite field in headers
  DEBUG_LOG("updated bitset: %s",b64Bitset.c_str());
  DEBUG_LOG("updated cache-hdrs:\n%s\n------\n",cachhdrs.wireStr().c_str());
  DEBUG_LOG("updated cache-hdrs:\n%s\n------\n",txn.getCachedResponse().getHeaders().wireStr().c_str());
  txn.resume(); // done 
}

void
BlockStoreXform::handleReadResponseHeaders(Transaction &txn)
{
   DEBUG_LOG("updated cache-hdrs:\n%s\n------\n",txn.updateCachedResponse().getHeaders().wireStr().c_str());
   _ctxt.clean_server_response(txn);
   txn.resume();
}

// start read of all the blocks
void
BlockReadXform::handleReadCacheLookupComplete(Transaction &txn)
{
  auto &keys = _ctxt.keysInRange();

  if ( _vcsToRead.size() != keys.size() )
  {
    // create refcount-barrier from Deleter (called on last-ptr-copy dtor)
    auto barrierLock = std::shared_ptr<BlockReadXform>(this, [&txn](BlockReadXform *ptr) {
             ptr->handleReadCacheLookupComplete(txn);  // recurse from last one
          });
    for( auto i = 0U ; i < keys.size() ; ++i ) {
      _vcsToRead.emplace_back();
      // prep for async reads that init into VConn-futures
      auto contp = APICont::create_temp_tscont(_vcsToRead.back(),barrierLock);
      TSCacheRead(contp,keys[i]);
    }
    return;
  }

  auto nrdy = 0U;
  auto nvalid = 0U;

  // scan *all* keys and vconns to check if ready
  for( ; nvalid < _vcsToRead.size() ; ++nvalid ) 
  {
    if ( _vcsToRead[nvalid].get_future().wait_for(std::chrono::seconds(0)) != std::future_status::ready ) {
      break;
    }

    ++nrdy; // value is ready

    auto vconn = _vcsToRead[nvalid].get_future().get();
    // block isn't ready
    if ( ! vconn ) {
      break;
    }

    // block is ready and of right size?
    if ( TSVConnCacheObjectSizeGet(vconn) != static_cast<int64_t>(_ctxt.blockSize()) ) {
      // TODO: delete block
      break;
    }
  }

  // ready to read from cache...
  if ( nvalid < keys.size() ) {
    // erase current stub and start from scratch
    TSHttpTxnCacheLookupStatusSet(_ctxt.atsTxn(), TS_CACHE_LOOKUP_MISS);
    // TODO: reset bits and go to write-based restart
    _ctxt.handleReadCacheLookupComplete(txn); // resume there
    return;
  }

  TSHttpTxnCacheLookupStatusSet(_ctxt.atsTxn(), TS_CACHE_LOOKUP_HIT_FRESH);
  txn.resume(); // blocks are looked up .. so we can continue...
}
// }

// tsapi TSReturnCode TSBase64Decode(const char *str, size_t str_len, unsigned char *dst, size_t dst_size, size_t *length);
// tsapi TSReturnCode TSBase64Encode(const char *str, size_t str_len, char *dst, size_t dst_size, size_t *length);
void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
//  pluginPtr =  std::make_shared<RangeDetect>();
  plugin = new RangeDetect();
}
