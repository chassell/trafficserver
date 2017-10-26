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

#include <atscppapi/GlobalPlugin.h>
#include <atscppapi/TransformationPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define CONTENT_ENCODING_TAG     "Content-Encoding"
#define CONTENT_LENGTH_TAG       "Content-Length"
#define RANGE_TAG                "Range"
#define ACCEPT_ENCODING_TAG      "Accept-Encoding"
#define X_BLOCK_PRESENCE_TAG     "X-Block-Presence"

using namespace atscppapi;

namespace
{
std::shared_ptr<GlobalPlugin> plugin;

class BlockStoreXform;
class BlockSendXform;

class FindTxnBlockPlugin : public TransactionPlugin
{
public:
  FindTxnBlockPlugin(Transaction &txn, Headers &clntReq)
     : TransactionPlugin(txn), _clntReq(clntReq) 
  {
  }

  ~FindTxnBlockPlugin() override {}

//////////////////////////////////////////
//////////// in the Transaction phases
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    // add permission to use block-directory headers (on MISS)
    //    but if internal HIT then use Range as given
    _clntReq.append(ACCEPT_ENCODING,CONTENT_ENCODING_INTERNAL ";q=0.001");

    // detect use of stub file then
    //    A) cause a HIT->MISS change and transform response to correct range
    //    B) leave a HIT and transform response into stored block info
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
    txn.resume();
  }

  void
  handleReadCacheLookupComplete(Transaction &txn)
  {
    TSCacheKey blkKey = nullptr; // valid if stub-file block-hit occurred

    // stub has found a cached key?
    if (getCacheStatus() == CACHE_LOOKUP_HIT_FRESH && 
            (_stubHdrs=get_stub_hdrs()) && 
            (_blockInd=get_avail_blocks()) )
    {
      _sendXform.reset(new BlockSendXform(*this,txn));
      return; // insert cached block of data upon SEND_RESPONSE
    }

    if ( getCacheStatus() == CACHE_LOOKUP_HIT_FRESH && !_stubHdrs ) {
      return; // normal file is usable!
    }

    _storeXform.reset(new BlockStoreXform(*this,txn));

    // this needs to be a MISS...
    setCacheStatus(CACHE_LOOKUP_MISS); 

    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS);
    txn.resume();
  }

private:
  Headers *get_stub_hdrs() 
  {
     auto &ccheHdrs = getCacheHeaders();
     auto i = ccheHdrs.values(CONTENT_ENCODING_TAG).find(CONTENT_ENCODING_INTERNAL);
     return ( i != std::string::npos ? &ccheHdrs : nullptr );
  }

  uint64_t get_avail_blocks();

  Headers    &_clntReq;           // must be available
  Headers    *_stubHdrs = nullptr; // if found
  unsigned    _blockInd = 0U; // if found and in-cache
  std::string _clntRange;
  std::unique_ptr<BlockCopyXform> _copyXform;
  std::unique_ptr<BlockSendXform> _sendXform;
};

class BlockCopyXform : public TransformationPlugin
{
  BlockCopyXform(FindTxnBlockPlugin &ctxt, Transaction &txn)
     : TransformationPlugin(txn, RESPONSE_TRANSFORMATION),
  {
    // substitute a HIT->MISS change if block is missing
    TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);
  }
  ~BlockCopyXform() override {}

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    txn.getClientResponse().getHeaders().set(RANGE_TAG, _absRange);
    txn.getClientResponse().setStatusCode(HTTP_STATUS_PARTIAL_CONTENT);
    txn.resume();
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
}

class BlockSendXform : public TransformationPlugin
{
  BlockCopyXform(FindTxnBlockPlugin &ctxt, Transaction &txn)
     : TransformationPlugin(txn, RESPONSE_TRANSFORMATION),
  {
    // substitute a HIT->MISS change if block is missing
    TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);
  }
  ~BlockCopyXform() override {}

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    txn.getClientResponse().getHeaders().set(RANGE_TAG, _absRange);
    txn.getClientResponse().setStatusCode(HTTP_STATUS_PARTIAL_CONTENT);
    txn.resume();
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
}


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
      return; // doesn't apply to weird ones
    }

    // allow MISS --> block-dir-stub lookup as possible
    auto &txnPlugin = *new FindTxnBlockPlugin(txn, clntReq);

    // perform state setup ...
    txnPlugin.handleReadRequestHeadersPostRemap(txn);
  }

private:
  std::string _random
};

}


uint64_t FindTxnBlockPlugin::get_avail_blocks()
{
   auto rangeFld = _clntReq.value(RANGE_TAG);

   // value of digit-string after last '=' 
   //    or 0 if no '=' 
   //    or negative value if no digits and '-' present
   auto start = std::atoll( rangeFld.erase(0,rangeFld.rfind('=')).erase(0,1).c_str() );
   // negative value of digit-string after last '-'
   //    or 0 if no '=' or leading 0
   auto end = std::atoll( rangeFld.erase(0,rangeFld.rfind('-').c_str()));

   auto &stubHdrs = *_stubHdrs;
   auto bitString = stubHdrs.value(X_BLOCK_PRESENCE_TAG);
   auto len = std::stoll(stubHdrs.value(CONTENT_LENGTH_TAG));
   auto blksize = INK_ALIGN(len/bitString.size(),4096); // re-use block size chosen

   // to inclusive start/end of valid blocks
   start = start/blksize
   end = (end+blksize-1)/blksize;

   for( auto i = start ; i < end && is_base64_bit_set(bitString,i) ; ++i ) {
   }

   return i == 

}


// tsapi TSReturnCode TSBase64Decode(const char *str, size_t str_len, unsigned char *dst, size_t dst_size, size_t *length);
// tsapi TSReturnCode TSBase64Encode(const char *str, size_t str_len, char *dst, size_t dst_size, size_t *length);

  X_BLOCK_PRESENCE_TAG 
}

void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
  plugin = std::make_shared<GlobalHookPlugin>();
}
