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

#include <set>

//namespace
//{
std::shared_ptr<GlobalPlugin> plugin;

constexpr int8_t base64_values[] = {
  /*+*/ 62,
      ~0,~0,~0, /* ,-. */
  /*/ */ 63,
  /*0-9*/ 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
      ~0,~0,~0,~0,~0,~0,~0,~0, 
  /*A-Z*/ 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,23,24,25,
      ~0,~0,~0,~0,~0,~0,~0,
  /*a-z*/ 26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51
};


inline bool is_base64_bit_set(const std::string &base64,unsigned i)
{
   return (1<<(i%6)) & base64_values[base64[i/6]-'+'];
}


uint64_t BlockSetAccess::have_needed_blocks(Headers &stubHdrs)
{
   if ( ! _blkRange.empty() ) {
     return -1;
   }

   auto rangeFld = _respRange; // to parse
   auto bitString = stubHdrs.value(X_BLOCK_PRESENCE_TAG);
   auto contentLen = std::stoll(stubHdrs.value(CONTENT_LENGTH_TAG));

   // value of digit-string after last '=' 
   //    or 0 if no '=' 
   //    or negative value if no digits and '-' present
   auto start = std::atoll( rangeFld.erase(0,rangeFld.rfind('=')).erase(0,1).c_str() );
   
   if ( ! start && ( rangeFld.empty() || rangeFld.front() != '0' ) ) {
     start = -1;
   }

   // negative value of digit-string after last '-'
   //    or 0 if no '=' or leading 0
   auto end = - std::atoll( rangeFld.erase(0,rangeFld.rfind('-')).c_str());

   auto blksize = INK_ALIGN(contentLen/bitString.size(),4096*6); // re-use block size chosen

   // to inclusive start/end of valid blocks
   if ( start < 0 && ! end ) {
     return 0; // error!
   }

   auto startBlk = start/blksize; // inclusive-start block
   auto endBlk = (end+1+blksize-1)/blksize; // exclusive-end block
   auto lastBlk = (contentLen+blksize-1) / blksize;

   if ( start >= 0 && ! end ) {
     endBlk = lastBlk;
     end = contentLen;
   } else if ( start < 0 && -start == end ) {
     endBlk = lastBlk;
     end = contentLen;
     startBlk = endBlk; // switch to suffix-style range
   } else {
     ++end; // exclusive-end
   }

   _firstBlkSkip = start - startBlk*blksize; // may be zero
   _lastBlkTrunc = endBlk*blksize - end; // may be zero

   ink_assert( contentLen == static_cast<int64_t>(( endBlk - startBlk )*blksize) - _firstBlkSkip - _lastBlkTrunc );

   // store with inclusive end
   _blkRange = "bytes=";
   _blkRange += std::to_string(blksize*startBlk) + "-" + std::to_string(blksize*endBlk-1); 

   _respRange = std::to_string(start) + "-" + std::to_string(end-1) + "/" + std::to_string(contentLen);

   _keysInRange.resize( endBlk - startBlk );
   _vcsToRead.resize( endBlk - startBlk );
   _vcsToWrite.resize( endBlk - startBlk );

   auto misses = 0;

   for( auto i = startBlk ; i < endBlk ; ++i ) 
   {
     if ( ! is_base64_bit_set(bitString,i) ) {
       _keysInRange[i-startBlk] = CacheKey(_url,i*blksize);
       ++misses;
     }
   }

   if ( misses ) {
     return 0; // don't have all of them
   }

   // do have all of them!
   for( auto i = startBlk ; i < endBlk ; ++i ) {
     _keysInRange[i-startBlk] = CacheKey(_url,i*blksize);
   }

   return start;
}

void
BlockSetAccess::handleReadCacheLookupComplete(Transaction &txn)
{
  auto pstub = get_stub_hdrs(txn); // get (1) client-req ptr or (2) cached-stub ptr or nullptr

  if ( ! pstub ) {
    /// XXX delete old stub file in case of revalidate!
    return; // main file made it through
  }

  // clean up stub-response to be a normal header
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);

  // perform correct transformation
  if ( pstub == &_clntHdrs || ! have_needed_blocks(*pstub) ) {
    _storeXform = std::make_unique<BlockStoreXform>(txn,*this);
    _storeXform->handleReadCacheLookupComplete(txn);
  } else {
    // intercept data for new or updated stub version
    _sendXform = std::make_unique<BlockReadXform>(txn,*this);
    _sendXform->handleReadCacheLookupComplete(txn);
  }
}

void
BlockStoreXform::handleReadResponseHeaders(Transaction &txn)
{
  if ( ! _ctxt.blockRange().empty() ) {
    _ctxt.clientHdrs().set(RANGE_TAG, _ctxt.blockRange()); // request a block-based range if knowable
  }
  // create and store a *new*
}

//}

// tsapi TSReturnCode TSBase64Decode(const char *str, size_t str_len, unsigned char *dst, size_t dst_size, size_t *length);
// tsapi TSReturnCode TSBase64Encode(const char *str, size_t str_len, char *dst, size_t dst_size, size_t *length);
void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
  plugin = std::make_shared<RangeDetect>();
}
