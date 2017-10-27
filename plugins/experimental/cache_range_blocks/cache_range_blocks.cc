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
   return (1<<(i%6)) & base64_values[base64[i/6]+0];
}


uint64_t FindTxnBlockPlugin::have_avail_blocks(Headers &stubHdrs)
{
   if ( ! _blkRange.empty() ) {
     return -1;
   }

   auto rangeFld = _clntRange; // to parse
   auto bitString = stubHdrs.value(X_BLOCK_PRESENCE_TAG);
   auto len = std::stoll(stubHdrs.value(CONTENT_LENGTH_TAG));

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

   auto blksize = INK_ALIGN(len/bitString.size(),4096*6); // re-use block size chosen

   // to inclusive start/end of valid blocks
   if ( start < 0 && ! end ) {
     return 0; // error!
   } 
   
   auto startBlk = static_cast<int>(start/blksize); // switch to suffix-of
   auto endBlk = static_cast<int>((end+blksize-1)/blksize); // switch to suffix-of
   auto lastBlk = static_cast<int>((len+blksize-1) / blksize);

   if ( start >= 0 && ! end ) {
     endBlk = lastBlk;
   } else if ( start < 0 && end ) {
     startBlk = endBlk; // switch to suffix-style range
     endBlk = lastBlk;
   }

   _blkRange = std::to_string(blksize*startBlk) + "-" + std::to_string(blksize*endBlk-1);

   for( auto i = startBlk ; i < endBlk ; ++i ) {
     if ( ! is_base64_bit_set(bitString,i) ) {
       return 0;
     }
   }

   return start;
}

void
FindTxnBlockPlugin::handleReadCacheLookupComplete(Transaction &txn)
{
  auto pstub = get_stub_hdrs(txn); // get (1) client-req ptr or (2) cached-stub ptr or nullptr

  if ( ! pstub ) {
    /// XXX delete old stub file in case of revalidate!
    return; // main file made it through
  }

  // need client-response for all cases now
  TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);

  if ( pstub != &_clntHdrs && have_avail_blocks(*pstub) ) {
    _sendXform = std::make_unique<BlockSendXform>(txn,*this);
    _sendXform->handleReadCacheLookupComplete(txn);
    return; // insert cached block of data upon SEND_RESPONSE
  }

  // intercept data for new or updated stub version
  _storeXform = std::make_unique<BlockStoreXform>(txn,*this);
  _storeXform->handleReadCacheLookupComplete(txn);
}

//}

// tsapi TSReturnCode TSBase64Decode(const char *str, size_t str_len, unsigned char *dst, size_t dst_size, size_t *length);
// tsapi TSReturnCode TSBase64Encode(const char *str, size_t str_len, char *dst, size_t dst_size, size_t *length);
void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
  plugin = std::make_shared<GlobalHookPlugin>();
}
