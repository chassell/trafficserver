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

BlockStoreXform::BlockStoreXform(Transaction &txn, BlockSetAccess &ctxt)
   : TransactionPlugin(txn), _ctxt(ctxt), 
     _xform(ctxt.atsTxn(), [this](TSIOBufferReader r,int64_t pos,int64_t len) { return this->handleWrite(r,pos,len); }, 
             ctxt.rangeLen()),
     _vcsToWrite(ctxt.keysInRange().size())
{
  TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS); // add block-range and clean up
  TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // adjust headers to stub-file
}

void
BlockStoreXform::handleReadCacheLookupComplete(Transaction &txn)
{
  DEBUG_LOG("xform-store: xhook:%p _ctxt:%p len=%lu",this,&_ctxt,_ctxt.assetLen());

  // [will override the server response for headers]
  auto &keys = _ctxt.keysInRange();

  auto mutex = TSMutexCreate();

  for( auto i = 0U ; i < keys.size() ; ++i ) {
    if ( keys[i] ) {
      // prep for async write that inits into VConn-futures 
      auto contp = APICont::create_temp_tscont(mutex,_vcsToWrite[i]);
      TSCacheWrite(contp,keys[i]); // find room to store each key...
    }
  }

  DEBUG_LOG("xform-store: len=%lu",_ctxt._assetLen);
  txn.resume(); // wait for response
}

void
BlockStoreXform::handleSendRequestHeaders(Transaction &txn) {
  DEBUG_LOG("srvr-req: len=%lu",_ctxt._assetLen);
  _ctxt.clean_server_request(txn); // request full blocks if possible
  txn.resume();
}

void
BlockStoreXform::handleReadResponseHeaders(Transaction &txn)
{
  DEBUG_LOG("srvr-resp: len=%lu",_ctxt._assetLen);
   _ctxt.clean_server_response(txn);
   txn.resume();
}

int64_t BlockStoreXform::handleWrite(TSIOBufferReader r, int64_t pos, int64_t len)
{
  // XXX
  return 0L;
}

BlockStoreXform::~BlockStoreXform() 
{
  for( auto &&i : _vcsToWrite ) {
    TSVConnClose(i.get());
  }
}
