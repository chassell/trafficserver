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

#include <atscppapi/GlobalPlugin.h>
#include <atscppapi/TransactionPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>

using namespace atscppapi;

namespace
{
std::shared_ptr<GlobalPlugin> plugin;
}

class TransactionHookPlugin : public atscppapi::TransactionPlugin
{
public:
  TransactionHookPlugin(Transaction &transaction) : TransactionPlugin(transaction)
  {
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);  // maybe unneeded
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // maybe unneeded
    TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);
  }
  ~TransactionHookPlugin() override {}

  //
  // [called from global hook / remap hook]
  //
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    auto &clntHdrs = txn.getClientRequest().getHeaders();
    auto url  = txn.getEffectiveUrl();

    _absRange = clntHdrs.values("Range");

    // save fields for later (passthru and passback)
    clntHdrs.set("Range", _blkRange);
    txn.setCacheUrl(url);
    txn.resume();
  }

  void
  handleReadCacheHeaders(Transaction &txn) override
  {
    // [maybe spawn next block?]
    txn.resume();
  }

  void
  handleSendRequestHeaders(Transaction &txn) override
  {
    txn.getServerRequest().getHeaders().set("Range", _absRange);
    txn.resume();
  }

  void
  handleReadResponseHeaders(Transaction &txn) override
  {
    auto &srvResp = txn.getServerResponse();

    auto status = srvResp.getStatusCode();
    if ( status == HTTP_STATUS_PARTIAL_CONTENT ) {
      srvResp.setStatusCode(HTTP_STATUS_OK);
    }

    // don't cache in some cases?? erase headers?
    txn.resume();
  }

  void
  handleSendResponseHeaders(Transaction &txn) override
  {
    txn.getClientResponse().getHeaders().set("Range", _absRange);
    txn.getClientResponse().setStatusCode(HTTP_STATUS_PARTIAL_CONTENT);
    txn.resume();
  }

private:
  std::string _absRange;
  std::string _blkRange;
};

class GlobalHookPlugin : public atscppapi::GlobalPlugin
{
public:
  GlobalHookPlugin() { GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_PRE_REMAP); }

  void
  // determine if ranges of appropriate size are present
  handleSelectAlt(Transaction &txn) override
  {
  }

  // transaction
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    //
    // before lookup: do filtering here
    //
    if (false) {
      txn.resume();
      return;
    }

    auto &txnPlugin = *new TransactionHookPlugin(txn);
    txn.addPlugin(&txnPlugin);
    // forward event on
    txnPlugin.handleReadRequestHeadersPostRemap(txn);
  }
};

void
TSPluginInit(int, const char **)
{
  RegisterGlobalPlugin("CPP_Example_TransactionHook", "apache", "dev@trafficserver.apache.org");
  plugin = std::make_shared<GlobalHookPlugin>();
}
