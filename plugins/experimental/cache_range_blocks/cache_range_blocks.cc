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
#include <atscppapi/TransformationPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>

using namespace atscppapi;

namespace
{
std::shared_ptr<GlobalPlugin> plugin;
}

class RangeBlockPlugin : public TransformationPlugin
{
public:
  RangeBlockPlugin(Transaction &transaction, std::string &&rangeFld) 
     : TransformationPlugin(transaction, RESPONSE_TRANSFORMATION),
       _absRange(rangeFld)
  {
    TransactionPlugin::registerHook(HOOK_CACHE_LOOKUP_COMPLETE);
    TransactionPlugin::registerHook(HOOK_SEND_REQUEST_HEADERS);  // maybe unneeded
    TransactionPlugin::registerHook(HOOK_READ_RESPONSE_HEADERS); // maybe unneeded
    TransactionPlugin::registerHook(HOOK_SEND_RESPONSE_HEADERS);
  }
  ~RangeBlockPlugin() override {}

//////////////////////////////////////////
//////////// in the Transaction phases
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

private:
  std::string _absRange;
  std::string _blkRange;
};

class GlobalHookPlugin : public GlobalPlugin
{
public:
  GlobalHookPlugin() { GlobalPlugin::registerHook(HOOK_READ_REQUEST_HEADERS_PRE_REMAP); }

  void
  // determine if ranges of appropriate size are present
  handleSelectAlt(const Request &clientReq, const Request &cachedReq, const Response &cachedResp) override
  {
  }

  // transaction
  void
  handleReadRequestHeadersPostRemap(Transaction &txn) override
  {
    auto &clntHdrs = txn.getClientRequest().getHeaders();
    auto rangeFld = clntHdrs.values("Range");

    if ( rangeFld.empty() ) {
      return; // doesn't apply
    }

    // turn to blocks on a cache-miss of the file
    auto &txnPlugin = *new RangeBlockPlugin(txn, std::move(rangeFld));

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
