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

/**
 * @file Transaction.cc
 */

#include "atscppapi/Transaction.h"
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <memory>

#include "ts/ink_memory.h"
#include "atscppapi/shared_ptr.h"
#include "logging_internal.h"
#include "utils_internal.h"
#include "atscppapi/noncopyable.h"
using std::map;
using std::string;
using namespace atscppapi;

/**
 * @private
 */
struct atscppapi::TransactionState : noncopyable {
  TSHttpTxn const txn_;
  std::list<TransactionPlugin *> plugins_;
  ClientRequest client_request_;
  Request server_request_;
  Response server_response_;
  Response client_response_;
  Response cached_response_;
  Request cached_request_;
  map<string, shared_ptr<Transaction::ContextValue>> context_values_;

  TransactionState(TSHttpTxn txn, TSMBuffer client_request_hdr_buf, TSMLoc client_request_hdr_loc)
    : txn_(txn), client_request_(txn, client_request_hdr_buf, client_request_hdr_loc)
  {
  }
};

Transaction::Transaction(void *raw_txn)
{
  TSHttpTxn txn = static_cast<TSHttpTxn>(raw_txn);
  TSMBuffer hdr_buf;
  TSMLoc hdr_loc;
  (void)TSHttpTxnClientReqGet(txn, &hdr_buf, &hdr_loc);
  if (!hdr_buf || !hdr_loc) {
    LOG_ERROR("TSHttpTxnClientReqGet tshttptxn=%p returned a null hdr_buf=%p or hdr_loc=%p.", txn, hdr_buf, hdr_loc);
  }

  state_ = new TransactionState(txn, hdr_buf, hdr_loc);
  LOG_DEBUG("Transaction tshttptxn=%p constructing Transaction object %p, client req hdr_buf=%p, client req hdr_loc=%p", txn, this,
            hdr_buf, hdr_loc);
}

Transaction::~Transaction()
{
  LOG_DEBUG("Transaction tshttptxn=%p destroying Transaction object %p", state_->txn_, this);
  delete state_;
}

bool
Transaction::configIntSet(TSOverridableConfigKey conf, int value)
{
  return TS_SUCCESS == TSHttpTxnConfigIntSet(state_->txn_, conf, static_cast<TSMgmtInt>(value));
}
bool
Transaction::configIntGet(TSOverridableConfigKey conf, int *value)
{
  return TS_SUCCESS == TSHttpTxnConfigIntGet(state_->txn_, conf, reinterpret_cast<TSMgmtInt *>(value));
}

bool
Transaction::configFloatSet(TSOverridableConfigKey conf, float value)
{
  return TS_SUCCESS == TSHttpTxnConfigFloatSet(state_->txn_, conf, static_cast<TSMgmtFloat>(value));
}

bool
Transaction::configFloatGet(TSOverridableConfigKey conf, float *value)
{
  return TS_SUCCESS == TSHttpTxnConfigFloatGet(state_->txn_, conf, value);
}

bool
Transaction::configStringSet(TSOverridableConfigKey conf, std::string const &value)
{
  return TS_SUCCESS == TSHttpTxnConfigStringSet(state_->txn_, conf, const_cast<TSMgmtString>(value.data()), value.length());
}

bool
Transaction::configStringGet(TSOverridableConfigKey conf, std::string &value)
{
  const char *svalue;
  int length;
  bool zret = TS_SUCCESS == TSHttpTxnConfigStringGet(state_->txn_, conf, &svalue, &length);
  if (zret) {
    value.assign(svalue, length);
  } else {
    value.clear();
  }
  return zret;
}

bool
Transaction::configFind(std::string const &name, TSOverridableConfigKey *conf, TSRecordDataType *type)
{
  return TS_SUCCESS == TSHttpTxnConfigFind(name.data(), name.length(), conf, type);
}

void
Transaction::resume()
{
  TSHttpTxnReenable(state_->txn_, static_cast<TSEvent>(TS_EVENT_HTTP_CONTINUE));
}

void
Transaction::error()
{
  LOG_DEBUG("Transaction tshttptxn=%p reenabling to error state", state_->txn_);
  TSHttpTxnReenable(state_->txn_, static_cast<TSEvent>(TS_EVENT_HTTP_ERROR));
}

void
Transaction::error(const std::string &page)
{
  setErrorBody(page);
  error(); // finally, reenable with HTTP_ERROR
}

void
Transaction::setErrorBody(const std::string &page)
{
  LOG_DEBUG("Transaction tshttptxn=%p setting error body page length: %lu", state_->txn_, page.length());
  char *body = (char *)TSmalloc(page.length());
  memcpy(body, page.data(), page.length());
  TSHttpTxnErrorBodySet(state_->txn_, body, page.length(), NULL); // Default to text/html
}

void
Transaction::setErrorBody(const std::string &page, const std::string &mimetype)
{
  LOG_DEBUG("Transaction tshttptxn=%p setting error body page length: %lu", state_->txn_, page.length());
  char *body = (char *)TSmalloc(page.length());
  memcpy(body, page.data(), page.length());
  TSHttpTxnErrorBodySet(state_->txn_, body, page.length(), TSstrdup(mimetype.c_str()));
}

void
Transaction::setStatusCode(HttpStatus code)
{
  LOG_DEBUG("Transaction tshttptxn=%p setting status code: %d", state_->txn_, code);
  TSHttpTxnSetHttpRetStatus(state_->txn_, static_cast<TSHttpStatus>(code));
}

bool
Transaction::isInternalRequest() const
{
  return TSHttpTxnIsInternal(state_->txn_) == TS_SUCCESS;
}

void *
Transaction::getAtsHandle() const
{
  return static_cast<void *>(state_->txn_);
}

const std::list<atscppapi::TransactionPlugin *> &
Transaction::getPlugins() const
{
  return state_->plugins_;
}

void
Transaction::addPlugin(TransactionPlugin *plugin)
{
  LOG_DEBUG("Transaction tshttptxn=%p registering new TransactionPlugin %p.", state_->txn_, plugin);
  state_->plugins_.push_back(plugin);
}

shared_ptr<Transaction::ContextValue>
Transaction::getContextValue(const std::string &key)
{
  shared_ptr<Transaction::ContextValue> return_context_value;
  map<string, shared_ptr<Transaction::ContextValue>>::iterator iter = state_->context_values_.find(key);
  if (iter != state_->context_values_.end()) {
    return_context_value = iter->second;
  }

  return return_context_value;
}

void
Transaction::setContextValue(const std::string &key, shared_ptr<Transaction::ContextValue> value)
{
  state_->context_values_[key] = value;
}

ClientRequest &
Transaction::getClientRequest()
{
  return state_->client_request_;
}

string
Transaction::getEffectiveUrl()
{
  string ret_val;
  int length = 0;
  char *buf  = TSHttpTxnEffectiveUrlStringGet(state_->txn_, &length);
  if (buf && length) {
    ret_val.assign(buf, length);
  }

  if (buf) {
    TSfree(buf);
  }

  return ret_val;
}

bool
Transaction::setCacheUrl(const string &cache_url)
{
  TSReturnCode res = TSCacheUrlSet(state_->txn_, cache_url.c_str(), cache_url.length());
  return (res == TS_SUCCESS);
}

void
Transaction::setSkipRemapping(int flag)
{
  TSSkipRemappingSet(state_->txn_, flag);
}

const sockaddr *
Transaction::getIncomingAddress() const
{
  return TSHttpTxnIncomingAddrGet(state_->txn_);
}

const sockaddr *
Transaction::getClientAddress() const
{
  return TSHttpTxnClientAddrGet(state_->txn_);
}

const sockaddr *
Transaction::getNextHopAddress() const
{
  return TSHttpTxnNextHopAddrGet(state_->txn_);
}

const sockaddr *
Transaction::getServerAddress() const
{
  return TSHttpTxnServerAddrGet(state_->txn_);
}

bool
Transaction::setServerAddress(const sockaddr *sockaddress)
{
  return TSHttpTxnServerAddrSet(state_->txn_, sockaddress) == TS_SUCCESS;
}

bool
Transaction::setIncomingPort(uint16_t port)
{
  TSHttpTxnClientIncomingPortSet(state_->txn_, port);
  return true; // In reality TSHttpTxnClientIncomingPortSet should return SUCCESS or ERROR.
}

/*
 * Note: The following methods cannot be attached to a Response
 * object because that would require the Response object to
 * know that it's a server or client response because of the
 * TS C api which is TSHttpTxnServerRespBodyBytesGet.
 */
size_t
Transaction::getServerResponseBodySize()
{
  return static_cast<size_t>(TSHttpTxnServerRespBodyBytesGet(state_->txn_));
}

size_t
Transaction::getServerResponseHeaderSize()
{
  return static_cast<size_t>(TSHttpTxnServerRespHdrBytesGet(state_->txn_));
}

size_t
Transaction::getClientResponseBodySize()
{
  return static_cast<size_t>(TSHttpTxnClientRespBodyBytesGet(state_->txn_));
}

size_t
Transaction::getClientResponseHeaderSize()
{
  return static_cast<size_t>(TSHttpTxnClientRespHdrBytesGet(state_->txn_));
}

void
Transaction::setTimeout(Transaction::TimeoutType type, int time_ms)
{
  switch (type) {
  case TIMEOUT_DNS:
    TSHttpTxnDNSTimeoutSet(state_->txn_, time_ms);
    break;
  case TIMEOUT_CONNECT:
    TSHttpTxnConnectTimeoutSet(state_->txn_, time_ms);
    break;
  case TIMEOUT_NO_ACTIVITY:
    TSHttpTxnNoActivityTimeoutSet(state_->txn_, time_ms);
    break;
  case TIMEOUT_ACTIVE:
    TSHttpTxnActiveTimeoutSet(state_->txn_, time_ms);
    break;
  default:
    break;
  }
}

Transaction::CacheStatus
Transaction::getCacheStatus()
{
  int obj_status = TS_ERROR;

  if (TSHttpTxnCacheLookupStatusGet(state_->txn_, &obj_status) == TS_ERROR) {
    return CACHE_LOOKUP_NONE;
  }

  switch (obj_status) {
  case TS_CACHE_LOOKUP_MISS:
    return CACHE_LOOKUP_MISS;
  case TS_CACHE_LOOKUP_HIT_STALE:
    return CACHE_LOOKUP_HIT_STALE;
  case TS_CACHE_LOOKUP_HIT_FRESH:
    return CACHE_LOOKUP_HIT_FRESH;
  case TS_CACHE_LOOKUP_SKIPPED:
    return CACHE_LOOKUP_SKIPED;
  default:
    return CACHE_LOOKUP_NONE;
  }
}

void
Transaction::redirectTo(std::string const &url)
{
  char *s = ats_strdup(url.c_str());
  // Must re-alloc the string locally because ownership is transferred to the transaction.
  TSHttpTxnRedirectUrlSet(state_->txn_, s, url.length());
}

void
Transaction::refreshHeaders()
{
  state_->cached_response_.getHeaders().reset(nullptr, nullptr);
  state_->cached_request_.getHeaders().reset(nullptr, nullptr);

  state_->client_response_.getHeaders().reset(nullptr, nullptr);

  state_->server_request_.getHeaders().reset(nullptr, nullptr);
  state_->server_response_.getHeaders().reset(nullptr, nullptr);
}


template <typename T_TXN>
Request &
Transaction::init_from_getter(T_TXN txn, Request &obj, TSReturnCode (*getterFxn)(T_TXN, TSMBuffer *, TSMLoc *))
{
  if (! obj.getHeaders().isInitialized() || obj.getMethod() == HTTP_METHOD_UNKNOWN) {
    TSMBuffer buf;
    TSMLoc loc;

    if (getterFxn(txn, &buf, &loc) == TS_SUCCESS) {
      obj.init(buf, loc);
    }
  }

  return obj;
}

template <typename T_TXN>
Response &
Transaction::init_from_getter(T_TXN txn, Response &obj, TSReturnCode (*getterFxn)(T_TXN, TSMBuffer *, TSMLoc *))
{
  if (! obj.getHeaders().isInitialized() || obj.getStatusCode() == HTTP_STATUS_UNKNOWN) {
    TSMBuffer buf;
    TSMLoc loc;

    if (getterFxn(txn, &buf, &loc) == TS_SUCCESS) {
      obj.init(buf, loc);
    }
  }

  return obj;
}

template Request &Transaction::init_from_getter(TSHttpTxn txn, Request &obj, TSReturnCode (*getterFxn)(TSHttpTxn, TSMBuffer *, TSMLoc *));
template Request &Transaction::init_from_getter(TSHttpAltInfo txn, Request &obj, TSReturnCode (*getterFxn)(TSHttpAltInfo, TSMBuffer *, TSMLoc *));
template Response &Transaction::init_from_getter(TSHttpTxn txn, Response &obj, TSReturnCode (*getterFxn)(TSHttpTxn, TSMBuffer *, TSMLoc *));
template Response &Transaction::init_from_getter(TSHttpAltInfo txn, Response &obj, TSReturnCode (*getterFxn)(TSHttpAltInfo, TSMBuffer *, TSMLoc *));

Request &
Transaction::getServerRequest()
{
  return init_from_getter(state_->txn_, state_->server_request_, TSHttpTxnServerReqGet);
}

Response &
Transaction::getServerResponse()
{
  return init_from_getter(state_->txn_, state_->server_response_, TSHttpTxnServerRespGet);
}

Response &
Transaction::getClientResponse()
{
  return init_from_getter(state_->txn_, state_->client_response_, TSHttpTxnClientRespGet);
}

Request &
Transaction::getCachedRequest()
{
  return init_from_getter(state_->txn_, state_->cached_request_, TSHttpTxnCachedReqGet);
}

Response &
Transaction::getCachedResponse()
{
  return init_from_getter(state_->txn_, state_->cached_response_, TSHttpTxnCachedRespGet);
}

Response &
Transaction::updateCachedResponse()
{
  state_->cached_response_.getHeaders().reset(nullptr, nullptr); // re-get it
  return init_from_getter(state_->txn_, state_->cached_response_, TSHttpTxnCachedRespModifiableGet);
}
