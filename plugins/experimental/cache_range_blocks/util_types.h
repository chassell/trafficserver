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
#include "ts/ink_memory.h"  // for make_unique
#include "ts/ink_align.h"
#include "ts/ts.h"

#include <atscppapi/Url.h>

#include <memory>
#include <string>
#include <vector>
#include <future>

extern const int8_t base64_values[];
extern const char *const base64_chars;

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

class XformReader;

// namespace {

using TSCacheKey_t = std::unique_ptr<std::remove_pointer<TSCacheKey>::type>;
using TSCont_t = std::unique_ptr<std::remove_pointer<TSCont>::type>;
using TSMutex_t = std::unique_ptr<std::remove_pointer<TSMutex>::type>;
using TSMBuffer_t = std::unique_ptr<std::remove_pointer<TSMBuffer>::type>;
using TSIOBuffer_t = std::unique_ptr<std::remove_pointer<TSIOBuffer>::type>;
using TSIOBufferReader_t = std::unique_ptr<std::remove_pointer<TSIOBufferReader>::type>;

using TSMutexPtr_t = std::shared_ptr<std::remove_pointer<TSMutex>::type>;


namespace std {
// unique_ptr deletions
template <> inline void default_delete<TSCacheKey_t::element_type>::operator()(TSCacheKey key) const 
  { TSCacheKeyDestroy(key); }
template <> inline void default_delete<TSCont_t::element_type>::operator()(TSCont cont) const 
  { TSContDestroy(cont); }
template <> inline void default_delete<TSMutex_t::element_type>::operator()(TSMutex mutex) const 
  { TSMutexDestroy(mutex); }
template <> inline void default_delete<TSMBuffer_t::element_type>::operator()(TSMBuffer buff) const 
  { TSMBufferDestroy(buff); }
template <> inline void default_delete<TSIOBuffer_t::element_type>::operator()(TSIOBuffer buff) const 
  { TSIOBufferDestroy(buff); }
template <> inline void default_delete<TSIOBufferReader_t::element_type>::operator()(TSIOBufferReader reader) const 
  { TSIOBufferReaderFree(reader); }
}


// unique-ref object for a cache-read or cache-write request
struct APICacheKey : public TSCacheKey_t
{
  APICacheKey() = default; // nullptr by default

  operator TSCacheKey() const { return get(); }

  APICacheKey(const atscppapi::Url &url, uint64_t offset);
};

// object to request write/read into cache
struct APICont : public TSCont_t
{
  template <typename T, typename... Args>
  friend std::unique_ptr<T> std::make_unique(Args &&... args);

 public:
  template <typename T_DATA, typename T_REFCOUNTED>
  static TSCont create_temp_tscont(std::shared_future<T_DATA> &cbFuture, const T_REFCOUNTED &counted);

 public:
  APICont() = default; // nullptr by default

  // accepts TSHttpTxn handler functions
  template <class T_OBJ, typename T_DATA>
  APICont(T_OBJ &obj, void(T_OBJ::*funcp)(TSEvent,TSHttpTxn,T_DATA), T_DATA cbdata);

  operator TSCont() { return get(); }

  APICont &operator=(std::function<void(TSEvent,void*)> &&fxn) {
    _userCB = fxn;
    return *this;
  }

private:
  static int handleTSEvent(TSCont cont, TSEvent event, void *data);

  APICont(TSMutex mutex);

  // holds object and function pointer
  std::function<void(TSEvent,void*)> _userCB;
};

// object to request write/read into cache
struct APIXformCont : public TSCont_t
{
 public:
  APIXformCont() = default; // nullptr by default
  APIXformCont(TSHttpTxn txnHndl, TSHttpHookID xformType);

  operator TSVConn() { return get(); }

  APIXformCont &operator=(std::function<void(TSEvent,TSVConn)> &&fxn) {
    _userXformCB = fxn;
    return *this;
  }

private:
  static int handleXformTSEvent(TSCont cont, TSEvent event, void *data);

  // holds object and function pointer
  std::function<void(TSEvent,TSVConn)> _userXformCB;
  TSIOBuffer_t                         _commonOutput;
};


//}
