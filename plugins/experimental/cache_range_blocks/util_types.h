/** Licensed to the Apache Software Foundation (ASF) under one
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
#include "ts/ink_memory.h" // for make_unique
#include "ts/ink_align.h"
#include "ts/ts.h"

#include <atscppapi/Url.h>
#include <atscppapi/Transaction.h>

#include <memory>
#include <string>
#include <vector>
#include <future>

extern const int8_t base64_values[];
extern const char *const base64_chars;

static inline void
base64_bit_clr(std::string &base64, unsigned i)
{
  auto &c = base64[i / 6];
  c       = base64_chars[~(1 << (i % 6)) & base64_values[c - '+']];
}

static inline void
base64_bit_set(std::string &base64, unsigned i)
{
  auto &c = base64[i / 6];
  c       = base64_chars[(1 << (i % 6)) | base64_values[c - '+']];
}

static inline bool
is_base64_bit_set(const std::string &base64, unsigned i)
{
  return (1 << (i % 6)) & base64_values[base64[i / 6] - '+'];
}

void forward_vio_event(TSEvent event, TSVIO invio);

class XformReader;

namespace std
{
template <typename _Tp, typename... _Args>
inline unique_ptr<_Tp>
make_unique(_Args &&... __args)
{
  return unique_ptr<_Tp>(new _Tp(std::forward<_Args>(__args)...));
}
}

// namespace {

using TSCacheKey_t       = std::unique_ptr<std::remove_pointer<TSCacheKey>::type>;
using TSCont_t           = std::unique_ptr<std::remove_pointer<TSCont>::type>;
using TSMutex_t          = std::unique_ptr<std::remove_pointer<TSMutex>::type>;
using TSMBuffer_t        = std::unique_ptr<std::remove_pointer<TSMBuffer>::type>;
using TSIOBuffer_t       = std::unique_ptr<std::remove_pointer<TSIOBuffer>::type>;
using TSIOBufferReader_t = std::unique_ptr<std::remove_pointer<TSIOBufferReader>::type>;

using TSMutexPtr_t = std::shared_ptr<std::remove_pointer<TSMutex>::type>;

namespace std
{
// unique_ptr deletions
template <>
inline void
default_delete<TSCacheKey_t::element_type>::operator()(TSCacheKey key) const
{
  TSCacheKeyDestroy(key);
}
// NOTE: may require TSVConnClose()!
template <>
inline void
default_delete<TSCont_t::element_type>::operator()(TSCont cont) const
{
  TSContDestroy(cont);
}
template <>
inline void
default_delete<TSMutex_t::element_type>::operator()(TSMutex mutex) const
{
  TSMutexDestroy(mutex);
}
template <>
inline void
default_delete<TSMBuffer_t::element_type>::operator()(TSMBuffer buff) const
{
  TSMBufferDestroy(buff);
}
template <>
inline void
default_delete<TSIOBuffer_t::element_type>::operator()(TSIOBuffer buff) const
{
  TSIOBufferDestroy(buff);
}
template <>
inline void
default_delete<TSIOBufferReader_t::element_type>::operator()(TSIOBufferReader reader) const
{
  TSIOBufferReaderFree(reader);
}
}

// unique-ref object for a cache-read or cache-write request
struct APICacheKey : public TSCacheKey_t {
  APICacheKey() = default; // nullptr by default

  operator TSCacheKey() const { return get(); }
  APICacheKey(const atscppapi::Url &url, std::string const &etag, uint64_t offset);
};

// object to request write/read into cache
struct APICont : public TSCont_t {
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  template <typename T_DATA>
  static TSCont create_temp_tscont(TSMutex shared_mutex, std::shared_future<T_DATA> &cbFuture,
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());

public:
  APICont() = default; // nullptr by default

  // accepts TSHttpTxn handler functions
  template <class T_OBJ, typename T_DATA> APICont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, T_DATA), T_DATA cbdata);

  operator TSCont() { return get(); }
  APICont &
  operator=(std::function<void(TSEvent, void *)> &&fxn)
  {
    _userCB = fxn;
    return *this;
  }

private:
  static int handleTSEventCB(TSCont cont, TSEvent event, void *data);

  APICont(TSMutex mutex);

  // holds object and function pointer
  std::function<void(TSEvent, void *)> _userCB;
};

// object to request write/read into cache
struct APIXformCont : public TSCont_t {
  using XformCB_t = std::function<int64_t(TSEvent, TSVIO, int64_t)>;
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  //  APIXformCont() = default; // nullptr by default
  //  APIXformCont(APIXformCont &&) = default;
  APIXformCont(atscppapi::Transaction &txn, TSHttpHookID xformType, int64_t bytes = INT64_MAX, int64_t offset = 0);

  operator TSVConn() { return get(); }
  TSVConn
  input() const
  {
    return get();
  }
  TSVIO
  inputVIO() const
  {
    return _inVIO;
  }

  TSVConn
  output() const
  {
    return _outVConn;
  }
  TSVIO
  outputVIO() const
  {
    return _outVIO;
  }
  TSIOBuffer
  outputBuffer() const
  {
    return _outBufferP.get();
  }
  TSIOBufferReader
  outputReader() const
  {
    return _outReaderP.get();
  }
  int64_t
  outHeaderLen() const
  {
    return _outHeaderLen;
  }

  void
  set_body_handler(XformCB_t &&fxn)
  {
    TSDebug("cache_range_block", "[%s:%d] %s()", __FILE__, __LINE__, __func__);
    _bodyXformCB = fxn;
  }

  void
  set_copy_handler(int64_t pos, XformCB_t &&fxn)
  {
    TSDebug("cache_range_block", "[%s:%d] %s(): limit=%#lx", __FILE__, __LINE__, __func__, pos);
    _nextXformCB         = fxn;
    _nextXformCBAbsLimit = pos; // relative position to prev. one
  }

  int64_t copy_next_len(int64_t);
  int64_t skip_next_len(int64_t);

private:
  int check_completions(TSEvent event);

  int handleXformTSEvent(TSCont cont, TSEvent event, void *data);

  static int handleXformTSEventCB(TSCont cont, TSEvent event, void *data);

  void init_body_range_handlers(int64_t len, int64_t offset);

  // called for
  int64_t
  call_body_handler(TSEvent evt, TSVIO vio, int64_t left)
  {
    return _bodyXformCB ? _bodyXformCB(evt, vio, left) : left;
  }

  atscppapi::Transaction &_txn;
  TSHttpTxn _atsTxn;
  XformCB_t _xformCB;
  int64_t _xformCBAbsLimit = 0L;

  XformCB_t _nextXformCB;
  int64_t _nextXformCBAbsLimit = 0L;

  XformCB_t _bodyXformCB;

  TSVIO _inVIO = nullptr;

  TSIOBuffer_t _outBufferP;
  TSIOBufferReader_t _outReaderP;
  int64_t _outHeaderLen = 0L;
  TSVConn _outVConn     = nullptr;
  TSVIO _outVIO         = nullptr;
};

class BlockTeeXform : public APIXformCont
{
  using HookType = std::function<int64_t(TSIOBufferReader, int64_t pos, int64_t len)>;

public:
  BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset);

  int64_t handleEvent(TSEvent event, TSVIO vio, int64_t left);

  HookType _writeHook;
  TSIOBuffer_t _teeBufferP;
  TSIOBufferReader_t _teeReaderP;
};

//}
