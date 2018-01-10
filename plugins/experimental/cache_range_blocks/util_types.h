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

#pragma once

class ATSXformCont;

extern const int8_t base64_values[80];
extern const char base64_chars[65];

static inline void
base64_bit_clr(std::string &base64, unsigned i)
{
  char &c = base64[i / 6];
  auto v = ~(1 << (i % 6)) & base64_values[c - '+'];
  c       = base64_chars[v & 0x3f];
}

static inline void
base64_bit_set(std::string &base64, unsigned i)
{
  char &c = base64[i / 6];
  auto v = (1 << (i % 6)) | base64_values[c - '+'];
  c       = base64_chars[v];
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
struct ATSCacheKey : public TSCacheKey_t {
  ATSCacheKey() = default; // nullptr by default

  operator TSCacheKey() const { return get(); }
  ATSCacheKey(const atscppapi::Url &url, std::string const &etag, uint64_t offset);
};

// object to request write/read into cache
struct ATSCont : public TSCont_t {
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  template <class T_FUTURE>
  static TSCont create_temp_tscont(TSCont mutexSrc, T_FUTURE &cbFuture,
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());

public:
  explicit ATSCont(TSCont mutexSrc=nullptr); // no handler
  virtual ~ATSCont();

  // accepts TSHttpTxn handler functions
  template <class T_OBJ, typename T_DATA> 
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, T_DATA), T_DATA cbdata, TSCont mutexSrc=nullptr);

public:
  operator TSCont() const { return get(); }

  ATSCont &
  operator=(std::function<void(TSEvent, void *)> &&fxn)
  {
    _userCB = fxn;
    return *this;
  }

private:
  static int handleTSEventCB(TSCont cont, TSEvent event, void *data);

  // holds object and function pointer
  std::function<void(TSEvent, void *)> _userCB;
};

// cover up interface
struct ATSVConnFuture : private std::shared_future<TSVConn> 
{
  using std::shared_future<TSVConn>::operator=; // allow use

  explicit ATSVConnFuture() = default;
  explicit ATSVConnFuture(const ATSVConnFuture &) = default;

  ~ATSVConnFuture();

  int error() const;
  bool valid() const { return ! error(); }
  bool is_close_able() const;
  void reset() 
     { operator=(std::shared_future<TSVConn>{}); }
  TSVConn get() const 
     { return valid() ? std::shared_future<TSVConn>::get() : nullptr; }
};

struct ATSXformOutVConn
{
  friend ATSXformCont;
  using Uniq_t = std::unique_ptr<ATSXformOutVConn>;

  static Uniq_t create_if_ready(const ATSXformCont &xform, int64_t len, int64_t offset);

  ATSXformOutVConn(const ATSXformCont &xform, int64_t len, int64_t offset);

  operator TSVConn() const { return _outVConn; }
  operator TSVIO() const { return _outVIO; }
  operator TSIOBuffer() const { return _outBuffer; }
  operator TSIOBufferReader() const { return _outReader; }

  ~ATSXformOutVConn();

  void set_close_able(); // two phases: pre-VIO close and post-VIO close
  bool is_close_able() const; // two phases: pre-VIO close and post-VIO close
  bool check_refill(TSEvent event);

private:
  TSVConn const _inVConn = nullptr;
  TSVIO const _inVIO = nullptr;
  TSVConn const _outVConn = nullptr;
  TSIOBuffer _outBuffer;
  TSIOBufferReader _outReader;
  int64_t const _skipBytes;
  int64_t const _writeBytes;

  TSVIO _outVIO = nullptr;
  TSEvent _outVIOWaiting = TS_EVENT_NONE;
  uint64_t _outVIOWaitingTS = 0UL;

  ATSXformOutVConn() = delete;
  ATSXformOutVConn(ATSXformOutVConn&&) = delete;
  ATSXformOutVConn(const ATSXformOutVConn&) = delete;

};


// object to request write/read into cache
struct ATSXformCont : public TSCont_t {
  using XformCBFxn_t = int64_t(TSEvent, TSVIO, int64_t);
  using XformCB_t = std::function<int64_t(TSEvent, TSVIO, int64_t)>;
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  ATSXformCont() = delete; // nullptr by default
  ATSXformCont(ATSXformCont &&) = delete;
  ATSXformCont(atscppapi::Transaction &txn, TSHttpHookID xformType, int64_t bytes, int64_t offset = 0);

  // external-only body
  virtual ~ATSXformCont();

public:
  operator TSVConn() const { return get(); }

  TSVIO xformInputVIO() const { return TSVConnWriteVIOGet(get()); }
  TSVIO inputVIO() const { return _inVIO; }

  TSVConn output() const { return _outVConnU ? static_cast<TSVConn>(*_outVConnU) : nullptr; }
  TSVIO outputVIO() const { return _outVConnU ? static_cast<TSVIO>(*_outVConnU) : nullptr; }

  TSIOBuffer outputBuffer() const { return _outBufferU.get(); }
  TSIOBufferReader outputReader() const { return _outReaderU.get(); }

  bool using_two_buffers() const { return _inVIO && TSVIOVConnGet(_inVIO) != operator TSVConn(); }  // || TSVIOBufferGet(_inVIO) != outputBuffer(); }
  bool using_one_buffer() const { return _inVIO && TSVIOBufferGet(_inVIO) == outputBuffer(); }

  void reset_input_vio(TSVIO vio) { _inVIO = vio; }

  void
  set_body_handler(const XformCB_t &fxn)
  {
    _xformCB = fxn;
    TSDebug("cache_range_block", "%s",__func__);
  }

  int64_t copy_next_len(int64_t);
  int64_t skip_next_len(int64_t);

private:
  int check_completions(TSEvent event);

  void xform_input_event();  // for input events
  int xform_input_completion(TSEvent event);  // checks input events

  int handleXformTSEvent(TSCont cont, TSEvent event, void *data);

  void handleXformBufferEvent(TSEvent event, TSVIO evio);
  void handleXformInputEvent(TSEvent event, TSVIO evio);
  void handleXformOutputEvent(TSEvent event);

  static int handleXformTSEventCB(TSCont cont, TSEvent event, void *data);
protected:
  atscppapi::Transaction &_txn;
  const TSHttpTxn _atsTxn;

private:
  XformCB_t _xformCB;
  int64_t _xformCBAbsLimit = 0L;
  int64_t const _outSkipBytes;
  int64_t const _outWriteBytes;

  TSVIO _inVIO = nullptr;
  TSEvent _inVIOWaiting = TS_EVENT_NONE;

  ATSXformOutVConn::Uniq_t _outVConnU;
  TSIOBuffer_t const _outBufferU;
  TSIOBufferReader_t const _outReaderU;

  // for if WRITE_READY when _outVIO ran out...
  TSEvent _outVIOWaiting = TS_EVENT_NONE;
};



class BlockTeeXform : public ATSXformCont
{
  using HookType = std::function<int64_t(TSIOBufferReader, int64_t pos, int64_t len)>;

public:
  BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset);
  virtual ~BlockTeeXform() = default;

  TSIOBuffer teeBuffer() const { return _teeBufferP.get(); }
  TSIOBufferReader teeReader() const { return _teeReaderP.get(); }

  TSIOBufferReader cloneAndSkip(int64_t bytes);

public:
  int64_t inputEvent(TSEvent event, TSVIO vio, int64_t left);

  HookType _writeHook;
  TSIOBuffer_t _teeBufferP;
  TSIOBufferReader_t _teeReaderP;
};

//}
