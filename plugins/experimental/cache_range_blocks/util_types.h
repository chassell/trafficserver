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
#include "ts/INK_MD5.h"

#include <atscppapi/Url.h>
#include <atscppapi/Transaction.h>

#include <memory>
#include <string>
#include <vector>
#include <future>

#pragma once

#define VARY_TAG "Vary"
#define ETAG_TAG "ETag"
#define IF_MATCH_TAG "If-Match"
#define IF_MODIFIED_SINCE_TAG "If-Modified-Since"
#define IF_NONE_MATCH_TAG "If-None-Match"
#define IF_RANGE_TAG "If-Range"
#define CONTENT_ENCODING_TAG "Content-Encoding"
#define CONTENT_LENGTH_TAG "Content-Length"
#define RANGE_TAG "Range"
#define CONTENT_RANGE_TAG "Content-Range"
#define ACCEPT_ENCODING_TAG "Accept-Encoding"
#define X_BLOCK_BITSET_TAG "X-Block-Bitset"

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

void forward_vio_event(TSEvent event, TSVIO invio, TSCont mutexCont);

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
  TSDebug("cache_range_blocks", "%s: TSCont destroyed %p",__func__,cont);
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

inline std::pair<int64_t,int64_t> write_range_avail(TSVIO vio) { 
  auto rdr = TSVIOReaderGet(vio);
  if ( ! rdr ) {
    return std::make_pair(0,0);
  }
  int64_t avail = TSIOBufferReaderAvail(rdr);
  int64_t pos = TSVIONDoneGet(vio);
  return std::make_pair( pos, pos + avail );
}

// unique-ref object for a cache-read or cache-write request
struct ATSCacheKey : public TSCacheKey_t {
  explicit ATSCacheKey() : TSCacheKey_t{TSCacheKeyCreate()} { }
  explicit ATSCacheKey(TSCacheKey key) : TSCacheKey_t{key} { }
  explicit ATSCacheKey(ATSCacheKey &&old) : TSCacheKey_t{old.release()} { }
  ATSCacheKey(const std::string &url, std::string const &etag, uint64_t offset);

  operator TSCacheKey() const { return get(); }
  bool valid() const {
    return get() && *reinterpret_cast<const ats::CryptoHash*>(get()) != ats::CryptoHash();
  }

};

// object to request write/read into cache
struct ATSCont : public TSCont_t {
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  template <class T_FUTURE>
  static TSCont create_temp_tscont(TSCont mutexSrc, T_FUTURE &cbFuture,
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
  static TSCont create_temp_tscont(TSCont mutexSrc, 
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
public:
  explicit ATSCont(TSCont mutexSrc=nullptr); // no handler
  explicit ATSCont(ATSCont &&old) : TSCont_t(old.release()), _userCB(std::move(old._userCB)) { 
//      TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): move ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
    }
  virtual ~ATSCont();

  // handle TSHttpTxn continuations with a method (no dtor implied)
  template <class T_OBJ, typename T_DATA> 
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, const T_DATA&), T_DATA cbdata, TSCont mutexSrc=nullptr);

  // handle TSHttpTxn continuations with a function and a copied type
  template <typename T_DATA>
  ATSCont(TSEvent (*funcp)(TSCont, TSEvent, void *, const T_DATA&), T_DATA &&cbdata, TSCont mutexSrc=nullptr);

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
  using TSVConnFut_t = std::shared_future<TSVConn>; // allow use
  using TSVConnProm_t = std::promise<TSVConn>; // allow use
  using TSVConnFut_t::operator=; // allow use

  explicit ATSVConnFuture() = default;
  explicit ATSVConnFuture(const ATSVConnFuture &) = default;

  explicit ATSVConnFuture(TSVConn vconn) 
  {
    TSVConnProm_t prom;
    static_cast<TSVConnFut_t&>(*this) = prom.get_future();
    prom.set_value(vconn);
  }

  ~ATSVConnFuture();

  ATSVConnFuture &operator=(ATSVConnFuture &&old) {
    *this = static_cast<TSVConnFut_t&&>(old);
    return *this;
  }

  operator bool() const { return std::shared_future<TSVConn>::valid(); }
  int error() const;
  bool valid() const { return ! error(); }
  bool is_close_able() const;
  TSVConn release()
     { auto v = get(); operator=(std::shared_future<TSVConn>{}); return v; }
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
  ATSXformCont(atscppapi::Transaction &txn);
  ATSXformCont(atscppapi::Transaction &txn, int64_t bytes, int64_t offset = 0);

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

  std::pair<int64_t,int64_t> xformInputAvail() const { return write_range_avail(xformInputVIO()); }
  std::pair<int64_t,int64_t> inputAvail() const { return write_range_avail(inputVIO()); }
  std::pair<int64_t,int64_t> outputAvail() const { return write_range_avail(outputVIO()); }

  void reset_input_vio(TSVIO vio) { _inVIO = vio; }

  void
  set_body_handler(const XformCB_t &fxn)
  {
    _xformCB = fxn;
    TSDebug("cache_range_blocks", "%s",__func__);
  }

  int64_t copy_next_len(int64_t);
  int64_t skip_next_len(int64_t);

private:
  int check_completions(TSEvent event);

  int xform_input_event();  // for pure input events
  int xform_input_completion(TSEvent event);  // checks input events
  const char *xform_input_completion_desc(int error);

  int handleXformTSEvent(TSCont cont, TSEvent event, void *data);

  int handleXformBufferEvent(TSEvent event, TSVIO evio);
  int handleXformInputEvent(TSEvent event, TSVIO evio);
  int handleXformOutputEvent(TSEvent event);

  static int handleXformTSEventCB(TSCont cont, TSEvent event, void *data);
protected:
  int _txnID;

private:
  XformCB_t _xformCB;
  int64_t const _outSkipBytes;
  int64_t const _outWriteBytes;

  int _inLastError = 0;
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
  using HookType = std::function<void(TSIOBufferReader, int64_t inpos, int64_t newlen, int64_t added)>;

public:
  BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook);
  BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset);
  virtual ~BlockTeeXform() = default;

  TSIOBuffer teeBuffer() const { return _teeBufferP.get(); }
  TSIOBufferReader teeReader() const { return _teeReaderP.get(); }

  std::pair<int64_t,int64_t> teeAvail() const { 
    int64_t avail = TSIOBufferReaderAvail(_teeReaderP.get());
    return std::make_pair( _lastInputNDone - avail, _lastInputNDone );
  }

  TSIOBufferReader cloneAndSkip(int64_t bytes);
  void teeReenable();

private:
  int64_t inputEvent(TSEvent event, TSVIO vio, int64_t left);

  int64_t _lastInputNDone = 0;
  HookType _writeHook;
  TSIOBuffer_t _teeBufferP;
  TSIOBufferReader_t _teeReaderP;
};


// accepts TSHttpTxn handler functions
template <class T_OBJ, typename T_DATA>
ATSCont::ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, const T_DATA&), T_DATA cbdata, TSCont mutexSrc)
  : TSCont_t(TSContCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  static_cast<void>(cbdata);

  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj, funcp, cbdata](TSEvent event, void *evtdata) { 
    (obj.*funcp)(event, evtdata, cbdata); 
  });

  // deletion from obj's scope .. [helped by Cont-data nullptr]
}

template <typename T_DATA>
ATSCont::ATSCont(TSEvent (*funcp)(TSCont, TSEvent, void *, const T_DATA&), T_DATA &&cbdata, TSCont mutexSrc)
  : TSCont_t(TSContCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  static_cast<void>(cbdata);
  // memorize user data to forward on
  _userCB = decltype(_userCB)(
    [this, funcp, cbdata](TSEvent event, void *evtdata) 
    {
       if ( (*funcp)(*this, event, evtdata, cbdata) != TS_EVENT_CONTINUE ) {
         delete this;
       }
    });
}


//}
