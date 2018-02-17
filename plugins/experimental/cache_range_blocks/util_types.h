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
TSVConn spawn_sub_range(atscppapi::Transaction &origTxn, int64_t begin, int64_t end);
void spawn_range_request(atscppapi::Transaction &origTxn, int64_t begin, int64_t end, int64_t rdlen);

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
using TSVIO_t            = std::unique_ptr<std::remove_pointer<TSVIO>::type>;

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
  if ( buff ) {
    TSMBufferDestroy(buff);
  }
}
template <>
inline void
default_delete<TSIOBuffer_t::element_type>::operator()(TSIOBuffer buff) const
{
  if ( buff ) {
    TSIOBufferDestroy(buff);
  }
}
template <>
inline void
default_delete<TSIOBufferReader_t::element_type>::operator()(TSIOBufferReader reader) const
{
  if ( reader ) {
    TSIOBufferReaderFree(reader);
  }
}

template <> void default_delete<TSVIO_t::element_type>::operator()(TSVIO vio) const;

}

inline std::pair<int64_t,int64_t> write_range_avail(TSVIO vio) { 
  auto rdr = TSVIOReaderGet(vio);
  if ( ! rdr ) {
    return std::make_pair(0,0);
  }
  int64_t avail = TSIOBufferReaderAvail(rdr);
  int64_t pos = TSVIONDoneGet(vio);
  avail = std::min(avail, TSVIONTodoGet(vio)); // don't span past end...
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

class ThreadTxnID
{
  static thread_local int g_pluginTxnID;
  static std::atomic<long> g_ident;

public:
  static int get() { 
    return g_pluginTxnID;
//    return ( // atscppapi::TransactionPlugin::getTxnID() >= 0 
//             //    ? atscppapi::TransactionPlugin::getTxnID() : 
  }
  static int create_id(TSHttpTxn txn) {
    TSHRTime begin = 0;
    TSHttpTxnMilestoneGet(txn, TS_MILESTONE_UA_READ_HEADER_DONE, &begin);
    return (begin % 9000) + 1000;
  }

  ThreadTxnID(atscppapi::Transaction &txn) {
//    g_pluginTxnID = TSHttpTxnIdGet(static_cast<TSHttpTxn>(txn.getAtsHandle()));
    g_pluginTxnID = create_id(static_cast<TSHttpTxn>(txn.getAtsHandle()));
  }
  ThreadTxnID(TSHttpTxn txn) {
//    g_pluginTxnID = TSHttpTxnIdGet(txn);
    g_pluginTxnID = create_id(txn);
  }
  ThreadTxnID(int txnid) {
    g_pluginTxnID = txnid;
  }
  ~ThreadTxnID() {
    g_pluginTxnID = _oldTxnID; // restore
  }

private:
  int _oldTxnID = g_pluginTxnID; // always save it ..
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
template <typename T_DATA>
struct ATSFuture : private std::shared_future<T_DATA> 
{
  using TSFut_t = std::shared_future<T_DATA>; // allow use
  using TSProm_t = std::promise<T_DATA>; // allow use
  using TSFut_t::operator=; // allow use

  explicit ATSFuture() = default;
  explicit ATSFuture(const ATSFuture &) = default;

  explicit ATSFuture(T_DATA vconn) 
  {
    TSProm_t prom;
    static_cast<TSFut_t&>(*this) = prom.get_future();
    prom.set_value(vconn);
  }

  ~ATSFuture();

  ATSFuture &operator=(ATSFuture &&old) {
    *this = static_cast<TSFut_t&&>(old);
    return *this;
  }

  operator bool() const { return std::shared_future<T_DATA>::valid(); }
  int error() const;
  bool valid() const { return ! error(); }
  bool is_close_able() const;
  T_DATA release()
     { auto v = get(); operator=(std::shared_future<T_DATA>{}); return v; }
  T_DATA get() const 
     { return valid() ? std::shared_future<T_DATA>::get() : nullptr; }

};

using ATSVConnFuture = ATSFuture<TSVConn>;
using ATSVIOFuture = ATSFuture<TSVIO>;


struct ATSXformOutVConn
{
  friend ATSXformCont;
  using Uniq_t = std::unique_ptr<ATSXformOutVConn>;

  static Uniq_t create_if_ready(const ATSXformCont &xform, int64_t len, int64_t offset);

  ATSXformOutVConn(const ATSXformCont &xform, int64_t len, int64_t offset);

  operator TSVIO() const { return _outVIO; }
  std::pair<int64_t,int64_t> outputAvail() const { return write_range_avail(_outVIO); }
  std::pair<int64_t,int64_t> inputAvail() const { return write_range_avail(_inVIO); }

  ~ATSXformOutVConn();

  void set_close_able(); // two phases: pre-VIO close and post-VIO close
  bool is_close_able() const; // two phases: pre-VIO close and post-VIO close
  bool check_refill(TSEvent event);

private:
  TSCont const _cont;
  int64_t const _skipBytes;
  int64_t const _writeBytes;

  TSVIO_t _outVIOPtr; // shut down automatically
  TSVIO const _outVIO;

  TSVIO _inVIO = nullptr;

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
  ATSXformCont(atscppapi::Transaction &txn, int64_t bytes, int64_t offset = 0);

  // external-only body
  virtual ~ATSXformCont();

public:
  operator TSVConn() const { return get(); }

  TSVIO xformInputVIO() const { return TSVConnWriteVIOGet(get()); }
  TSVIO inputVIO() const { return _inVIO; }
  TSIOBufferReader inputReader() const { return TSVIOReaderGet(_inVIO); }

  TSVIO outputVIO() const { return _outVConnU ? _outVConnU->operator TSVIO() : nullptr; }

  TSIOBuffer outputBuffer() const { return _outBufferU.get(); }

  int64_t outputLen() const { return _outWriteBytes; }
  int64_t outputOffset() const { return _outSkipBytes; }

  std::pair<int64_t,int64_t> xformInputAvail() const { return write_range_avail(xformInputVIO()); }
  std::pair<int64_t,int64_t> inputAvail() const { return write_range_avail(inputVIO()); }
  std::pair<int64_t,int64_t> outputAvail() const { return write_range_avail(outputVIO()); }
  std::pair<int64_t,int64_t> bufferAvail() const {
      if ( ! outputVIO() ) {
        int64_t ndone = TSVIONDoneGet(inputVIO());
        return std::make_pair( ndone, ndone );
      }
      auto r = write_range_avail(outputVIO());
      r.first += _outSkipBytes; // offset + or -
      r.second += _outSkipBytes; // offset + or -
      return std::move(r);
    }

  void reset_input_vio(TSVIO vio);
  void reset_output_length(int64_t len) { _outWriteBytes = len; }

  void
  set_body_handler(const XformCB_t &fxn)
  {
    _xformCB = fxn;
    TSDebug("cache_range_blocks", "%s",__func__);
  }

  void
  disable_transform_start() {
    _transformHook = TS_HTTP_LAST_HOOK;
  }

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
  int _txnID = ThreadTxnID::get();

private:
  XformCB_t _xformCB;
  int64_t _outSkipBytes;
  int64_t _outWriteBytes;

  TSHttpHookID _transformHook; // needs init.. but may be reset!

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
  BlockTeeXform(atscppapi::Transaction &txn, HookType &&writeHook, int64_t xformLen, int64_t xformOffset);
  virtual ~BlockTeeXform() = default;

  TSIOBuffer teeBuffer() const { return _teeBufferP.get(); }
  TSIOBufferReader teeReader() const { return _teeReaderP.get(); }

  std::pair<int64_t,int64_t> teeAvail() const { 
    int64_t avail = TSIOBufferReaderAvail(_teeReaderP.get());
    return std::make_pair( _lastInputNDone - avail, _lastInputNDone );
  }

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
