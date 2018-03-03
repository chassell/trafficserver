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
#include <atscppapi/Mutex.h>

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

using TSCacheKeyO_t       = typename std::remove_pointer<TSCacheKey>::type;
using TSContO_t           = typename std::remove_pointer<TSCont>::type;
using TSMutexO_t          = typename std::remove_pointer<TSMutex>::type;
using TSMBufferO_t        = typename std::remove_pointer<TSMBuffer>::type;
using TSIOBufferO_t       = typename std::remove_pointer<TSIOBuffer>::type;
using TSIOBufferReaderO_t = typename std::remove_pointer<TSIOBufferReader>::type;
using TSVIOO_t            = typename std::remove_pointer<TSVIO>::type;

using TSMutexPtr_t = std::shared_ptr<std::remove_pointer<TSMutex>::type>;

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
} // namespace std

inline std::pair<int64_t,int64_t> vio_range_avail(TSVIO vio) { 
  auto rdr = TSVIOReaderGet(vio);
  if ( ! rdr ) {
    TSIOBufferReader_t temp{ TSIOBufferReaderAlloc(TSVIOBufferGet(vio)) };
    return std::make_pair(0,TSIOBufferReaderAvail(temp.get()));
  }
  int64_t avail = TSIOBufferReaderAvail(rdr);
  int64_t pos = TSVIONDoneGet(vio);
  avail = std::min(avail, TSVIONTodoGet(vio)); // don't span past end...
  return std::make_pair( pos, pos + avail );
}

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
    TSHttpTxnMilestoneGet(txn, TS_MILESTONE_SM_START, &begin);
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

struct ATSVIO
{
  ATSVIO() = default;
  ATSVIO(const ATSVIO &v) = default;
  ATSVIO(TSVIO v) : _vio(v) { }

  TSVIO get() const { return _vio; }

  bool operator==(TSVIO v) { return _vio == v; }
  bool operator!=(TSVIO v) { return _vio != v; }

  ATSVIO &operator=(const ATSVIO &v) { _vio = v._vio; return *this; }
  ATSVIO &operator=(TSVIO v) { _vio = v; return *this; }

  operator bool() const { return ready(); }
  operator TSVIO() const { return _vio; }

  operator TSIOBuffer() const 
     { return ( _vio ? TSVIOBufferGet(_vio) : nullptr ); }
  operator TSIOBufferReader() const 
     { return ( _vio ? TSVIOReaderGet(_vio) : nullptr ); }
  operator TSCont() const { return cont(); }

  TSCont cont() const
     { return ( _vio ? TSVIOContGet(_vio) : nullptr ); } // to-src events
  TSVConn vconn() const 
     { return ( _vio ? TSVIOVConnGet(_vio) : nullptr ); } // wr-dst or rd-src
  TSIOBuffer buffer() const 
     { return ( _vio ? TSVIOBufferGet(_vio) : nullptr ); } // wr-src or rd-dst
  TSIOBufferReader reader() const 
     { return ( _vio ? TSVIOReaderGet(_vio) : nullptr ); } // wr-src only

  std::pair<int64_t,int64_t> avail() const 
     { return vio_range_avail(_vio); }
  bool ready() const 
     { return _vio && operator TSIOBuffer(); }
  bool write_ready() const 
     { return ready() && operator TSIOBufferReader(); }
  bool active() const 
     { return ready() && TSVIONTodoGet(_vio); }
  bool completed() const 
     { return _vio && TSVIONDoneGet(_vio) && ! TSVIONTodoGet(_vio); }

  int64_t ndone() const 
     { return _vio ? TSVIONDoneGet(_vio) : -1; }
  int64_t nleft() const 
     { return _vio ? TSVIONTodoGet(_vio) : -1; }
  int64_t ntodo() const 
     { return _vio ? TSVIONTodoGet(_vio) : -1; }
  int64_t nbytes() const 
     { return _vio ? TSVIONBytesGet(_vio) : -1; }

  void drain(int64_t adj) 
     { _vio ? TSVIONDoneSet(_vio, ndone() + adj) : (void)-1; }
  void fill(int64_t adj)
     { _vio ? TSVIONBytesSet(_vio, nbytes() + adj) : (void)-1; }
  void ndone(int64_t n)
     { _vio ? TSVIONDoneSet(_vio,n) : (void)-1; }
  void nbytes(int64_t n)
     { _vio ? TSVIONBytesSet(_vio,n) : (void)-1; }

  int waiting() const 
     { return operator TSIOBufferReader() ? TSIOBufferReaderAvail(*this) : -1; }

  void shutdown();

  ATSVIO const ivc_writer() const { return _vio ? TSVConnWriteVIOGet(vconn()) : nullptr; }
  ATSVIO const ivc_input() const { return _vio ? TSVConnWriteVIOGet(vconn()) : nullptr; }
  ATSVIO ivc_writer() { return _vio ? TSVConnWriteVIOGet(vconn()) : nullptr; }
  ATSVIO ivc_input() { return _vio ? TSVConnWriteVIOGet(vconn()) : nullptr; }

  ATSVIO const ivc_reader() const { return _vio ? TSVConnReadVIOGet(vconn()) : nullptr; }
  ATSVIO const ivc_data() const { return _vio ? TSVConnReadVIOGet(vconn()) : nullptr; }
  ATSVIO ivc_reader() { return _vio ? TSVConnReadVIOGet(vconn()) : nullptr; }
  ATSVIO ivc_data() { return _vio ? TSVConnReadVIOGet(vconn()) : nullptr; }

  void free() {
    free_owned(); // discard any owned objs
    shutdown(); // allow a new VIO to same VConn
    const_cast<TSVIO&>(_vio) = nullptr;
  }

  bool is_ivc_writer() const { return _vio && ivc_writer() == _vio; }
  bool is_ivc_reader() const { return _vio && ivc_reader() == _vio; }

  void reenable() {
    if ( active() && reader() ) {
      write_send_event(vconn(),TS_EVENT_IMMEDIATE);
    } else if ( active() ) {
      read_send_event(vconn(),TS_EVENT_IMMEDIATE);
    }
  }

  // prone to deadlock .. but upstream is safer
  void write_send_event(TSCont evtcont, TSEvent evt) {
    atscppapi::ScopedContinuationLock llock(vconn()); // lock/re-lock downstream first
    atscppapi::ScopedContinuationLock ulock(cont()); // lock *upstream* second
    if ( evt == TS_EVENT_IMMEDIATE && evtcont == vconn() ) {
      TSVIOReenable(_vio); // active side must wakeup
    } else {
      TSContCall(evtcont,evt,_vio);
    }
  }

  // less prone to deadlock?
  void read_send_event(TSCont evtcont, TSEvent evt) {
    atscppapi::ScopedContinuationLock ulock(cont()); // lock/re-lock shared downstream first
    atscppapi::ScopedContinuationLock llock(vconn()); // lock *upstream* second [dead-end likely]
    if ( evt == TS_EVENT_IMMEDIATE && evtcont == vconn() ) {
      TSVIOReenable(_vio); // active side must wakeup
    } else {
      TSContCall(evtcont,evt,_vio);
    }
  }

  void send_chg_event(TSEvent evt) {
    ( reader() ? write_send_event(cont(),evt) : read_send_event(cont(),evt) );
  }

  void send_vc_event(TSEvent evt, void *data) {
    ( reader() ? write_send_event(vconn(),evt) : read_send_event(vconn(),evt) );
  }

  int64_t ivc_copy(int64_t inskip=0);
  int64_t ivc_transfer(int64_t inskip=0);

 public:
  void close_vconn();
  void free_owned();
  void complete_vio();

 protected:
  TSVIO _vio = nullptr;
};



// object to request write/read into cache
struct ATSCont : public TSCont_t {
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  static TSCont create_temp_tscont(TSCont mutexSrc, 
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
  template <class T_FUTURE>
  static TSCont create_temp_tscont(TSCont mutexSrc, T_FUTURE &cbFuture,
                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
//  static TSCont create_temp_tscont(TSVConn vconn, ATSVIOFuture &vioFuture, TSIOBufferReader buff, 
//                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
//  static TSCont create_temp_tsvio(TSCont mutex, ATSVIOFuture &vioFuture, TSIOBufferReader buff, 
//                                   const std::shared_ptr<void> &counted = std::shared_ptr<void>());
public:
  explicit ATSCont(TSCont mutexSrc=nullptr); // no handler
  explicit ATSCont(ATSCont &&old) : TSCont_t(old.release()), _userCB(std::move(old._userCB)) { 
//      TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): move ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
    }
  virtual ~ATSCont();

  // continuation with a method (no auto-dtor)
  template <class T_OBJ, typename T_DATA> 
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, const T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc=nullptr);

  // continuation with deletion upon return of TS_EVENT_NONE...
  template <typename T_DATA>
  ATSCont(TSEvent (*funcp)(TSCont, TSEvent, void *, const T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc=nullptr);

  // vconn-continuation with a method (no auto-dtor) and flag for "shutdown" status ...
  template <class T_OBJ, typename T_DATA>
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc);

  // vconn-continuation with a method (no auto-dtor) and flag for "shutdown" status ...
  template <class T_OBJ, typename T_DATA>
  ATSCont(atscppapi::Transaction &txn, T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, T_DATA&), T_DATA &&cbdataRV);
public:
  operator TSCont() const { return get(); }

  ATSCont &operator=(ATSCont &&cont) = default;

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
  explicit ATSFuture(ATSFuture &&) = default; // move ctor is okay
  explicit ATSFuture(TSFut_t &&other) 
     : TSFut_t(other) 
     { }

  explicit ATSFuture(T_DATA vconn) 
  {
    TSProm_t prom;
    static_cast<TSFut_t&>(*this) = prom.get_future();
    prom.set_value(vconn);
  }

  ~ATSFuture();

  ATSFuture &operator=(ATSFuture &&old) {
    ATSFuture(release()); // delete old future-value
    TSFut_t::operator=(old); // assign over empty one
    return *this;
  }

  operator bool() const { return TSFut_t::valid(); }
  int error() const;
  bool valid() const { return ! error(); }
  bool is_close_able() const;
  T_DATA release()
     { auto v = get(); operator=(TSFut_t{}); return v; }
  T_DATA get() const 
     { return valid() ? TSFut_t::get() : nullptr; }
};

// destructors are special for both...
template <> ATSFuture<TSVConn>::~ATSFuture();
template <> ATSFuture<TSVIO>::~ATSFuture();
using ATSVConnFuture = ATSFuture<TSVConn>;

class NullVConn;

struct ATSVConn
{
  static TSIOBuffer_t g_emptyBuffer;
  static TSIOBufferReader_t g_emptyReader;

  // standard obj-api *must* be invalid for a bit...
  ATSVConn() = default;
  ATSVConn(const ATSVConn &) = default;
  ATSVConn(TSVConn vc) : _vconn(vc) { }

  ~ATSVConn() { }

  operator bool() const { return pullvio().ready() || pushvio().ready(); }
  operator TSVConn() const { return _vconn; }
  ATSVIO pullvio() const { return _vconn ? TSVConnWriteVIOGet(_vconn) : nullptr; }
  ATSVIO pushvio() const { return _vconn ? TSVConnReadVIOGet(_vconn) : nullptr; }
  ATSVConn xformvc() const { return _vconn ? TSTransformOutputVConnGet(_vconn) : nullptr; }

 protected:
  ATSVConn &operator=(TSVConn vc) { _vconn = vc; return *this; }

 private:
  TSVConn _vconn = nullptr;
};



// Allo writes *into* and *from* a held buffer...
struct ATSVConnAPI : public ATSVConn
{
 public:
  // standard obj-api *must* be invalid for a bit...
  ATSVConnAPI() = default;
  ATSVConnAPI(const ATSVConnAPI &) = default;

  virtual ~ATSVConnAPI() = 0; // no obvious dtor possible

  ATSVIO input() const { return pullvio(); }
  ATSVIO data() const { return pushvio(); }
  int64_t data_base() const { return data().ndone(); }
  int64_t data_newest() const { return data().nbytes(); }
  int64_t data_window(const ATSVIO &vio) const { return data_newest() - (vio.ndone() + data_base()); }
  int64_t data_reader_pos(TSIOBufferReader rdr) const { return data_newest() - TSIOBufferReaderAvail(rdr); }

  int64_t input_left() { return input() && input().reader() == g_emptyReader.get() 
                                         && input().cont() == operator TSVConn()   // not for elsewhere
                             ? input().ntodo() - data().ntodo() : 0; }

  bool dummy_input() { 
     return input().cont() == *this && input().reader() == g_emptyReader.get(); 
  } // own pullvio handled by me?

  // reset exact position

 public:
  // NOTE: auto-wakeup via VC for starting read/write 
  virtual TSVIO add_ext_input(TSVConn vc, int64_t len)               { return nullptr; }
  virtual TSVIO add_outflow(ATSVConnAPI &vc, int64_t len, int64_t skip=0L) { return nullptr; }
  virtual TSVIO add_outflow(TSVConn vc, int64_t len, int64_t skip=0L) { return nullptr; }
  virtual TSVIO add_stream(TSVConn vc)                            { return nullptr; }
  virtual TSVIO add_tee_stream(TSVConn vc)                        { return nullptr; }

  virtual void on_input(int64_t bytes) = 0;      // new input with data 
  virtual void on_input_ended(int64_t bytes) = 0; // input source is stopping
  virtual void on_reenabled(int64_t bytes) = 0;  // possibly new data

  // cannot ignore queued inputs..
  virtual void on_pre_input(TSCont wrreply, int64_t len) { 
    ink_assert(!"lost critical event");
  }

  // never-blocked reads are okay by default...
  virtual void on_read_blocked(int64_t bytes, ATSVIO vio) { 
    ink_assert(!"lost critical event");
  }
  virtual void on_read_ended(int64_t bytes, ATSVIO vio) { 
    // data now appended somewhere
  }

  // used to clone/replace a new write to this VConn
  virtual void on_writer_chain(ATSVIO vio) {
    ink_assert(!"lost critical event");
  }

  // never-blocked writes (with vconn as continuation) is okay...
  virtual void on_writer_blocked(ATSVIO vio) {
    ink_assert(!"lost critical event");
  }
  virtual void on_writer_ended(ATSVIO vio) {
    // data was read from from somewhere
  }

  // unknown events are not okay
  virtual void on_event(TSEvent evt, void *data) {
    ink_assert(!"unknown event");
  }

 protected:
  void handleVCEvent(int closed, TSEvent event, void *evtdata, std::shared_ptr<void>&);

  int64_t chk_input_ready(int64_t xfer);
  void set_dummy_input() { 
    TSVConnWrite(*this,*this,g_emptyReader.get(),0);
  }
};

class NullVConn : public ATSVConnAPI
{
 public: 
  NullVConn() {
    ATSVConn::operator=(_cont);
//    operator=(_cont.operator TSCont());
  }

  ~NullVConn() { TSVConnClose(_cont.get()); }

 protected:
  void on_input(int64_t bytes) override { };       // new input with data 
  void on_input_ended(int64_t bytes) override { }; // input source is stopping
  void on_reenabled(int64_t bytes) override { };   // possibly new data

  ATSCont const _cont{ static_cast<ATSVConnAPI&>(*this), &NullVConn::handleVCEvent, std::shared_ptr<void>(), nullptr };
};


class BufferVConn : public ATSVConnAPI
{
 public:
  BufferVConn() : _cont() { } // totally empty...
  BufferVConn(BufferVConn &&orig) = default;

  BufferVConn(std::shared_ptr<void> refptr) :
     _cont{ static_cast<ATSVConnAPI&>(*this), &BufferVConn::handleVCEvent, std::move(refptr), nullptr }
  {
    ATSVConn::operator=(_cont);
  }

  BufferVConn(atscppapi::Transaction &txn, std::shared_ptr<void> refptr) :
     _cont{ txn, static_cast<ATSVConnAPI&>(*this), &BufferVConn::handleVCEvent, std::move(refptr) }
  {
    ATSVConn::operator=(_cont);
  }

  virtual ~BufferVConn();

  int error() const { return ( data() ? 0 : _nextBasePos+0 ); }

  void add_output_skip(int64_t skip) { _nextBasePos >= 0 ? (_nextBasePos += skip) : 0; }

  // NOTE: auto-wakeup via VC for starting read/write 
  TSVIO add_ext_input(TSVConn vc, int64_t len) override;
  TSVIO add_outflow(ATSVConnAPI &vc, int64_t len, int64_t skip=0L) override;
  TSVIO add_outflow(TSVConn vc, int64_t len, int64_t skip=0L) override;
  TSVIO add_stream(TSVConn vc) override;
  TSVIO add_tee_stream(TSVConn vc) override;

 protected:
  void on_input(int64_t bytes) override;      // new input with data 
  void on_input_ended(int64_t bytes) override; // input source is stopping
  void on_reenabled(int64_t bytes) override;  // possibly new data

  void on_pre_input(TSVConn wrreply, int64_t len) override;

  // destination of reads (queueable)
  void on_read_blocked(int64_t bytes, ATSVIO vio) override;
  void on_read_ended(int64_t bytes, ATSVIO vio) override;

  // source of writes (queueable)
  void on_writer_chain(ATSVIO vio) override;
  void on_writer_blocked(ATSVIO vio) override;
  void on_writer_ended(ATSVIO vio) override;
  void on_event(TSEvent evt, void *data) override;

  void input_shutdown() { 
    for ( auto && pair : _preInputs ) {
      atscppapi::ScopedContinuationLock llock(*this); // lock/re-lock downstream first
      atscppapi::ScopedContinuationLock ulock(pair.first); // lock *upstream* second
      TSContCall(pair.first,TS_EVENT_VCONN_WRITE_COMPLETE,input().operator TSVIO()); // done..
    }
    _preInputs.clear();
    input().free();
  }

  void error_shutdown(int e) { 
    input_shutdown();

    for ( auto && pair : _outputQueue ) {
      auto cont = pair.first.cont();
      pair.first.shutdown();
      TSContSchedule(cont, 0, TS_THREAD_POOL_TASK); // wakeup..
    }
    _outputQueue.clear();

    _baseReader.release(); // will destroy buff
    _currReader.release(); // will destroy buff
    data().free(); 
    _nextBasePos = e; 
  }


  ATSVIO               _outvio; // needs reenable if non-null
  ATSVIO               _teevio; // needs reenable if non-null [uses prevReader only once]

 private:
  void init();

  TSVIO add_outflow_any(TSVConn vc, int64_t len, int64_t skip=0L, ATSVConnAPI *vcapi=nullptr);

 private:
  // default constructor
  ATSCont              _cont{ static_cast<ATSVConnAPI&>(*this), &BufferVConn::handleVCEvent, std::shared_ptr<void>(), nullptr };

  TSIOBufferReader_t   _baseReader; // always-available reader clone
  TSIOBufferReader_t   _currReader; // latest live reader clone (or null).. for outvio
  std::atomic<int64_t> _nextBasePos{0L};

  std::vector<std::pair<TSVConn,int64_t>> _preInputs; // a WRITE_READY event to start input ...
  std::vector<std::pair<ATSVIO,int64_t>>  _outputQueue; // apply to on_writer_chain() to start true write ...
};


class ProxyVConn : public BufferVConn 
{
 public:
  ProxyVConn() = default;

  ProxyVConn(std::shared_ptr<void> &&refptr) 
     : BufferVConn(std::move(refptr)) { }

  ProxyVConn(std::function<TSAction(TSCont cont)> &&fxn) 
     : _retry(fxn) { }
   
  virtual ~ProxyVConn();

  void on_event(TSEvent evt, void *data);

 private:
  std::function<TSAction(TSCont cont)> _retry;
};

// object to request write/read into cache
class ATSXformVConn : private BufferVConn 
{
  template <class T, typename... Args> friend std::unique_ptr<T> std::make_unique(Args &&... args);

public:
  ATSXformVConn() = delete; // nullptr by default
  ATSXformVConn(ATSXformCont &&) = delete;
  ATSXformVConn(atscppapi::Transaction &txn, int64_t bytes, int64_t offset = 0);

  // external-only body
  virtual ~ATSXformVConn();

public:
  // detect output source... to pick...
  ATSVIO outputVIO() const { return xformvc().pullvio(); }
  const ATSVConn &inputVC() const { return outputVIO().reader() == _bufferVC.data() ? _bufferVC : *this; }

  ATSVConn bufferVC() const { return _bufferVC; }
  ATSVIO xformInputVIO() const { return input(); }

  void reset_output_length(int64_t len) { xformvc().pullvio().nbytes(len); }

private:
  int check_completions(TSEvent event);
  int check_transform_commit(); // detect current hook and begin transform when enabled

protected:
  int _txnID = ThreadTxnID::get();

private:
  TSHttpTxn    _txn;
  TSHttpHookID _transformHook; // needs init.. but may be reset!
  BufferVConn  _bufferVC; // non-transform-fed buffer
};

// accepts TSHttpTxn handler functions
template <class T_OBJ, typename T_DATA>
ATSCont::ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, const T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc)
  : TSCont_t(TSContCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  // memorize user data to forward on
  auto cbdata = std::move(cbdataRV); // remove the orig
  _userCB = decltype(_userCB)([&obj, funcp, cbdata](TSEvent event, void *evtdata) { 
    (obj.*funcp)(event, evtdata, cbdata); 
  });

  // deletion from obj's scope .. [helped by Cont-data nullptr]
}

template <typename T_DATA>
ATSCont::ATSCont(TSEvent (*funcp)(TSCont, TSEvent, void *, const T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc)
  : TSCont_t(TSContCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  // memorize user data to forward on
  auto cbdata = std::move(cbdataRV); // remove the orig
  _userCB = decltype(_userCB)(
    [this, funcp, cbdata](TSEvent event, void *evtdata) 
    {
       if ( (*funcp)(*this, event, evtdata, cbdata) != TS_EVENT_CONTINUE ) {
         delete this;
       }
    });
}

// accepts TSHttpTxn handler functions
template <class T_OBJ, typename T_DATA>
ATSCont::ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, T_DATA&), T_DATA &&cbdataRV, TSCont mutexSrc)
  : TSCont_t(TSVConnCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  auto cbdata = std::move(cbdataRV); // remove the orig
  auto vconnp = get();

  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj, funcp, vconnp, cbdata](TSEvent event, void *evtdata) mutable {
      (obj.*funcp)(TSVConnClosedGet(vconnp), event, evtdata, cbdata); 
  });

  // deletion from obj's scope .. [helped by Cont-data nullptr]
}

template <class T_OBJ, typename T_DATA>
ATSCont::ATSCont(atscppapi::Transaction &txn, T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, T_DATA&), T_DATA &&cbdataRV)
  : TSCont_t(TSTransformCreate(&ATSCont::handleTSEventCB, static_cast<TSHttpTxn>(txn.getAtsHandle())))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  auto cbdata = std::move(cbdataRV); // remove the orig
  auto vconnp = get();

  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj, funcp, vconnp, cbdata](TSEvent event, void *evtdata) mutable {
      (obj.*funcp)(TSVConnClosedGet(vconnp), event, evtdata, cbdata); 
  });

  // deletion from obj's scope .. [helped by Cont-data nullptr]
}


//}
