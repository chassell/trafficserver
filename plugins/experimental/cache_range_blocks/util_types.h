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

  operator TSVIO() const { return _vio; }

  operator TSIOBuffer() const 
     { return ( _vio ? TSVIOBufferGet(_vio) : nullptr ); }
  operator TSIOBufferReader() const 
     { return ( _vio ? TSVIOReaderGet(_vio) : nullptr ); }
  operator TSCont() const { return cont(); }

  TSCont cont() const
     { return ( _vio ? TSVIOContGet(_vio) : nullptr ); }
  TSVConn vconn() const 
     { return ( _vio ? TSVIOVConnGet(_vio) : nullptr ); }

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

  operator bool() const { return ready(); }

  void shutdown() {
    TSVConnShutdown(vconn(), ! write_ready(), write_ready());
  }

  ATSVIO const ivc_writer() const { return TSVConnWriteVIOGet(vconn()); }
  ATSVIO const ivc_input() const { return TSVConnWriteVIOGet(vconn()); }
  ATSVIO ivc_writer() { return TSVConnWriteVIOGet(vconn()); }
  ATSVIO ivc_input() { return TSVConnWriteVIOGet(vconn()); }

  ATSVIO const ivc_reader() const { return TSVConnReadVIOGet(vconn()); }
  ATSVIO const ivc_buffer() const { return TSVConnReadVIOGet(vconn()); }
  ATSVIO ivc_reader() { return TSVConnReadVIOGet(vconn()); }
  ATSVIO ivc_buffer() { return TSVConnReadVIOGet(vconn()); }

  void free() {
    free_owned(); // discard any owned objs
    shutdown(); // allow a new VIO to same VConn
    const_cast<TSVIO&>(_vio) = nullptr;
  }

  bool is_ivc_writer() const { return ivc_writer() == _vio; }
  bool is_ivc_reader() const { return ivc_reader() == _vio; }

  void send_src_event(TSEvent evt) {
    if ( cont() ) {
       atscppapi::ScopedContinuationLock lock(cont());
       TSContCall(cont(),evt,_vio);
    }
  }
  void send_dst_event(TSEvent evt, void *data) {
    if ( vconn() ) {
       atscppapi::ScopedContinuationLock lock(vconn());
       TSContCall(vconn(),evt,data);
    }
  }

  int64_t ivc_copy(int64_t inskip=0);
  int64_t ivc_transfer(int64_t inskip=0);

 protected:
  void close_vconn();
  void free_owned();
  void complete_vio();

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
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(TSEvent, void *, const T_DATA&), T_DATA cbdata, TSCont mutexSrc=nullptr);

  // continuation with deletion upon return of TS_EVENT_NONE...
  template <typename T_DATA>
  ATSCont(TSEvent (*funcp)(TSCont, TSEvent, void *, const T_DATA&), T_DATA &&cbdata, TSCont mutexSrc=nullptr);

  // vconn-continuation with a method (no auto-dtor) and flag for "shutdown" status ...
  template <class T_OBJ, typename T_DATA>
  ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, const T_DATA&), T_DATA cbdata, TSCont mutexSrc);
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
  // standard obj-api *must* be invalid for a bit...
  ATSVConn() = default;
  ATSVConn(const ATSVConn &) = default;
  ATSVConn(TSVConn vc) : _vconn(vc) { }

  operator TSVConn() const { return _vconn; }
  ATSVIO input() const { return TSVConnWriteVIOGet(_vconn); }

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

  ATSVIO buffer() const { return TSVConnReadVIOGet(operator TSVConn()); }
  int64_t buffer_base() const { return buffer().ndone(); }
  int64_t buffer_newest() const { return buffer().nbytes(); }

  int64_t waiting(const ATSVIO &vio) const { return buffer_newest() - (vio.ndone() + buffer_base()); }

 public:
  // NOTE: auto-wakeup via VC for starting read/write 
  virtual TSVIO add_output(TSVConn vc, int64_t len, int64_t skip) { return nullptr; }
  virtual TSVIO add_stream(TSVConn vc)                            { return nullptr; }
  virtual TSVIO add_tee_stream(TSVConn vc)                        { return nullptr; }


  virtual void on_input(int64_t bytes) = 0;      // new input with data 
  virtual void on_input_ended(int64_t bytes) = 0; // input source is stopping
  virtual void on_reenabled(int64_t bytes) = 0;  // possibly new data

  // cannot ignore queued inputs..
  virtual void on_pre_input(TSCont wrreply) { 
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
  virtual void on_writer_blocked(int64_t bytes, ATSVIO vio) {
    ink_assert(!"lost critical event");
  }
  virtual void on_writer_ended(int64_t bytes, ATSVIO vio) {
    // data was read from from somewhere
  }

  // unknown events are not okay
  virtual void on_event(TSEvent evt, void *data) {
    ink_assert(!"unknown event");
  }

 protected:
  void handleTSEvent(int closed, TSEvent event, void *evtdata, const std::shared_ptr<void>&);

  int64_t bytes_ready();
};

class NullVConn : public ATSVConnAPI
{
 public: 
  NullVConn() {
    ATSVConn::operator=(_cont);
//    operator=(_cont.operator TSCont());
  }

 protected:
  void on_input(int64_t bytes) override { };       // new input with data 
  void on_input_ended(int64_t bytes) override { }; // input source is stopping
  void on_reenabled(int64_t bytes) override { };   // possibly new data

  ATSCont const _cont{ static_cast<ATSVConnAPI&>(*this), &NullVConn::handleTSEvent, std::shared_ptr<void>(), nullptr };
};


class BufferVConn : public ATSVConnAPI
{
  BufferVConn() { 
    ATSVConn::operator=(_cont);
//    static_cast<ATSVConn&>(*this).operator=(_cont.operator TSCont());
  }
  virtual ~BufferVConn();

  void add_input_skip(int64_t skip) { skip ? (_inputSkip += skip) : 0; }

  // NOTE: auto-wakeup via VC for starting read/write 
  TSVIO add_output(TSVConn vc, int64_t len, int64_t skip) override;
  TSVIO add_stream(TSVConn vc) override;
  TSVIO add_tee_stream(TSVConn vc) override;
  void on_input(int64_t bytes) override;      // new input with data 
  void on_input_ended(int64_t bytes) override; // input source is stopping
  void on_reenabled(int64_t bytes) override;  // possibly new data

  void on_pre_input(TSCont wrreply) override;

  // destination of reads (queueable)
  void on_read_blocked(int64_t bytes, ATSVIO vio) override;
  void on_read_ended(int64_t bytes, ATSVIO vio) override;

  // source of writes (queueable)
  void on_writer_chain(ATSVIO vio) override;
  void on_writer_blocked(int64_t bytes, ATSVIO vio) override;
  void on_writer_ended(int64_t bytes, ATSVIO vio) override;
  void on_event(TSEvent evt, void *data) override;

  ATSCont        const _cont{ static_cast<ATSVConnAPI&>(*this), &BufferVConn::handleTSEvent, std::shared_ptr<void>(), nullptr };
  TSIOBufferReader_t   _prevReader; // [last reader assigned to a write]
  TSIOBufferReader_t   _currReader; // [last reader assigned to a write]
  ATSVIO               _outvio; // needs reenable if non-null
  ATSVIO               _teevio; // needs reenable if non-null

  std::vector<TSCont>  _preInputs;
  std::vector<TSVConn> _outputQueue;

  std::atomic<int64_t> _inputSkip{0L};
  std::atomic<int64_t> _resetWriteLen{0L};
};



struct ATSXformOutVConn
{
  friend ATSXformCont;
  using Uniq_t = std::unique_ptr<ATSXformOutVConn>;

  static Uniq_t create_if_ready(const ATSXformCont &xform, TSIOBufferReader rdr, int64_t len, int64_t offset);

  ATSXformOutVConn(const ATSXformCont &xform, TSIOBufferReader rdr, int64_t len, int64_t offset);

  operator TSVIO() const { return _outVIO; }
  operator TSIOBuffer() const { return TSVIOBufferGet(_outVIO); }
  operator TSIOBufferReader() const { return TSVIOReaderGet(_outVIO); }
  std::pair<int64_t,int64_t> outputAvail() const { return vio_range_avail(_outVIO); }
  std::pair<int64_t,int64_t> inputAvail() const { return vio_range_avail(_inVIO); }

  ~ATSXformOutVConn();

  void set_close_able(); // two phases: pre-VIO close and post-VIO close
  bool is_close_able() const; // two phases: pre-VIO close and post-VIO close
  bool check_refill(TSEvent event);

private:
  TSCont const _cont;
  int64_t const _skipBytes;
  int64_t const _writeBytes;

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

  TSVIO outputVIO() const { return _outVConnU ? static_cast<TSVIO>(*_outVConnU) : nullptr; }
  TSIOBuffer outputBuffer() const { return _outVConnU ? static_cast<TSIOBuffer>(*_outVConnU) : _outBufferU.get(); }
  TSIOBufferReader outputReader() const { return _outVConnU ? static_cast<TSIOBufferReader>(*_outVConnU) : _outReaderU.get(); }

  int64_t outputLen() const { return _outWriteBytes; }
  int64_t outputOffset() const { return _outSkipBytes; }

  std::pair<int64_t,int64_t> xformInputAvail() const { return vio_range_avail(xformInputVIO()); }
  std::pair<int64_t,int64_t> inputAvail() const { return vio_range_avail(inputVIO()); }
  std::pair<int64_t,int64_t> outputAvail() const { return vio_range_avail(outputVIO()); }
  std::pair<int64_t,int64_t> bufferAvail() const 
    {
      int64_t ndone = TSVIONDoneGet(inputVIO());
      if ( ! outputVIO() ) {
        return std::make_pair( ndone, ndone );
      }
      auto r = vio_range_avail(outputVIO());
      r.first += _outSkipBytes; // offset + or -
      r.second += _outSkipBytes; // offset + or -
      return std::move(r);
    }

  void reset_input_vio(TSVIO vio);
  void reset_output_length(int64_t len);

  void
  set_body_handler(const XformCB_t &fxn)
  {
    _xformCB = fxn;
  }

  void init_enabled_transform();

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
  TSHttpTxn _txn;
  XformCB_t _xformCB;
  int64_t _outSkipBytes;
  int64_t _outWriteBytes;

  TSHttpHookID _transformHook; // needs init.. but may be reset!

  int _inLastError = 0;
  TSVIO _inVIO = nullptr;
  TSEvent _inVIOWaiting = TS_EVENT_NONE;

  ATSXformOutVConn::Uniq_t _outVConnU;
  TSIOBuffer_t             _outBufferU;
  TSIOBufferReader_t       _outReaderU;

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

// accepts TSHttpTxn handler functions
template <class T_OBJ, typename T_DATA>
ATSCont::ATSCont(T_OBJ &obj, void (T_OBJ::*funcp)(int, TSEvent, void *, const T_DATA&), T_DATA cbdata, TSCont mutexSrc)
  : TSCont_t(TSVConnCreate(&ATSCont::handleTSEventCB, ( mutexSrc ? TSContMutexGet(mutexSrc) : TSMutexCreate() )))
{
//  TSDebug("cache_range_blocks", "[%d] [%s:%d] %s(): ctor %p cont:%p",  -1, __FILE__, __LINE__, __func__, this, get());
  // point back here
  TSContDataSet(get(), this);

  static_cast<void>(cbdata);
  auto vconnp = get();

  // memorize user data to forward on
  _userCB = decltype(_userCB)([&obj, funcp, vconnp, cbdata](TSEvent event, void *evtdata) {
    (obj.*funcp)(TSVConnClosedGet(vconnp), event, evtdata, cbdata); 
  });

  // deletion from obj's scope .. [helped by Cont-data nullptr]
}


//}
