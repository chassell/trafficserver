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
#include "util_types.h"

#include "ts/ink_memory.h"
#include "ts/ts.h"

#include <atscppapi/Url.h>
#include <atscppapi/Transaction.h>
#include <atscppapi/TransactionPlugin.h>
#include <atscppapi/TransformationPlugin.h>
#include <atscppapi/GlobalPlugin.h>
#include <atscppapi/PluginInit.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <future>

#pragma once

#define PLUGIN_NAME "cache_range_blocks"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

class ThreadTxnID
{
  static thread_local int g_pluginTxnID;

public:
  static int get() { 
    return ( // atscppapi::TransactionPlugin::getTxnID() >= 0 
             //    ? atscppapi::TransactionPlugin::getTxnID() : 
                g_pluginTxnID );
  }

  ThreadTxnID(atscppapi::Transaction &txn) {
    g_pluginTxnID = TSHttpTxnIdGet(static_cast<TSHttpTxn>(txn.getAtsHandle()));
  }
  ThreadTxnID(TSHttpTxn txn) {
    g_pluginTxnID = TSHttpTxnIdGet(txn);
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

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%d] [%s:%d] %s(): " fmt,  ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%d] [%s:%d] %s(): " fmt, ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define MIN_BLOCK_STORED 8192

using namespace atscppapi;

class BlockStoreXform;
class BlockReadXform;

class BlockSetAccess : public TransactionPlugin
{
  friend BlockStoreXform; // when it needs to change over
  friend BlockReadXform;  // when it needs to change over
  using Txn_t = Transaction;
public:
  static void start_if_range_present(Transaction &txn);

public:
  explicit BlockSetAccess(Transaction &txn);
  ~BlockSetAccess() override;

  Transaction & txn() const { return _txn; }
  TSHttpTxn atsTxn() const { return _atsTxn; }
  Headers & clientHdrs() { return _clntHdrs; }
  const std::string & clientUrl() const { return _url; }
  const std::string & clientRangeStr() const { return _clntRangeStr; }
  const std::string & blockRangeStr() const { return _blkRangeStr; }

  const std::vector<ATSCacheKey> & keysInRange() const { return _keysInRange; }
  std::vector<ATSCacheKey> & keysInRange() { return _keysInRange; }

  int64_t assetLen() const { return _assetLen; }
  int64_t contentLen() const { return _endByte - _beginByte; }
  int64_t indexLen() const { return endIndex() - firstIndex(); }
  int64_t firstIndex() const { return _beginByte / _blkSize; }
  int64_t endIndex() const { return (_endByte + _blkSize-1) / _blkSize; }
  int64_t blockSize() const { return _blkSize; }
  
  void clean_client_request(); // allow secondary-accepting block-set match
  
  // clean up, increase range-request, avoid any 304/200 if client didn't request one
  void clean_server_request(Transaction &txn);

  void prepare_cached_stub(Transaction &txn);
  void clean_client_response(Transaction &txn);

public:
  // permit use of blocks if possible
  void handleReadRequestHeadersPostRemap(Transaction &txn) override;

  // detect a block map file here
  void handleReadCacheLookupComplete(Transaction &txn) override;

  // upon a new URL init .. add these only
  void handleSendRequestHeaders(Transaction &txn) override;
  void handleReadResponseHeaders(Transaction &txn) override;

  // restore the request as before...
  void handleSendResponseHeaders(Transaction &txn) override;
private:

  void reset_range_keys();
  void launch_block_tests();
  void handle_block_tests();

private:
  Headers *get_stub_hdrs();

  int64_t select_needed_blocks();

  void start_cache_miss();
  void start_cache_hit();
  void set_cache_hit_bitset();

private:
  Transaction &_txn;
  const TSHttpTxn _atsTxn = nullptr;
  std::string _url;
  Headers &_clntHdrs;
  std::string _clntRangeStr;
  std::string _blkRangeStr; // from clnt req for serv req

  int64_t _assetLen = 0L;    // if cached and found
  std::string _etagStr;      // if cached and found
  int64_t _blkSize  = ( 1L << 20 ); // fixed

  int64_t _beginByte = -1L;
  int64_t _endByte   = -1L;

  // objs w/destructors

  ATSCont _mutexOnlyCont;

  std::vector<ATSCacheKey> _keysInRange;               // in order with index
  std::vector<ATSVConnFuture> _vcsToRead; // indexed as the keys

  // delayed creation: transform objects must be committed to, only upon response

  std::unique_ptr<BlockReadXform> _readXform;   // state-object ptr [registers Transforms/Continuations]
  std::shared_ptr<BlockStoreXform> _storeXform; // state-object ptr [registers Transforms/Continuations]
};

/////////////////////////////////////////////////
class BlockStoreXform : public std::enable_shared_from_this<BlockStoreXform>,
                        public BlockTeeXform
{
  friend struct BlockWriteInfo;
public:
  using Ptr_t = std::shared_ptr<BlockStoreXform>;
  using WriteVCs_t = std::vector<ATSVConnFuture>;
  using WriteVCsPtr_t = std::shared_ptr<std::vector<ATSVConnFuture>>;

public:
  BlockStoreXform(BlockSetAccess &ctxt, int blockCount);
  ~BlockStoreXform() override;

  long write_count() const { return this->_writeCheck.use_count() - 1; } 
  void reset_write_keys() {
    _keysToWrite.clear();
    std::swap(_ctxt._keysInRange,_keysToWrite);
  }
private:
  TSCacheKey next_valid_vconn(int64_t pos, int64_t len, int64_t &skipDist);

  void handleBodyRead(TSIOBufferReader r, int64_t pos, int64_t len, int64_t added);

private:
  BlockSetAccess          &_ctxt;
  std::vector<ATSCacheKey> _keysToWrite; // in order with index
  std::shared_ptr<void>    _writeCheck{&this->_writeCheck, [](std::shared_ptr<void>*){ }};
  ATSCont                  _wakeupCont{_ctxt._mutexOnlyCont.get()}; // no handler at first
  std::atomic<TSEvent>     _blockVIOUntil{TS_EVENT_NONE}; // event targeted to fix block
};

/////////////////////////////////////////////////
struct BlockWriteInfo : public std::enable_shared_from_this<BlockWriteInfo>
{
  using Ptr_t = std::shared_ptr<BlockWriteInfo>;

  BlockWriteInfo(BlockStoreXform &store, TSIOBuffer buff, TSIOBufferReader rdr, int blk);
  ~BlockWriteInfo();

  static TSEvent handleBlockWriteCB(TSCont c, TSEvent evt, void *p, const std::shared_ptr<BlockWriteInfo> &ptr) {
    return ptr->handleBlockWrite(c,evt,p);
  }

  TSEvent handleBlockWrite(TSCont, TSEvent, void *);

  BlockStoreXform::Ptr_t _writeXform;
  int                    _ind;
  ATSCacheKey            _key;
  ATSVConnFuture         _vconn;
  TSIOBuffer_t           _buff;
  TSIOBufferReader_t     _rdr;
  std::shared_ptr<void>  _writeRef;

  int                    _blkid; // for debug
  int                    _txnid = ThreadTxnID::get(); // for debug
};


/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockReadXform : public ATSXformCont
{
  template <typename _Tp, typename... _Args>
  friend unique_ptr<_Tp> std::make_unique(_Args &&... __args); // when it needs to change over

public:
  ~BlockReadXform() override;

private:
  BlockReadXform(BlockSetAccess &ctxt, int64_t skip);

  void launch_block_reads();
};

//}
