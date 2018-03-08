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
#include <deque>
#include <future>
#include <atomic>

#pragma once

#define PLUGIN_NAME "cache_range_blocks"

#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)

#define PLUGIN_NAME "cache_range_blocks"
#define DEBUG_LOG(fmt, ...) TSDebug(PLUGIN_NAME, "[%d] [%s:%d] %s(): " fmt,  ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)
#define ERROR_LOG(fmt, ...) TSError("[%d] [%s:%d] %s(): " fmt, ThreadTxnID::get(), __FILENAME__, __LINE__, __func__, ##__VA_ARGS__)

#define CONTENT_ENCODING_INTERNAL "x-block-cache-range"

#define MIN_BLOCK_STORED 8192

using namespace atscppapi;

using KeyRange_t = std::deque<ATSCacheKey>;
using KeyRange_i = typename std::deque<ATSCacheKey>::const_iterator;

class BlockBufferStore;
class BlockReadTform;

class BlockSetAccess : public TransactionPlugin
{
  friend BlockBufferStore; // when it needs to change over
  friend BlockReadTform;  // when it needs to change over
  using Txn_t = Transaction;
public:
  static void start_if_range_present(Transaction &txn);

public:
  explicit BlockSetAccess(Transaction &txn);
  ~BlockSetAccess() override;

  Transaction & txn() const { return _txn; }
  TSHttpTxn atsTxn() const { return _atsTxn; }

  const KeyRange_t & keysInRange() const { return _keysInRange; }
  int64_t assetLen() const { return _assetLen; }
  int64_t contentLen() const { return _endByte - _beginByte; }
  // assume index of first of *read* blocks...
  int64_t firstIndex() const { return ( _firstInd >= 0 ? _firstInd : _beginByte / _blkSize ); }
  // assume index of ending of *read* blocks...
  int64_t endIndex() const { return ( _endInd > 0 ? _endInd : (_endByte + _blkSize-1) / _blkSize ); }
  int64_t indexCnt() const { return endIndex() - firstIndex(); }
  int64_t blockSize() const { return _blkSize; }
  
  KeyRange_t & keysInRange() { return _keysInRange; }

  void clean_client_request(); // allow secondary-accepting block-set match
  
  // clean up, increase range-request, avoid any 304/200 if client didn't request one
  void clean_server_request(Transaction &txn);

  void prepare_cached_stub(Transaction &txn);
  void clean_client_response(Transaction &txn);

  void cache_blk_hits(int count, int failed);

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
  Headers *get_stub_hdrs();

  void new_asset_body(Transaction &txn);

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

  int64_t _srvrBeginByte = -1L;
  int64_t _srvrEndByte   = -1L;

  int64_t _firstInd = -1L;
  int64_t _endInd   = -1L;

  // objs w/destructors

  ATSCont _mutexOnlyCont;

  KeyRange_t _keysInRange;   // in order with index

  // delayed creation: transform objects must be committed to, only upon response

  std::shared_ptr<BlockReadTform>   _tform;   // state-object ptr [registers Transforms/Continuations]
  std::shared_ptr<BlockBufferStore> _storeTform; // state-object ptr [registers Transforms/Continuations]
};



/////////////////////////////////////////////////
class BlockBufferStore : public std::enable_shared_from_this<BlockBufferStore>
{
  friend struct BlockWriteInfo;

public:
  using use_p = std::shared_ptr<BlockBufferStore>;

  BlockBufferStore(BufferVConn &&buffer, int64_t blkSize, KeyRange_i bkey, KeyRange_i ekey, int lastblkid);
  ~BlockBufferStore();

  long write_count() const { return this->_writeCheck.use_count() - 1; } 
private:

  TSCacheKey next_valid_vconn(int64_t buffpos, int64_t endbuf, int64_t &skipDist);

private:
  int                      _txnid = ThreadTxnID::get(); // for debug
  BufferVConn              _inputBuffer;
  std::shared_ptr<void>    _writeCheck{&this->_writeCheck, [](std::shared_ptr<void>*){ }};
  KeyRange_i               _keyit;    // keys cleared if read / unwritable ...
  const KeyRange_i         _ekeyit;    // keys cleared if read / unwritable ...
  const int                _lastid;
  const int64_t            _blkSize;   // local copy..
};



/////////////////////////////////////////////////
struct BlockWriteInfo : public std::enable_shared_from_this<BlockWriteInfo>,
                        public ProxyVConn
{
  using use_p = std::shared_ptr<BlockWriteInfo>;

  // move key into store
  BlockWriteInfo(BlockBufferStore &store, ATSCacheKey &&key, int blk);
  ~BlockWriteInfo();

  long ref_count() const { return this->_writeTform.use_count(); } 

  BlockBufferStore::use_p _writeTform;
  std::shared_ptr<void>  _writeRef{ this->_writeTform->_writeCheck };
  ATSCacheKey            _key;
  const int              _ind;
  const int              _blkid; // for debug
  const int              _txnid = ThreadTxnID::get(); // for debug
};


/////////////////////////////////////////////////
/////////////////////////////////////////////////
class BlockReadTform : public ATSTformVConn
{
  using use_p = std::shared_ptr<BlockReadTform>;
//  template <typename _Tp, typename... _Args>
//  friend unique_ptr<_Tp> std::make_unique(_Args &&... __args); // when it needs to change over

public:
  static use_p            try_cache_hit(BlockSetAccess &, atscppapi::Transaction &txn, int64_t offset);


  BlockReadTform(BlockSetAccess &ctxt, atscppapi::Transaction &txn, int skip);
  ~BlockReadTform() override;

  void add_block_reads(); // enqueue more keys (again) if possible...
  BlockBufferStore::use_p full_cache_miss();

protected:
//  void on_output_setup() override; // ready for writes
  void on_output_empty() override; // more data needed...
  void on_input_sync() override; // sync-point reached

private:
  void read_block_tests(void *ptr);

  BlockSetAccess         &_ctxt;
  std::deque<ProxyVConn>  _cacheVIOs; // indexed with _keyit as base

  KeyRange_i        _keyit{ this->_ctxt.keysInRange().begin() }; // keys cleared if read / unwritable ...
  const KeyRange_i  _ekeyit{ this->_ctxt.keysInRange().end() };    // past-final to use...
  const int         _lastid = this->_ctxt.keysInRange().size(); 
  const int64_t     _blkSize{ this->_ctxt.blockSize() };   // local copy..
};

//}
