/** @file

  A brief file description

  @section license License

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

#include "ts/ink_defs.h"
#include "ts/ink_platform.h"
#include "ts/ts.h"

#include "atscppapi/GlobalPlugin.h"
#include "atscppapi/AsyncTimer.h"
#include "atscppapi/PluginInit.h"

#include <unistd.h>
#include <iostream>
#include <map>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <string>

#include <memory>

#include <stdbool.h>
#include <getopt.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>


#ifdef HAVE_PCRE_PCRE_H
#include <pcre/pcre.h>
#else
#include <pcre.h>
#endif

#define LOG_PREFIX "cache_inv_scan"
#define CONFIG_TMOUT 60000
#define FREE_TMOUT 300000
#define OVECTOR_SIZE 30
#define LOG_ROLL_INTERVAL 86400
#define LOG_ROLL_OFFSET 0

using namespace atscppapi;
using std::string;  

template <class T_CONTOBJ>
struct TSContObj 
{
 public:
  TSContObj(TSCont cb) 
    : _heldCb(cb)
  {
  }

  TSContObj(TSEventFunc fxn, T_CONTOBJ *obj)
    : _heldCb( TSContCreate(fxn,TSMutexCreate()) )
  {
    TSContDataSet(_heldCb, obj);
  }

  ~TSContObj() { TSContDestroy(_heldCb); }

  T_CONTOBJ &operator *() 
     { return *static_cast<T_CONTOBJ*>(TSContDataGet(_heldCb)); }
  T_CONTOBJ *operator ->() 
     { return static_cast<T_CONTOBJ*>(TSContDataGet(_heldCb)); }
  operator TSCont() { return _heldCb; }

 private:
  const TSCont _heldCb;
};

class PluginState : public AsyncReceiver<AsyncTimer>
{
  public:
     PluginState(int argc, const char *argv[])
     {
       Async::execute<AsyncTimer>(this, &_cfgCheck, std::shared_ptr<Mutex>());
     }

     static int 
     init_scan(TSCont cont, TSEvent, void *)
     { 
       PluginState &cfg = *static_cast<PluginState*>(TSContDataGet(cont));
       TSContDestroy(cont);
       cfg.configChanged(); 
     }

     static int 
     handleScanEvent(TSCont cont, TSEvent event, void *arg);

     void 
     handleAsyncComplete(AsyncTimer &timer) override // config timer
       { configChanged(); }

  private:
     void configChanged();
     void objFound(TSCacheHttpInfo info);

  private:
     AsyncTimer  _cfgCheck{AsyncTimer::TYPE_PERIODIC, CONFIG_TMOUT};
};

class ScanConfig : public AsyncReceiver<PluginState>
{
  public:
   ScanConfig(int argc, const char *argv[])
   {
   }

   TSCont  _scanCb SContCreate(&ScanConfig::handleScanEvent, TSMutexCreate());
   void 
   handleAsyncComplete(PluginState &timer) override; // cancel of scan
};

void PluginState::configChanged()
{
   auto cb = 
   TSCacheScan(cb, nullptr, 512000);
}

int 
ScanConfig::handleScanEvent(TSCont cont, TSEvent event, void *arg)
{
  int r;
  ScanConfig &cfg = *static_cast<ScanConfig*>(TSContDataGet(cont));
  switch ( event ) {
    case TS_EVENT_CACHE_SCAN:
    {
        TSVConn conn = static_cast<TSVConn>(arg); // VConn from a CacheVC
    }
    case TS_EVENT_CACHE_SCAN_OBJECT:
        cfg.objFound(static_cast<TSCacheHttpInfo>(arg));
        break;
    case TS_EVENT_CACHE_SCAN_DONE:
        r = reinterpret_cast<intptr_t>(arg);
        return EVENT_DONE;
    case TS_EVENT_CACHE_SCAN_FAILED:
        return EVENT_DONE;
    case TS_EVENT_CACHE_SCAN_OPERATION_BLOCKED:
    case TS_EVENT_CACHE_SCAN_OPERATION_FAILED:
      break;
  }
}

//    pcre *preq = pcre_compile(patt, 0, &error, &erroffset, nullptr);

void
ScanConfig::objFound(TSCacheHttpInfo info)
{
  
  switch (event) {
  case TS_CACHE_EVENT_SCAN: {
    cache_vc = (CacheVC *)e;
    return EVENT_CONT;
  }

  case TS_CACHE_EVENT_SCAN_OBJECT: {
    HTTPInfo *alt = (HTTPInfo *)e;
    char xx[501], m[501];
    int ib = 0, xd = 0, ml = 0;

    alt->request_get()->url_print(xx, 500, &ib, &xd);
    xx[ib] = '\0';

    const char *mm = alt->request_get()->method_get(&ml);

    memcpy(m, mm, ml);
    m[ml] = 0;

    int res = CACHE_SCAN_RESULT_CONTINUE;
    // res = CACHE_SCAN_RESULT_DELETE;
    // res = CACHE_SCAN_RESULT_DELETE_ALL_ALTERNATES:
    // res = CACHE_SCAN_RESULT_UPDATE;
    // res = EVENT_DONE

    for ( ... all patterns with pcre ... )
    {
      const char *error;
      int erroffset;

      int r = pcre_exec(preq, nullptr, xx, ib, 0, 0, nullptr, 0);

      if (r == -1) {
        continue;
      }

      HTTPInfo new_info;
      res = CACHE_SCAN_RESULT_UPDATE;
      new_info.copy(alt);
      new_info.response_get()->set_cooked_cc_need_revalidate_once();
      cache_vc->set_http_info(&new_info);
      break;
    }
    return res;
  }

  case TS_CACHE_EVENT_SCAN_DONE:
  }
}

void TSPluginInit(int argc, const char *argv[])
{
  auto cb = TSContCreate(&PluginState::init_scan, TSMutexCreate());
  TSContDataSet(cb, new PluginState(argc,argv));
  TSLifecycleHookAdd(TS_LIFECYCLE_CACHE_READY_HOOK, cb);
}
