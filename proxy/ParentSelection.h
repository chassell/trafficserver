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

/*****************************************************************************
 *
 *  ParentSelection.h - Interface to Parent Selection System
 *
 *
 ****************************************************************************/

#ifndef _PARENT_SELECTION_H_
#define _PARENT_SELECTION_H_

#include "Main.h"
#include "ProxyConfig.h"
#include "ControlBase.h"
#include "ControlMatcher.h"
#include "ink_apidefs.h"
#include "P_RecProcess.h"

#define MAX_PARENTS 64

struct RequestData;
struct matcher_line;
struct ParentResult;
class ParentRecord;
class ParentSelectionBase;

enum ParentResultType {
  PARENT_UNDEFINED,
  PARENT_DIRECT,
  PARENT_SPECIFIED,
  PARENT_AGENT,
  PARENT_FAIL,
  PARENT_ORIGIN,
};

enum ParentRR_t {
  P_NO_ROUND_ROBIN = 0,
  P_STRICT_ROUND_ROBIN,
  P_HASH_ROUND_ROBIN,
  P_CONSISTENT_HASH,
};

// struct pRecord
//
//    A record for an invidual parent
//
struct pRecord : ATSConsistentHashNode {
  char hostname[MAXDNAME + 1];
  int port;
  time_t failedAt;
  int failCount;
  int32_t upAt;
  const char *scheme; // for which parent matches (if any)
  int idx;
  float weight;
};

typedef ControlMatcher<ParentRecord, ParentResult> P_table;

// class ParentRecord : public ControlBase
//
//   A record for a configuration line in the parent.config
//    file
//
class ParentRecord : public ControlBase
{
public:
  ParentRecord()
    : parents(NULL), secondary_parents(NULL), num_parents(0), num_secondary_parents(0), 
      round_robin(P_NO_ROUND_ROBIN), rr_next(0), ignore_query(false), go_direct(true), 
      parent_is_proxy(true), lookup_strategy(NULL)
  {
  }

  ~ParentRecord();

  config_parse_error Init(matcher_line *line_info);
  bool DefaultInit(char *val);
  void UpdateMatch(ParentResult *result, RequestData *rdata);
  void Print();
  pRecord *parents;
  pRecord *secondary_parents;
  int num_parents;
  int num_secondary_parents;

  bool
  bypass_ok() const
  {
    return go_direct;
  }
  bool
  isParentProxy() const
  {
    return parent_is_proxy;
  }

  const char *scheme;
  // private:
  const char *ProcessParents(char *val, bool isPrimary);
  ParentRR_t round_robin;
  bool ignore_query;
  volatile uint32_t rr_next;
  bool go_direct;
  bool parent_is_proxy;
  ParentSelectionBase *lookup_strategy;
};

// If the parent was set by the external customer api,
//   our HttpRequestData structure told us what parent to
//   use and we are only called to preserve clean interface
//   between HttpTransact & the parent selection code.  The following
ParentRecord *const extApiRecord = (ParentRecord *)0xeeeeffff;

struct ParentResult {
  ParentResult()
    : r(PARENT_UNDEFINED), hostname(NULL), port(0), retry(false), line_number(0), epoch(NULL), rec(NULL), last_parent(0),
      start_parent(0), wrap_around(false), last_lookup(0)
  {
  }

  // For outside consumption
  ParentResultType r;
  const char *hostname;
  int port;
  bool retry;

  // Internal use only
  //   Not to be modified by HTTP
  int line_number;
  P_table *epoch; // A pointer to the table used.
  ParentRecord *rec;
  uint32_t last_parent;
  uint32_t start_parent;
  bool wrap_around;
  int last_lookup; // state for for consistent hash.
};

//
// API definition.
class ParentSelectionInterface
{
public:
  // bool apiParentExists(HttpRequestData* rdata)
  //
  //   Retures true if a parent has been set through the api
  virtual bool apiParentExists(HttpRequestData *rdata) = 0;

  // void findParent(RequestData* rdata, ParentResult* result)
  //
  //   Does initial parent lookup
  //
  virtual void findParent(HttpRequestData *rdata, ParentResult *result) = 0;

  // void markParentDown(ParentResult* rsult)
  //
  //    Marks the parent pointed to by result as down
  //
  virtual void markParentDown(ParentResult *result) = 0;

  // void nextParent(RequestData* rdata, ParentResult* result);
  //
  //    Marks the parent pointed to by result as down and attempts
  //      to find the next parent
  //
  virtual void nextParent(HttpRequestData *rdata, ParentResult *result) = 0;

  // uint32_t numParents(ParentResult *result);
  //
  // Returns the number of parent records in a strategy.
  //
  virtual uint32_t numParents(ParentResult *result) = 0;

  // bool parentExists(HttpRequestData* rdata)
  //
  //   Returns true if there is a parent matching the request data and
  //   false otherwise
  virtual bool parentExists(HttpRequestData *rdata) = 0;

  // void recordRetrySuccess
  //
  //    After a successful retry, http calls this function
  //      to clear the bits indicating the parent is down
  //
  virtual void recordRetrySuccess(ParentResult *result) = 0;
};

//
// Implements common functionality of the ParentSelectionInterface.
//
class ParentSelectionBase : public ParentSelectionInterface
{
public:
  ParentRecord *parent_record;
  P_table *parent_table;
  ParentRecord *DefaultParent;
  int32_t ParentRetryTime;
  int32_t ParentEnable;
  int32_t FailThreshold;
  int32_t DNS_ParentOnly;

  ParentSelectionBase(P_table *_parent_table) { parent_table = _parent_table; }
  ParentSelectionBase();
  void
  setParentTable(P_table *_parent_table)
  {
    parent_table = _parent_table;
  }
  bool apiParentExists(HttpRequestData *rdata);
  void findParent(HttpRequestData *rdata, ParentResult *result);
  void nextParent(HttpRequestData *rdata, ParentResult *result);
  bool parentExists(HttpRequestData *rdata);
  virtual void lookupParent(bool firstCall, ParentResult *result, RequestData *rdata) = 0;
};

class ParentSelectionStrategy : public ParentSelectionBase, public ConfigInfo
{
public:
  ParentSelectionStrategy(P_table *_parent_table) { parent_table = _parent_table; }
  ~ParentSelectionStrategy(){};

  void
  lookupParent(bool firstCall, ParentResult *result, RequestData *rdata)
  {
    ink_release_assert(result->rec->lookup_strategy != NULL);
    return result->rec->lookup_strategy->lookupParent(firstCall, result, rdata);
  }

  void
  markParentDown(ParentResult *result)
  {
    ink_release_assert(result->rec->lookup_strategy != NULL);
    result->rec->lookup_strategy->markParentDown(result);
  }

  void
  nextParent(HttpRequestData *rdata, ParentResult *result)
  {
    ink_release_assert(result->rec->lookup_strategy != NULL);
    result->rec->lookup_strategy->nextParent(rdata, result);
  }

  uint32_t
  numParents(ParentResult *result)
  {
    ink_release_assert(result->rec->lookup_strategy != NULL);
    return result->rec->lookup_strategy->numParents(result);
  }

  void
  recordRetrySuccess(ParentResult *result)
  {
    ink_release_assert(result != NULL);
    result->rec->lookup_strategy->recordRetrySuccess(result);
  }
};

class HttpRequestData;

struct ParentConfig {
public:
  static void startup();
  static void reconfigure();
  static void print();
  static void set_parent_table(P_table *pTable, ParentRecord *rec, int num_elements);

  static ParentSelectionStrategy *
  acquire()
  {
    return (ParentSelectionStrategy *)configProcessor.get(ParentConfig::m_id);
  }

  static void
  release(ParentSelectionStrategy *strategy)
  {
    configProcessor.release(ParentConfig::m_id, strategy);
  }

  static int m_id;
};

// Helper Functions
ParentRecord *createDefaultParent(char *val);
void reloadDefaultParent(char *val);
void reloadParentFile();
int parentSelection_CB(const char *name, RecDataT data_type, RecData data, void *cookie);

// Unit Test Functions
void show_result(ParentResult *aParentResult);
void br(HttpRequestData *h, const char *os_hostname, sockaddr const *dest_ip = NULL); // short for build request
int verify(ParentResult *r, ParentResultType e, const char *h, int p);

/*
  For supporting multiple Socks servers, we essentially use the
  ParentSelection infrastructure. Only the initialization is different.
  If needed, we will have to implement most of the functions in
  ParentSection.cc for Socks as well. For right now we will just use
  ParentSelection

  All the members in ParentConfig are static. Right now
  we will duplicate the code for these static functions.
*/
struct SocksServerConfig {
  static void startup();
  static void reconfigure();
  static void print();

  static ParentSelectionStrategy *
  acquire()
  {
    return (ParentSelectionStrategy *)configProcessor.get(SocksServerConfig::m_id);
  }
  static void
  release(ParentSelectionStrategy *params)
  {
    configProcessor.release(SocksServerConfig::m_id, params);
  }

  static int m_id;
};

#endif
