/** @file

  Implements RFC 5861 (HTTP Cache-Control Extensions for Stale Content)

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

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <search.h>
#include <getopt.h>
#include <arpa/inet.h>

#include "ts/ink_defs.h"
#include "ts/ts.h"
#include "ts/experimental.h"

#define PLUGIN_NAME "stale_while_revalidate"

static const char HTTP_VALUE_STALE_WHILE_REVALIDATE[] = "stale-while-revalidate";
static const char HTTP_VALUE_STALE_IF_ERROR[]         = "stale-if-error";
static const char HTTP_VALUE_STALE_WARNING[]          = "110 Response is stale";

typedef struct {
  TSTextLogObject object;
  bool all, stale_if_error, stale_while_revalidate;
  char *filename;
} log_info_t;

typedef struct {
  void *troot;
  TSMutex troot_mutex;
  int txn_slot;
  time_t stale_if_error_override;
  log_info_t log_info;
} config_t;

typedef struct {
  time_t date, stale_while_revalidate, stale_on_error, max_age;
} CachedHeaderInfo;

typedef struct {
  char *effective_url;
  TSMBuffer buf;
  TSMLoc http_hdr_loc;
  struct sockaddr *client_addr;
} RequestInfo;

typedef struct {
  TSMBuffer buf;
  TSMLoc http_hdr_loc;
  TSHttpParser parser;
  bool parsed;
  TSHttpStatus status;
} ResponseInfo;

typedef struct {
  TSHttpTxn txn;
  TSCont main_cont;
  bool async_req;
  TSIOBuffer req_io_buf, resp_io_buf;
  TSIOBufferReader req_io_buf_reader, resp_io_buf_reader;
  TSVIO r_vio, w_vio;
  TSVConn vconn;
  RequestInfo *req_info;
  ResponseInfo *resp_info;
  time_t txn_start;
  config_t *plugin_config;
} StateInfo;

static ResponseInfo *
create_response_info(void)
{
  ResponseInfo *resp_info;

  resp_info = (ResponseInfo *)TSmalloc(sizeof(ResponseInfo));

  resp_info->buf          = TSMBufferCreate();
  resp_info->http_hdr_loc = TSHttpHdrCreate(resp_info->buf);
  resp_info->parser       = TSHttpParserCreate();
  resp_info->parsed       = false;

  return resp_info;
}

static void
free_response_info(ResponseInfo *resp_info)
{
  TSHandleMLocRelease(resp_info->buf, TS_NULL_MLOC, resp_info->http_hdr_loc);
  TSMBufferDestroy(resp_info->buf);
  TSHttpParserDestroy(resp_info->parser);
  TSfree(resp_info);
}

static RequestInfo *
create_request_info(TSHttpTxn txn)
{
  RequestInfo *req_info;
  char *url;
  int url_len;
  TSMBuffer buf;
  TSMLoc loc;

  req_info = (RequestInfo *)TSmalloc(sizeof(RequestInfo));

  url                     = TSHttpTxnEffectiveUrlStringGet(txn, &url_len);
  req_info->effective_url = TSstrndup(url, url_len);
  TSfree(url);

  TSHttpTxnClientReqGet(txn, &buf, &loc);
  req_info->buf = TSMBufferCreate();
  TSHttpHdrClone(req_info->buf, buf, loc, &(req_info->http_hdr_loc));
  TSHandleMLocRelease(buf, TS_NULL_MLOC, loc);

  req_info->client_addr = TSmalloc(sizeof(struct sockaddr));
  memmove((void *)req_info->client_addr, (void *)TSHttpTxnClientAddrGet(txn), sizeof(struct sockaddr));

  return req_info;
}

static void
free_request_info(RequestInfo *req_info)
{
  TSfree(req_info->effective_url);
  TSHandleMLocRelease(req_info->buf, TS_NULL_MLOC, req_info->http_hdr_loc);
  TSMBufferDestroy(req_info->buf);
  TSfree(req_info->client_addr);
  TSfree(req_info);
}

static void
free_state_info(StateInfo *state)
{
  if ( state->req_info ) free_request_info(state->req_info);
  if ( state->resp_info ) free_response_info(state->resp_info);

  if ( state->req_io_buf_reader ) TSIOBufferReaderFree(state->req_io_buf_reader);
  if ( state->req_io_buf ) TSIOBufferDestroy(state->req_io_buf);
  if ( state->resp_io_buf_reader ) TSIOBufferReaderFree(state->resp_io_buf_reader);
  if ( state->resp_io_buf ) TSIOBufferDestroy(state->resp_io_buf);
  TSfree(state);
}

static CachedHeaderInfo *
get_cached_header_info(TSHttpTxn txn)
{
  CachedHeaderInfo *chi;
  TSMBuffer cr_buf;
  TSMLoc cr_hdr_loc, cr_date_loc, cr_cache_control_loc, cr_cache_control_dup_loc;
  int cr_cache_control_count, val_len, i;
  char *value, *ptr;

  chi                         = (CachedHeaderInfo *)TSmalloc(sizeof(CachedHeaderInfo));
  chi->date                   = 0;
  chi->max_age                = 0;
  chi->stale_while_revalidate = 0;
  chi->stale_on_error         = 0;

  if (TSHttpTxnCachedRespGet(txn, &cr_buf, &cr_hdr_loc) == TS_SUCCESS) {
    cr_date_loc = TSMimeHdrFieldFind(cr_buf, cr_hdr_loc, TS_MIME_FIELD_DATE, TS_MIME_LEN_DATE);
    if (cr_date_loc != TS_NULL_MLOC) {
      TSDebug(PLUGIN_NAME, "Found a date");
      chi->date = TSMimeHdrFieldValueDateGet(cr_buf, cr_hdr_loc, cr_date_loc);
      TSHandleMLocRelease(cr_buf, cr_hdr_loc, cr_date_loc);
    }

    cr_cache_control_loc = TSMimeHdrFieldFind(cr_buf, cr_hdr_loc, TS_MIME_FIELD_CACHE_CONTROL, TS_MIME_LEN_CACHE_CONTROL);

    while (cr_cache_control_loc != TS_NULL_MLOC) {
      TSDebug(PLUGIN_NAME, "Found cache-control");
      cr_cache_control_count = TSMimeHdrFieldValuesCount(cr_buf, cr_hdr_loc, cr_cache_control_loc);

      for (i = 0; i < cr_cache_control_count; i++) {
        value = (char *)TSMimeHdrFieldValueStringGet(cr_buf, cr_hdr_loc, cr_cache_control_loc, i, &val_len);
        ptr   = value;

        if (strncmp(value, TS_HTTP_VALUE_MAX_AGE, TS_HTTP_LEN_MAX_AGE) == 0) {
          TSDebug(PLUGIN_NAME, "Found max-age");
          ptr += TS_HTTP_LEN_MAX_AGE;
          if (*ptr == '=') {
            ptr++;
            chi->max_age = atol(ptr);
          } else {
            ptr = TSstrndup(value, TS_HTTP_LEN_MAX_AGE + 2);
            TSDebug(PLUGIN_NAME, "This is what I found: %s", ptr);
            TSfree(ptr);
          }
        } else if (strncmp(value, HTTP_VALUE_STALE_WHILE_REVALIDATE, strlen(HTTP_VALUE_STALE_WHILE_REVALIDATE)) == 0) {
          TSDebug(PLUGIN_NAME, "Found stale-while-revalidate");
          ptr += strlen(HTTP_VALUE_STALE_WHILE_REVALIDATE);
          if (*ptr == '=') {
            ptr++;
            chi->stale_while_revalidate = atol(ptr);
          }
        } else if (strncmp(value, HTTP_VALUE_STALE_IF_ERROR, strlen(HTTP_VALUE_STALE_IF_ERROR)) == 0) {
          TSDebug(PLUGIN_NAME, "Found stale-on-error");
          ptr += strlen(HTTP_VALUE_STALE_IF_ERROR);
          if (*ptr == '=') {
            ptr++;
            chi->stale_on_error = atol(ptr);
          }
        } else {
          TSDebug(PLUGIN_NAME, "Unknown field value");
        }
      }

      cr_cache_control_dup_loc = TSMimeHdrFieldNextDup(cr_buf, cr_hdr_loc, cr_cache_control_loc);
      TSHandleMLocRelease(cr_buf, cr_hdr_loc, cr_cache_control_loc);
      cr_cache_control_loc = cr_cache_control_dup_loc;
    }
    TSHandleMLocRelease(cr_buf, TS_NULL_MLOC, cr_hdr_loc);
  }

  return chi;
}

static int
xstrcmp(const void *a, const void *b)
{
  return strcmp((const char *)a, (const char *)b);
}

static void
parse_response(StateInfo *state)
{
  TSIOBufferBlock block;
  TSParseResult pr = TS_PARSE_CONT;
  int64_t avail;
  char *start;

  block = TSIOBufferReaderStart(state->resp_io_buf_reader);

  while ((pr == TS_PARSE_CONT) && (block != NULL)) {
    start = (char *)TSIOBufferBlockReadStart(block, state->resp_io_buf_reader, &avail);
    if (avail > 0) {
      pr = TSHttpHdrParseResp(state->resp_info->parser, state->resp_info->buf, state->resp_info->http_hdr_loc,
                              (const char **)&start, (const char *)(start + avail));
    }
    block = TSIOBufferBlockNext(block);
  }

  if (pr != TS_PARSE_CONT) {
    state->resp_info->status = TSHttpHdrStatusGet(state->resp_info->buf, state->resp_info->http_hdr_loc);
    state->resp_info->parsed = true;
    TSDebug(PLUGIN_NAME, "HTTP Status: %d", state->resp_info->status);
  }
}

static int 
consume_shutdown(TSVConn vconn, StateInfo *state);
static int
consume_resource(TSCont cont, TSEvent event ATS_UNUSED, void *edata ATS_UNUSED)
{
  StateInfo *state;
  int64_t avail;
  TSVConn vconn;

  vconn = (TSVConn)edata;
  state = (StateInfo *)TSContDataGet(cont);

  switch (event) {
  case TS_EVENT_VCONN_WRITE_READY:
    // We shouldn't get here because we specify the exact size of the buffer.
    TSDebug(PLUGIN_NAME, "Write Ready");
    break;
  case TS_EVENT_VCONN_WRITE_COMPLETE:
    TSDebug(PLUGIN_NAME, "Write Complete");
    // TSDebug(PLUGIN_NAME, "TSVConnShutdown()");
    // TSVConnShutdown(state->vconn, 0, 1);
    // TSVIOReenable(state->w_vio);
    break;
  case TS_EVENT_VCONN_READ_READY:
    TSDebug(PLUGIN_NAME, "Read Ready");

    if ((state->resp_info) && !state->resp_info->parsed) {
      parse_response(state);
    }

    // Consume data
    avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);
    TSIOBufferReaderConsume(state->resp_io_buf_reader, avail);
    TSVIONDoneSet(state->r_vio, TSVIONDoneGet(state->r_vio) + avail);
    TSVIOReenable(state->r_vio);
    break;

  case TS_EVENT_VCONN_READ_COMPLETE:
    TSDebug(PLUGIN_NAME, "Read Complete");
    TSVConnClose(state->vconn);
    consume_shutdown(vconn,state);
    TSContDestroy(cont);
    break;
  case TS_EVENT_VCONN_EOS:
    TSDebug(PLUGIN_NAME, "EOS");
    TSVConnClose(state->vconn);
    consume_shutdown(vconn,state);
    TSContDestroy(cont);
    break;
  case TS_EVENT_VCONN_INACTIVITY_TIMEOUT:
    TSDebug(PLUGIN_NAME, "Inactivity Timeout");
    TSVConnAbort(vconn, TS_VC_CLOSE_ABORT);
    consume_shutdown(vconn,state);
    TSContDestroy(cont);
    break;

  default:
    TSError("[stale_while_revalidate] Unknown event %d.", event);
    break;
  }

  return 0;
}


static int 
consume_shutdown(TSVConn vconn, StateInfo *state)
{
  if ((state->resp_info) && !state->resp_info->parsed) {
    parse_response(state);
  }

  // Consume data
  int64_t avail = TSIOBufferReaderAvail(state->resp_io_buf_reader);

  TSIOBufferReaderConsume(state->resp_io_buf_reader, avail);
  TSVIONDoneSet(state->r_vio, TSVIONDoneGet(state->r_vio) + avail);
  if (state->async_req) 
  {
    TSDebug(PLUGIN_NAME, "Unlock URL");
    TSMutexLock(state->plugin_config->troot_mutex);
    tdelete(state->req_info->effective_url, &(state->plugin_config->troot), xstrcmp);
    TSMutexUnlock(state->plugin_config->troot_mutex);
    free_state_info(state);
    return 0;
  } 

  TSDebug(PLUGIN_NAME, "In sync path. setting fresh and re-enabling");
  TSHttpTxnCacheLookupCountGet(state->txn, &lookup_count);
  do {
    if ( ! state->resp_info ) break;
    if ( lookup_count <= 2 ) {
      if ( state->resp_info->status == 501 ) 
         break;
      if ( state->resp_info->status < 500 || state->resp_info->status > 504 ) 
         break;
    }

    TSDebug(PLUGIN_NAME, "Sending stale data as fresh");
    if (state->plugin_config->log_info.object &&
        (state->plugin_config->log_info.all || state->plugin_config->log_info.stale_if_error)) 
    {
      CachedHeaderInfo *chi = get_cached_header_info(state->txn);
      TSTextLogObjectWrite(state->plugin_config->log_info.object, "stale-if-error: %d - %d < %d + %d %s", (int)state->txn_start,
                           (int)chi->date, (int)chi->max_age, (int)chi->stale_on_error, state->req_info->effective_url);
      TSfree(chi);
    }

    TSHttpTxnHookAdd(state->txn, TS_HTTP_SEND_RESPONSE_HDR_HOOK, state->main_cont);
    TSHttpTxnCacheLookupStatusSet(state->txn, TS_CACHE_LOOKUP_HIT_FRESH);
    TSHttpTxnReenable(state->txn, TS_EVENT_HTTP_CONTINUE);

    free_state_info(state);
    return 0;
  }

  TSMLoc url_loc;
  TSDebug(PLUGIN_NAME, "Attempting new cache lookup");
  TSHttpHdrUrlGet(state->req_info->buf, state->req_info->http_hdr_loc, &url_loc);
  TSHttpTxnNewCacheLookupDo(state->txn, state->req_info->buf, url_loc);
  TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, url_loc);

  // TODO add txn translation hook and pass result along, maybe inside continuation?
  // TSHttpTxnHookAdd(state->txn, TS_HTTP_RESPONSE_TRANSFORM_HOOK, TSTransformCreate(replace_transform, state->txn));

  TSHttpTxnReenable(state->txn, TS_EVENT_HTTP_CONTINUE);
  free_state_info(state);
  return;
}


static int
fetch_resource(TSCont cont, TSEvent event ATS_UNUSED, void *edata ATS_UNUSED)
{
  StateInfo *state;
  TSCont consume_cont;
  // struct sockaddr_in client_addr;
  TSMLoc connection_hdr_loc, connection_hdr_dup_loc;

  state = (StateInfo *)TSContDataGet(cont);

  // li = (RequestInfo *) edata;
  if (state->async_req) {
    TSMutexLock(state->plugin_config->troot_mutex);
    // If already doing async lookup lets just close shop and go home
    if (tfind(state->req_info->effective_url, &(state->plugin_config->troot), xstrcmp) != NULL) {
      TSDebug(PLUGIN_NAME, "Looks like an async is already in progress");
      TSMutexUnlock(state->plugin_config->troot_mutex);
      free_state_info(state);
      state = NULL;
    }
    // Otherwise lets do the lookup!
    else {
      // Lock in tree
      TSDebug(PLUGIN_NAME, "Locking URL");
      tsearch(state->req_info->effective_url, &(state->plugin_config->troot), xstrcmp);
      TSMutexUnlock(state->plugin_config->troot_mutex);
    }
  }

  if (state) {
    TSDebug(PLUGIN_NAME, "Lets do the lookup");
    consume_cont = TSContCreate(consume_resource, TSMutexCreate());
    TSContDataSet(consume_cont, (void *)state);

    if (state->async_req) {
      state->resp_info = NULL;
    } else {
      state->resp_info = create_response_info();
    }

    TSDebug(PLUGIN_NAME, "Set Connection: close");
    connection_hdr_loc =
      TSMimeHdrFieldFind(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION);

    while (connection_hdr_loc != TS_NULL_MLOC) {
      TSDebug(PLUGIN_NAME, "Found old Connection hdr");

      connection_hdr_dup_loc = TSMimeHdrFieldNextDup(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
      TSMimeHdrFieldRemove(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
      TSMimeHdrFieldDestroy(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
      TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
      connection_hdr_loc = connection_hdr_dup_loc;
    }

    // This seems to have little effect
    TSDebug(PLUGIN_NAME, "Creating Connection hdr");
    TSMimeHdrFieldCreateNamed(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION,
                              &connection_hdr_loc);
    TSMimeHdrFieldValueStringInsert(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc, -1,
                                    TS_HTTP_VALUE_CLOSE, TS_HTTP_LEN_CLOSE);
    TSMimeHdrFieldAppend(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
    TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);

    /*
    TSDebug(PLUGIN_NAME, "Creating @stale_while_revalidate header");
    TSMimeHdrFieldCreateNamed(state->req_info->buf, state->req_info->http_hdr_loc, TS_MIME_FIELD_CONNECTION, TS_MIME_LEN_CONNECTION,
    &connection_hdr_loc);
    TSMimeHdrFieldValueStringInsert(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc, -1,
    TS_HTTP_VALUE_CLOSE, TS_HTTP_LEN_CLOSE);
    TSMimeHdrFieldAppend(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
    TSHandleMLocRelease(state->req_info->buf, state->req_info->http_hdr_loc, connection_hdr_loc);
    */

    TSDebug(PLUGIN_NAME, "Create Buffers");
    state->req_io_buf         = TSIOBufferCreate();
    state->req_io_buf_reader  = TSIOBufferReaderAlloc(state->req_io_buf);
    state->resp_io_buf        = TSIOBufferCreate();
    state->resp_io_buf_reader = TSIOBufferReaderAlloc(state->resp_io_buf);

    TSHttpHdrPrint(state->req_info->buf, state->req_info->http_hdr_loc, state->req_io_buf);
    TSIOBufferWrite(state->req_io_buf, "\r\n", 2);

    // memmove((void *) &client_addr, (void *) state->req_info->client_addr, sizeof(struct sockaddr));
    // TSDebug(PLUGIN_NAME, "client_addr: %s:%d", inet_ntoa(client_addr.sin_addr), client_addr.sin_port);
    state->vconn = TSHttpConnect((struct sockaddr const *)state->req_info->client_addr);

    state->r_vio = TSVConnRead(state->vconn, consume_cont, state->resp_io_buf, INT64_MAX);
    state->w_vio =
      TSVConnWrite(state->vconn, consume_cont, state->req_io_buf_reader, TSIOBufferReaderAvail(state->req_io_buf_reader));
  }

  TSContDestroy(cont);

  return 0;
}

static int
main_plugin(TSCont cont, TSEvent event, void *edata)
{
  TSHttpTxn txn = (TSHttpTxn)edata;

  switch (event) {
  // Is this the proper event?

  case TS_EVENT_HTTP_READ_REQUEST_HDR: {

    if (TSHttpTxnIsInternal(txn) != TS_SUCCESS) {
      TSDebug(PLUGIN_NAME, "Internal Request"); // This is insufficient if there are other plugins using TSHttpConnect
      TSHttpTxnConfigIntSet(txn, TS_CONFIG_HTTP_SERVER_SESSION_SHARING_MATCH, TS_SERVER_SESSION_SHARING_MATCH_NONE);

      TSHttpTxnHookAdd(txn, TS_HTTP_READ_RESPONSE_HDR_HOOK, cont);
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break; //////////////////////////////////////////// --> ...READ_RESPONSE_HDR @ main_plugin
    }

    TSDebug(PLUGIN_NAME, "External Request");

    config_t *plugin_config = (config_t *)TSContDataGet(cont);

    StateInfo *state = TSmalloc(sizeof(StateInfo));
    state->plugin_config = plugin_config;
    time(&state->txn_start);
    state->req_info = create_request_info(txn);
    state->resp_info = NULL;

    TSHttpTxnArgSet(txn, state->plugin_config->txn_slot, (void *)state);
    TSHttpTxnHookAdd(txn, TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, cont);
    TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
    break; //////////////////////////////////////////// --> ...LOOKUP_COMPLETE @ main_plugin
  }


  case TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE: {
    config_t *plugin_config = (config_t *)TSContDataGet(cont);
    StateInfo *state = (StateInfo *)TSHttpTxnArgGet(txn, plugin_config->txn_slot);

    int status;
    int r = TSHttpTxnCacheLookupStatusGet(txn, &status);

    // Are we not check-able or not stale?
    if ( r != TS_SUCCESS || status != TS_CACHE_LOOKUP_HIT_STALE ) 
    {
      TSDebug(PLUGIN_NAME, ( r != TS_SUCCESS ? "Could not get CacheLookupStatus"
                                             : "Not Stale!"));
      free_state_info(state);
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break; //////////////////////////////////////////// txn final (~state,HTTP_CONTINUE)
    }

    // stale
    TSDebug(PLUGIN_NAME, "CacheLookupStatus is STALE");

    // Get headers
    CachedHeaderInfo *chi = get_cached_header_info(txn);

    if (chi->stale_on_error < state->plugin_config->stale_if_error_override) {
      chi->stale_on_error = state->plugin_config->stale_if_error_override;
    }

    time_t ageOverMax = state->txn_start - (chi->date + chi->max_age);

    // handle too-old case
    if ( ageOverMax >= MAX(chi->stale_while_revalidate, chi->stale_on_error) ) 
    {
      TSDebug(PLUGIN_NAME, "No love? now: %d date: %d max-age: %d swr: %d soe: %d", (int)state->txn_start, (int)chi->date,
              (int)chi->max_age, (int)chi->stale_while_revalidate, (int)chi->stale_on_error);

      TSfree(chi);
      free_state_info(state);
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break; //////////////////////////////////////////// txn final (~state,HTTP_CONTINUE,~chi)
    }

    // handle error-only case (not background)
    if ( ageOverMax >= chi->stale_while_revalidate && ageOverMax < chi->stale_on_error )
    {
      TSDebug(PLUGIN_NAME, "Looks like we can return fresh data on 500 error");
#if (TS_VERSION_NUMBER >= 3003000)
      TSHttpTxnConfigIntSet(txn, TS_CONFIG_HTTP_INSERT_AGE_IN_RESPONSE, 1);
#endif
      // lookup sync
      state->async_req = false;
      state->txn       = txn;
      state->main_cont = cont; // we need this for the warning header callback. not sure i like it, but it works.

      TSCont fetch_cont = TSContCreate(fetch_resource, TSMutexCreate());
      TSContDataSet(fetch_cont, (void *)state);
      TSContSchedule(fetch_cont, 0, TS_THREAD_POOL_NET);

      TSfree(chi);
      // state is kept 
      // TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE); // [not called]
      break; ///////////////////////// (~chi) txn cont --> @ fetch_resource
    }
    
    TSDebug(PLUGIN_NAME, "Looks like we can return fresh info and validate in the background");
    if (state->plugin_config->log_info.object &&
        (state->plugin_config->log_info.all || state->plugin_config->log_info.stale_while_revalidate)) 
    {
      TSTextLogObjectWrite(state->plugin_config->log_info.object, "stale-while-revalidate: %d - %d < %d + %d %s",
                           (int)state->txn_start, (int)chi->date, (int)chi->max_age, (int)chi->stale_while_revalidate,
                           state->req_info->effective_url);
    }

// lookup async

#if (TS_VERSION_NUMBER >= 3003000)
    TSHttpTxnConfigIntSet(txn, TS_CONFIG_HTTP_INSERT_AGE_IN_RESPONSE, 1);
#endif

    // Set warning header
    TSHttpTxnHookAdd(txn, TS_HTTP_SEND_RESPONSE_HDR_HOOK, cont);

    TSDebug(PLUGIN_NAME, "set state as async");

    state->async_req = true;
    TSHttpTxnCacheLookupStatusSet(txn, TS_CACHE_LOOKUP_HIT_FRESH);

    TSCont fetch_cont = TSContCreate(fetch_resource, TSMutexCreate());
    TSContDataSet(fetch_cont, (void *)state);
    TSContSchedule(fetch_cont, 0, TS_THREAD_POOL_NET);
    TSfree(chi);
    // state is kept 
    TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
    ////////////////////////////////////////////  --> ...SEND_RESPONSE_HDR @ main_plugin
    break; ////////////////////// (~chi) txn cont --> @ fetch_resource
  }

  case TS_EVENT_HTTP_READ_RESPONSE_HDR: {
    TSMBuffer buf;
    TSMLoc loc;
    if (TS_SUCCESS != TSHttpTxnServerRespGet(txn, &buf, &loc)) {
      TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
      break; // end
    }

    TSHttpStatus http_status = TSHttpHdrStatusGet(buf, loc);
    if ((http_status == 500) || ((http_status >= 502) && (http_status <= 504))) // 500, 502, 503, or 504
    { 
      TSDebug(PLUGIN_NAME, "Set non-cachable");
#if (TS_VERSION_NUMBER >= 3003000)
      TSHttpTxnServerRespNoStoreSet(txn, 1);
#else
      TSHttpTxnServerRespNoStore(txn);
#endif
    }
    TSHandleMLocRelease(buf, TS_NULL_MLOC, loc);
    TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
    break; // end
  }

  case TS_EVENT_HTTP_SEND_RESPONSE_HDR: {
    TSMBuffer buf;
    TSMLoc loc, warn_loc;
    TSDebug(PLUGIN_NAME, "set warning header");
    TSHttpTxnClientRespGet(txn, &buf, &loc);
    TSMimeHdrFieldCreateNamed(buf, loc, TS_MIME_FIELD_WARNING, TS_MIME_LEN_WARNING, &warn_loc);
    TSMimeHdrFieldValueStringInsert(buf, loc, warn_loc, -1, HTTP_VALUE_STALE_WARNING, strlen(HTTP_VALUE_STALE_WARNING));
    TSMimeHdrFieldAppend(buf, loc, warn_loc);
    TSHandleMLocRelease(buf, loc, warn_loc);
    TSHandleMLocRelease(buf, TS_NULL_MLOC, loc);
    TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
    break; // end
  }
  default:
    TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
    break;
  }

  return 0;
}

void
TSPluginInit(int argc, const char *argv[])
{
  config_t *plugin_config;
  TSPluginRegistrationInfo info;
  TSCont main_cont;

  info.plugin_name   = PLUGIN_NAME;
  info.vendor_name   = "Apache Software Foundation";
  info.support_email = "dev@trafficserver.apache.org";

  if (TSPluginRegister(&info) != TS_SUCCESS) {
    TSError("[%s] Plugin registration failed.\n", PLUGIN_NAME);

    return;
  } else {
    TSDebug(PLUGIN_NAME, "Plugin registration succeeded.\n");
  }

  plugin_config = TSmalloc(sizeof(config_t));

  plugin_config->troot                           = NULL;
  plugin_config->troot_mutex                     = TSMutexCreate();
  plugin_config->stale_if_error_override         = 0;
  plugin_config->log_info.object                 = NULL;
  plugin_config->log_info.all                    = false;
  plugin_config->log_info.stale_if_error         = false;
  plugin_config->log_info.stale_while_revalidate = false;
  plugin_config->log_info.filename               = PLUGIN_NAME;

  if (argc > 1) {
    int c;
    static const struct option longopts[] = {{"log-all", no_argument, NULL, 'a'},
                                             {"log-stale-while-revalidate", no_argument, NULL, 'r'},
                                             {"log-stale-if-error", no_argument, NULL, 'e'},
                                             {"log-filename", required_argument, NULL, 'f'},
                                             {"force-stale-if-error", required_argument, NULL, 'E'},
                                             {NULL, 0, NULL, 0}};

    while ((c = getopt_long(argc, (char *const *)argv, "aref:E:", longopts, NULL)) != -1) {
      switch (c) {
      case 'a':
        plugin_config->log_info.all = true;
        break;
      case 'r':
        plugin_config->log_info.stale_while_revalidate = true;
        break;
      case 'e':
        plugin_config->log_info.stale_if_error = true;
        break;
      case 'f':
        plugin_config->log_info.filename = strdup(optarg);
        break;
      case 'E':
        plugin_config->stale_if_error_override = atoi(optarg);
        break;
      default:
        break;
      }
    }

    if (plugin_config->log_info.all || plugin_config->log_info.stale_while_revalidate || plugin_config->log_info.stale_if_error)
      TSTextLogObjectCreate(plugin_config->log_info.filename, TS_LOG_MODE_ADD_TIMESTAMP, &(plugin_config->log_info.object));
  }

  // proxy.config.http.insert_age_in_response
  TSHttpArgIndexReserve(PLUGIN_NAME, "txn state info", &(plugin_config->txn_slot));
  main_cont = TSContCreate(main_plugin, NULL);
  TSContDataSet(main_cont, (void *)plugin_config);
  TSHttpHookAdd(TS_HTTP_READ_REQUEST_HDR_HOOK, main_cont);

  TSDebug(PLUGIN_NAME, "Plugin Init Complete.\n");
}
