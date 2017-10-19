/** @file

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

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <getopt.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ts/ts.h>

#ifdef HAVE_PCRE_PCRE_H
#include <pcre/pcre.h>
#else
#include <pcre.h>
#endif

#define LOG_PREFIX "regex_revalidate"
#define CONFIG_TMOUT 20000
#define FREE_TMOUT 300000
#define OVECTOR_SIZE 30
#define LOG_ROLL_INTERVAL 86400
#define LOG_ROLL_OFFSET 0

// rational maximum
#define MAX_TTL (365 * 24 * 3600)

static inline void *
ts_malloc(size_t s)
{
  return TSmalloc(s);
}

static inline void
ts_free(void *s)
{
  return TSfree(s);
}

typedef struct invalidate_t {
  const char *regex_text;
  pcre *regex;
  pcre_extra *regex_extra;
  time_t refresh;
  time_t expiry;
  time_t priority;
  struct invalidate_t *next;
} invalidate_t;

typedef struct {
  invalidate_t *invalidate_list;
  char *config_file;
  time_t last_load;
  TSTextLogObject log;
} plugin_state_t;

static void
free_invalidate_t(invalidate_t *i)
{
  if (!i) {
    return;
  }
  if (i->regex_extra) {
#ifndef PCRE_STUDY_JIT_COMPILE
    pcre_free(i->regex_extra);
#else
    pcre_free_study(i->regex_extra);
#endif
  }
  if (i->regex) {
    pcre_free(i->regex);
  }
  if (i->regex_text) {
    pcre_free_substring(i->regex_text);
  }
  TSfree(i);
}

static void
free_invalidate_t_list(invalidate_t *i)
{
  while (i) {
    invalidate_t *p = i;
    i               = p->next;
    free_invalidate_t(p);
  }
}

static plugin_state_t *
init_plugin_state_t(plugin_state_t *pstate)
{
  pstate->invalidate_list = NULL;
  pstate->config_file     = NULL;
  pstate->last_load       = 0;
  pstate->log             = NULL;
  return pstate;
}

static void
free_plugin_state_t(plugin_state_t *pstate)
{
  free_invalidate_t_list(pstate->invalidate_list);
  if (pstate->config_file) {
    TSfree(pstate->config_file);
  }
  if (pstate->log) {
    TSTextLogObjectDestroy(pstate->log);
  }
  TSfree(pstate);
}

static invalidate_t *
copy_invalidate_t(invalidate_t *i)
{
  invalidate_t *iptr;
  const char *errptr;
  int erroffset;

  iptr              = (invalidate_t *)TSmalloc(sizeof(invalidate_t));
  iptr->regex_text  = TSstrdup(i->regex_text);
  iptr->regex       = pcre_compile(iptr->regex_text, 0, &errptr, &erroffset, NULL); // There is no pcre_copy :-(
  iptr->regex_extra = pcre_study(iptr->regex, 0, &errptr); // Assuming no errors since this worked before :-/
  iptr->refresh     = i->refresh;
  iptr->expiry      = i->expiry;
  iptr->priority    = i->priority;
  iptr->next        = NULL;
  return iptr;
}

static invalidate_t *
copy_config(invalidate_t *oelt)
{
  invalidate_t *head = NULL;
  invalidate_t **itr = &head;
  while (oelt) {
    (*itr) = copy_invalidate_t(oelt);
    itr = &(*itr)->next;
    oelt = oelt->next;
  }

  return head;
}

static void
prune_config(const time_t now, invalidate_t **iref)
{
  for( ; *iref && (*iref)->expiry > now ; iref = &(*iref)->next ) {
    // not yet expired elts
  }

  if ( *iref ) {
    prune_config(now, &(*iref)->next);

    invalidate_t *expd = *iref; // [below recurse-down]

    // atomically snip out:  i.e. (*iref) = expd->next
    if ( __sync_val_compare_and_swap(iref, expd, expd->next) == expd ) {
      TSDebug(LOG_PREFIX, "Removing expired %+lds: %s", expd->expiry - now, expd->regex_text);
      free_invalidate_t(expd);
    }
  }
}

invalidate_t *
load_line(int ln, const char *line, time_t cfgnow, time_t now, const pcre *config_re)
{
  const char *errptr;
  int erroffset, ovector[OVECTOR_SIZE], rc;
  invalidate_t *i;

  // ([0-1] defines entire pattern match
  // ([6-7] defines optional-subpatt match
  //            rc == 3 :   regex:[2-3]  expiry:[4-5]     ----
  //            rc == 4 :   regex:[2-3]   start:[4-5]    ttl:[8-9]

  rc = pcre_exec(config_re, NULL, line, strlen(line), 0, 0, ovector, OVECTOR_SIZE);

  if (rc != 3 && rc != 5) {
    TSDebug(LOG_PREFIX, "Skipping line %d: %s", ln, line);
    return NULL; /// CONTINUE
  }

  i = (invalidate_t *)TSmalloc(sizeof(invalidate_t));
  memset(i, '\0', sizeof(*i)); // if early-freed

  pcre_get_substring(line, ovector, rc, 1, &i->regex_text);

  if (rc == 3) {
    // regex-identical lines?: later expiry takes precedence
    i->expiry   = atoi(line + ovector[4]);
    i->refresh  = cfgnow;    // assumed new
    i->priority = i->expiry; // among all regexs
  } else if (rc == 5) {
    // regex-identical lines?: later refresh takes precedence
    i->refresh  = atoi(line + ovector[4]);
    i->expiry   = cfgnow + atoi(line + ovector[8]);
    i->priority = i->refresh; // among all regexs
  }

  if (i->expiry <= now) {
    TSDebug(LOG_PREFIX, "Ignoring expired %+lds: %s", i->expiry - now, i->regex_text);
    TSError(LOG_PREFIX " - NOT Loaded, already expired: %s %+lds", i->regex_text, now - i->expiry);
    free_invalidate_t(i);
    return NULL; /// CONTINUE
  }

  // line not expired

  i->regex = pcre_compile(i->regex_text, 0, &errptr, &erroffset, NULL);
  if (i->regex == NULL) {
    TSDebug(LOG_PREFIX, "Failed regex compile: %s", i->regex_text);
    free_invalidate_t(i);
    return NULL; /// CONTINUE
  }

  i->regex_extra = pcre_study(i->regex, 0, &errptr);
  i->next        = NULL;
  return i;
}


invalidate_t **
find_lower_bound(invalidate_t **itr, invalidate_t *i)
{
  //
  // insert into a later-expire-first sorted linked list
  //    (i.e. skip all later entries and stop at first earlier one)
  //
  while (*itr) {

    // higher-pri than rest?
    if ((*itr)->priority < i->priority) {
      return itr; // lower bound 
    }

    int cmp = strcmp(i->regex_text, (*itr)->regex_text);

    // at equal-pri and higher-regex?
    if ((*itr)->priority == i->priority && cmp < 0) {
      return itr; //// RETURN 
    }

    // everything is equal?
    if (!cmp && (*itr)->priority == i->priority) {
      return NULL; //// RETURN 
    }

    // regex are equal but adding lower-pri?  discard dup entry now
    if (!cmp) {

      time_t now = time(NULL);
      invalidate_t *idup = *itr;
      (*itr) = (*itr)->next;

      TSDebug(LOG_PREFIX, "Old-config duplicate %+.3fhrs+%.3fhrs (vs. %+.3f+%.3fhrs): %s", 
               (idup->refresh - now)/3600.0, (idup->expiry - idup->refresh)/3600.0, 
               (i->refresh - now)/3600.0, (i->expiry - i->refresh)/3600.0, i->regex_text);

      free_invalidate_t(idup);
      continue; ///// CONTINUE (forward)
    }

    // still have lower-pri or lower-regex 
    itr = &(*itr)->next; 
  }

  return itr; // use end-anchor
}

invalidate_t **
find_dup_regex(invalidate_t **itr, invalidate_t *i)
{
  invalidate_t **rdup = &i->next;

  // check later for dups to remove
  while (*rdup && strcmp(i->regex_text, (*rdup)->regex_text)) {
    rdup = &(*rdup)->next;
  }

  return (*rdup ? rdup : NULL);
}

static time_t
load_config(plugin_state_t *pstate, invalidate_t **ilist)
{
  FILE *fs;
  struct stat s;
  size_t path_len;
  char *path;
  char line[LINE_MAX];
  time_t now, cfgnow, newload;
  int ln = 0;
  invalidate_t *i;

  if (pstate->config_file[0] != '/') {
    path_len = strlen(TSConfigDirGet()) + strlen(pstate->config_file) + 2;
    path     = alloca(path_len);
    snprintf(path, path_len, "%s/%s", TSConfigDirGet(), pstate->config_file);
  } else {
    path = pstate->config_file;
  }

  if (stat(path, &s) < 0) {
    TSDebug(LOG_PREFIX, "Could not stat %s", path);
    return 0; ////// RETURN
  }

  if (s.st_mtime <= pstate->last_load) {
    TSDebug(LOG_PREFIX, "File mod time is not newer: [%+lds]", s.st_mtime - pstate->last_load);
    return 0; ////// RETURN
  }

  newload = 0;
  cfgnow = s.st_mtime;
  now    = time(NULL);

  if (!(fs = fopen(path, "r"))) {
    TSDebug(LOG_PREFIX, "Could not open %s for reading", path);
    return 0; ////// RETURN
  }

  static const pcre *config_re = NULL;

  // fill out once
  if (!config_re) {
    const char *errptr;
    int erroffset;

#define URLSUB "([^#].+?)"
#define WSPC "\\s+"
#define INTSUB "(\\d+)"

    config_re = pcre_compile("^" URLSUB WSPC INTSUB "(" WSPC INTSUB ")?"
                             "\\s*$",
                             0, &errptr, &erroffset, NULL);
  }

  for (ln = 0; fgets(line, LINE_MAX, fs); ++ln) {
    // assign file date if new lines are found
    i = load_line(ln, line, cfgnow, now, config_re);
    if (!i) {
      continue; ////// CONTINUE (skip line)
    }

    // find a linked-list anchor to insert
    invalidate_t **itr = find_lower_bound(ilist, i);

    if (!itr) {
      free_invalidate_t(i);
      i = NULL;
      continue; // CONTINUE (dup line with previous)
    }

    newload = cfgnow; // new entry present

    // insert element before GLB
    i->next = *itr;
    *itr    = i;

    TSDebug(LOG_PREFIX, "New-config refresh %+.3fhrs + %.3fhrs: %s", (i->refresh - now)/3600.0, (i->expiry - i->refresh)/3600.0, i->regex_text);

    // remove 'overshadowed' entry
    if ((itr = find_dup_regex(&i->next, i))) {
      invalidate_t *idup = *itr;

      TSDebug(LOG_PREFIX, "Old-config duplicate %+.3f+%.3fhrs (vs. %+.3f+%.3fhrs): %s", (idup->refresh - now)/3600.0, (idup->expiry - idup->refresh)/3600.0, 
              (i->refresh - now)/3600.0, (i->expiry - i->refresh)/3600.0, i->regex_text);

      (*itr) = (*itr)->next; // snip
      free_invalidate_t(idup);

    }
  }

  TSDebug(LOG_PREFIX, "File mod time updated: %d lines [%+lds]", ln, cfgnow - pstate->last_load);

  fclose(fs);
  pstate->last_load = cfgnow; // updated for file chk
  return newload; // zero if nothing changed
}

static void
list_config(plugin_state_t *pstate, invalidate_t *i)
{
  invalidate_t *iptr;
  time_t now = time(NULL);

  TSDebug(LOG_PREFIX, "Current config:");
  if (pstate->log) {
    TSTextLogObjectWrite(pstate->log, "Current config:");
  }
  if (i) {
    iptr = i;
    while (iptr) {
      TSDebug(LOG_PREFIX, "refresh/expiry: %+.3fhrs %+.3fhrs %s", (iptr->refresh - now)/3600.0, (iptr->expiry - now)/3600.0, iptr->regex_text);
      if (pstate->log) {
        TSTextLogObjectWrite(pstate->log, "%s refresh: %ld expiry: %ld", iptr->regex_text, iptr->refresh, iptr->expiry);
      }
      iptr = iptr->next;
    }
  } else {
    TSDebug(LOG_PREFIX, "EMPTY");
    if (pstate->log) {
      TSTextLogObjectWrite(pstate->log, "EMPTY");
    }
  }
}

static int
free_handler(TSCont cont, TSEvent event ATS_UNUSED, void *edata ATS_UNUSED)
{
  invalidate_t *iptr;

  TSDebug(LOG_PREFIX, "Freeing old config");
  iptr = (invalidate_t *)TSContDataGet(cont);
  free_invalidate_t_list(iptr);
  TSContDestroy(cont);
  return 0;
}

static int
config_handler(TSCont cont, TSEvent event ATS_UNUSED, void *edata ATS_UNUSED)
{
  invalidate_t *newlist, *oldlist, *tofree;
  TSCont free_cont;
  plugin_state_t *pstate = (plugin_state_t *)TSContDataGet(cont);
  invalidate_t **const rhead = &pstate->invalidate_list;

  TSDebug(LOG_PREFIX, "In config Handler");

  prune_config(time(NULL),rhead); // atomic deletes

  // sample/merge from *rhead once

  oldlist = *rhead;
  newlist = copy_config(oldlist);
  tofree  = newlist;

  // merge any new entries in and remove older entries
  if (load_config(pstate, &newlist) && __sync_val_compare_and_swap(rhead, oldlist, newlist) == oldlist ) {
    tofree = oldlist;
  }

  // last_load was updated
  TSContSchedule(cont, CONFIG_TMOUT, TS_THREAD_POOL_TASK);

  if (tofree == newlist) {
    TSDebug(LOG_PREFIX, (*rhead != oldlist ? "Blocked" : "No Changes"));
    free_invalidate_t_list(newlist);
    return -1; //// RETURN
  }

  list_config(pstate, newlist); // successfully swapped

  free_cont = TSContCreate(free_handler, TSMutexCreate());
  TSContDataSet(free_cont, (void *)tofree);
  // allow time for old list to become unused
  TSContSchedule(free_cont, FREE_TMOUT, TS_THREAD_POOL_TASK);
  return 0; //// RETURN (success)
}

static time_t
get_date_from_cached_hdr(TSHttpTxn txn)
{
  TSMBuffer buf;
  TSMLoc hdr_loc, date_loc;
  time_t date = 0;

  if (TSHttpTxnCachedRespGet(txn, &buf, &hdr_loc) == TS_SUCCESS) {
    date_loc = TSMimeHdrFieldFind(buf, hdr_loc, TS_MIME_FIELD_DATE, TS_MIME_LEN_DATE);
    if (date_loc != TS_NULL_MLOC) {
      date = TSMimeHdrFieldValueDateGet(buf, hdr_loc, date_loc);
      TSHandleMLocRelease(buf, hdr_loc, date_loc);
    }
    TSHandleMLocRelease(buf, TS_NULL_MLOC, hdr_loc);
  }

  return date;
}

static int
main_handler(TSCont cont, TSEvent event, void *edata)
{
  TSHttpTxn txn = (TSHttpTxn)edata;
  int status;
  invalidate_t *iptr;
  plugin_state_t *pstate;

  time_t date = 0, now = 0;
  char *url   = NULL;
  int url_len = 0;

  switch (event) {
  case TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE:
    if (TSHttpTxnCacheLookupStatusGet(txn, &status) == TS_SUCCESS) {
      if (status == TS_CACHE_LOOKUP_HIT_FRESH) {
        pstate = (plugin_state_t *)TSContDataGet(cont);
        iptr   = pstate->invalidate_list;
        while (iptr) {
          if (!date) {
            date = get_date_from_cached_hdr(txn);
            now  = time(NULL);
          }
          if (iptr->refresh >= date && iptr->expiry >= now) {
            if (!url) {
              url = TSHttpTxnEffectiveUrlStringGet(txn, &url_len);
            }
            if (pcre_exec(iptr->regex, iptr->regex_extra, url, url_len, 0, 0, NULL, 0) >= 0) {
              TSHttpTxnCacheLookupStatusSet(txn, TS_CACHE_LOOKUP_HIT_STALE);
              iptr = NULL;
              TSDebug(LOG_PREFIX, "Forced revalidate - %.*s", url_len, url);
            }
          }
          if (iptr) {
            iptr = iptr->next;
          }
        }
        if (url) {
          TSfree(url);
        }
      }
    }
    break;
  default:
    break;
  }

  TSHttpTxnReenable(txn, TS_EVENT_HTTP_CONTINUE);
  return 0;
}

static bool
check_ts_version()
{
  const char *ts_version = TSTrafficServerVersionGet();

  if (ts_version) {
    int major_ts_version = 0;
    int minor_ts_version = 0;
    int micro_ts_version = 0;

    if (sscanf(ts_version, "%d.%d.%d", &major_ts_version, &minor_ts_version, &micro_ts_version) != 3) {
      return false;
    }

    if ((TS_VERSION_MAJOR == major_ts_version) && (TS_VERSION_MINOR == minor_ts_version) &&
        (TS_VERSION_MICRO == micro_ts_version)) {
      return true;
    }
  }

  return false;
}

void
TSPluginInit(int argc, const char *argv[])
{
  TSPluginRegistrationInfo info;
  TSCont main_cont, config_cont;
  plugin_state_t *pstate;
  invalidate_t *iptr = NULL;

  TSDebug(LOG_PREFIX, "Starting plugin init");

  pstate = (plugin_state_t *)TSmalloc(sizeof(plugin_state_t));
  init_plugin_state_t(pstate);

  int c;
  static const struct option longopts[] = {
    {"config", required_argument, NULL, 'c'}, {"log", required_argument, NULL, 'l'}, {NULL, 0, NULL, 0}};

  while ((c = getopt_long(argc, (char *const *)argv, "c:l:", longopts, NULL)) != -1) {
    switch (c) {
    case 'c':
      pstate->config_file = TSstrdup(optarg);
      break;
    case 'l':
      if (TS_SUCCESS == TSTextLogObjectCreate(optarg, TS_LOG_MODE_ADD_TIMESTAMP, &pstate->log)) {
        TSTextLogObjectRollingEnabledSet(pstate->log, 1);
        TSTextLogObjectRollingIntervalSecSet(pstate->log, LOG_ROLL_INTERVAL);
        TSTextLogObjectRollingOffsetHrSet(pstate->log, LOG_ROLL_OFFSET);
      }
      break;
    default:
      break;
    }
  }

  if (!pstate->config_file) {
    TSError("[regex_revalidate] Plugin requires a --config option along with a config file name");
    free_plugin_state_t(pstate);
    return;
  }

  if (!load_config(pstate, &iptr)) {
    TSDebug(LOG_PREFIX, "Problem loading config from file %s", pstate->config_file);
  } else {
    pstate->invalidate_list = iptr;
    list_config(pstate, iptr);
  }

  info.plugin_name   = LOG_PREFIX;
  info.vendor_name   = "Apache Software Foundation";
  info.support_email = "dev@trafficserver.apache.org";

  if (TSPluginRegister(&info) != TS_SUCCESS) {
    TSError("[regex_revalidate] Plugin registration failed");

    free_plugin_state_t(pstate);
    return;
  } else {
    TSDebug(LOG_PREFIX, "Plugin registration succeeded");
  }

  if (!check_ts_version()) {
    TSError("[regex_revalidate] Plugin requires Traffic Server %d.%d.%d", TS_VERSION_MAJOR, TS_VERSION_MINOR, TS_VERSION_MICRO);
    free_plugin_state_t(pstate);
    return;
  }

  pcre_malloc = &ts_malloc;
  pcre_free   = &ts_free;

  main_cont = TSContCreate(main_handler, NULL);
  TSContDataSet(main_cont, (void *)pstate);
  TSHttpHookAdd(TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, main_cont);

  config_cont = TSContCreate(config_handler, TSMutexCreate());
  TSContDataSet(config_cont, (void *)pstate);
  TSContSchedule(config_cont, CONFIG_TMOUT, TS_THREAD_POOL_TASK);

  TSDebug(LOG_PREFIX, "Plugin Init Complete");
}
