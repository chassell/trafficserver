/** @file

  Implementation of Parent Proxy routing

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
#include "ParentConsistentHash.h"

ParentConsistentHash::ParentConsistentHash(ParentRecord *_parent_record)
{
  int i;

  go_direct = false;
  parent_record = _parent_record;
  ink_assert(parent_record->num_parents > 0);
  parents[PRIMARY] = parent_record->parents;
  parents[SECONDARY] = parent_record->secondary_parents;
  for (i = PRIMARY; i < SECONDARY + 1; i++) {
    last_parent[i] = 0;
    start_parent[i] = 0;
    wrap_around[i] = false;
  }
  memset(foundParents, 0, sizeof(foundParents));

  chash[PRIMARY] = new ATSConsistentHash();

  for (i = 0; i < parent_record->num_parents; i++) {
    chash[PRIMARY]->insert(&(parent_record->parents[i]), parent_record->parents[i].weight, (ATSHash64 *)&hash[PRIMARY]);
  }

  if (parent_record->num_secondary_parents > 0) {
    Debug("parent_select", "ParentConsistentHash(): initializing the secondary parents hash.");
    chash[SECONDARY] = new ATSConsistentHash();

    for (i = 0; i < parent_record->num_secondary_parents; i++) {
      chash[SECONDARY]->insert(&(parent_record->secondary_parents[i]), parent_record->secondary_parents[i].weight,
                               (ATSHash64 *)&hash[SECONDARY]);
    }
  } else {
    chash[SECONDARY] = NULL;
  }
  Debug("parent_select", "Using a consistent hash parent selection strategy.");
}

ParentConsistentHash::~ParentConsistentHash()
{
  if (parent_record) {
    delete parent_record;
  }
}


uint64_t
ParentConsistentHash::getPathHash(HttpRequestData *hrdata, ATSHash64 *h)
{
  const char *tmp = NULL;
  int len;
  URL *url = hrdata->hdr->url_get();

  // Always hash on '/' because paths returned by ATS are always stripped of it
  h->update("/", 1);

  tmp = url->path_get(&len);
  if (tmp) {
    h->update(tmp, len);
  }

  if (!parent_record->ignore_query) {
    tmp = url->query_get(&len);
    if (tmp) {
      h->update("?", 1);
      h->update(tmp, len);
    }
  }

  h->final();

  return h->get();
}

void
ParentConsistentHash::lookupParent(bool first_call, ParentResult *result, RequestData *rdata)
{
  Debug("parent_select", "In ParentConsistentHash::lookupParent(): Using a consistent hash parent selection strategy.");
  Debug("parent_select", "ParentConsistentHash::lookupParent(): parent_table: %p.", parent_table);
  int cur_index = 0;
  bool parentUp = false;
  bool parentRetry = false;
  uint64_t path_hash;

  ATSHash64Sip24 hash;
  pRecord *prtmp = NULL;

  HttpRequestData *request_info = static_cast<HttpRequestData *>(rdata);

  ink_assert(numParents(result) > 0 || go_direct == true);

  if (first_call) { // First lookup (called by findParent()).
    // We should only get into this state if
    //  we are supposed to go direct.
    if (parents[PRIMARY] == NULL && parents[SECONDARY] == NULL) {
      ink_assert(go_direct == true);
      // Could not find a parent
      if (go_direct == true) {
        result->r = PARENT_DIRECT;
      } else {
        result->r = PARENT_FAIL;
      }
      result->hostname = NULL;
      result->port = 0;
      return;
    } else { // lookup a parent.
      result->last_lookup = PRIMARY;
      start_parent[result->last_lookup] = 0;
      last_parent[result->last_lookup] = 0;
      wrap_around[result->last_lookup] = 0;
      path_hash = getPathHash(request_info, (ATSHash64 *)&hash);
      if (path_hash) {
        prtmp = (pRecord *)chash[result->last_lookup]->lookup_by_hashval(path_hash, &chashIter[result->last_lookup],
                                                                         &wrap_around[result->last_lookup]);
        if (prtmp) {
          cur_index = prtmp->idx;
          foundParents[result->last_lookup][cur_index] = true;
          start_parent[result->last_lookup]++;
        } else {
          Error("%s:%d - ConsistentHash lookup returned NULL (first lookup)", __FILE__, __LINE__);
          cur_index = cur_index % numParents(result);
        }
      }
    }
  } else { // Subsequent lookups (called by nextParent()).
    if (parent_record->num_secondary_parents > 0 && result->last_lookup == PRIMARY) { // if there are secondary parents, try them.
      result->last_lookup = SECONDARY;
      start_parent[result->last_lookup] = 0;
      last_parent[result->last_lookup] = 0;
      wrap_around[result->last_lookup] = 0;
      path_hash = getPathHash(request_info, (ATSHash64 *)&hash);
      if (path_hash) {
        prtmp = (pRecord *)chash[result->last_lookup]->lookup_by_hashval(path_hash, &chashIter[result->last_lookup],
                                                                         &wrap_around[result->last_lookup]);
        if (prtmp) {
          cur_index = prtmp->idx;
          foundParents[result->last_lookup][cur_index] = true;
          start_parent[result->last_lookup]++;
        } else {
          Error("%s:%d - ConsistentHash lookup returned NULL (first lookup)", __FILE__, __LINE__);
          cur_index = cur_index % numParents(result);
        }
      }
    } else {
      result->last_lookup = PRIMARY;
      Debug("parent_select", "start_parent=%d, num_parents=%d", start_parent[result->last_lookup], numParents(result));
      if (start_parent[result->last_lookup] >= (unsigned int)numParents(result)) {
        wrap_around[result->last_lookup] = true;
        start_parent[result->last_lookup] = 0;
        memset(foundParents[result->last_lookup], 0, sizeof(foundParents[result->last_lookup]));
      }

      do {
        prtmp = (pRecord *)chash[result->last_lookup]->lookup(NULL, 0, &chashIter[result->last_lookup],
                                                              &wrap_around[result->last_lookup], &hash);
      } while (prtmp && foundParents[result->last_lookup][prtmp->idx]);

      if (prtmp) {
        cur_index = prtmp->idx;
        foundParents[result->last_lookup][cur_index] = true;
        start_parent[result->last_lookup]++;
      } else {
        Error("%s:%d - Consistent Hash lookup returned NULL (subsequent lookup)", __FILE__, __LINE__);
        cur_index = ink_atomic_increment((int32_t *)&parent_record->rr_next, 1);
        cur_index = cur_index % numParents(result);
      }
    }
  }

  // Loop through the array of parent seeing if any are up or
  //   should be retried
  do {
    // DNS ParentOnly inhibits bypassing the parent so always return that t
    if ((parents[result->last_lookup][cur_index].failedAt == 0) ||
        (parents[result->last_lookup][cur_index].failCount < FailThreshold)) {
      Debug("parent_select", "FailThreshold = %d", FailThreshold);
      Debug("parent_select", "Selecting a parent due to little failCount"
                             "(faileAt: %u failCount: %d)",
            (unsigned)parents[result->last_lookup][cur_index].failedAt, parents[result->last_lookup][cur_index].failCount);
      parentUp = true;
    } else {
      if ((wrap_around[result->last_lookup]) ||
          ((parents[result->last_lookup][cur_index].failedAt + ParentRetryTime) < request_info->xact_start)) {
        Debug("parent_select", "Parent[%d].failedAt = %u, retry = %u,xact_start = %" PRId64 " but wrap = %d", cur_index,
              (unsigned)parents[result->last_lookup][cur_index].failedAt, ParentRetryTime, (int64_t)request_info->xact_start,
              wrap_around[result->last_lookup]);
        // Reuse the parent
        parentUp = true;
        parentRetry = true;
        Debug("parent_select", "Parent marked for retry %s:%d", parents[result->last_lookup][cur_index].hostname,
              parents[result->last_lookup][cur_index].port);
      } else {
        parentUp = false;
      }
    }

    if (parentUp == true) {
      if (!parent_record->parent_is_proxy) {
        result->r = PARENT_ORIGIN;
      } else {
        result->r = PARENT_SPECIFIED;
      }
      result->hostname = parents[result->last_lookup][cur_index].hostname;
      result->port = parents[result->last_lookup][cur_index].port;
      result->last_parent = cur_index;
      result->retry = parentRetry;
      ink_assert(result->hostname != NULL);
      ink_assert(result->port != 0);
      Debug("parent_select", "Chosen parent = %s.%d", result->hostname, result->port);
      return;
    }

    if (start_parent[result->last_lookup] == (unsigned int)numParents(result)) {
      wrap_around[result->last_lookup] = true;
      start_parent[result->last_lookup] = 0;
      memset(foundParents[result->last_lookup], 0, sizeof(foundParents[result->last_lookup]));
    }

    do {
      prtmp = (pRecord *)chash[result->last_lookup]->lookup(NULL, 0, &chashIter[result->last_lookup],
                                                            &wrap_around[result->last_lookup], &hash);
    } while (prtmp && foundParents[result->last_lookup][prtmp->idx]);

    if (prtmp) {
      cur_index = prtmp->idx;
      foundParents[result->last_lookup][cur_index] = true;
      start_parent[result->last_lookup]++;
    }
  } while (wrap_around[result->last_lookup]);

  if (go_direct == true) {
    result->r = PARENT_DIRECT;
  } else {
    result->r = PARENT_FAIL;
  }

  result->hostname = NULL;
  result->port = 0;
}

void
ParentConsistentHash::markParentDown(ParentResult *result)
{
  time_t now;
  pRecord *pRec;
  int new_fail_count = 0;

  Debug("parent_select", "Starting ParentConsistentHash::markParentDown()");

  //  Make sure that we are being called back with with a
  //   result structure with a parent
  ink_assert(result->r == PARENT_SPECIFIED || result->r == PARENT_ORIGIN);
  if (result->r != PARENT_SPECIFIED && result->r != PARENT_ORIGIN) {
    return;
  }
  // If we were set through the API we currently have not failover
  //   so just return fail
  if (result->rec == extApiRecord) {
    return;
  }

  ink_assert((last_parent[result->last_lookup]) < numParents(result));
  pRec = parents[result->last_lookup] + last_parent[result->last_lookup];

  // If the parent has already been marked down, just increment
  //   the failure count.  If this is the first mark down on a
  //   parent we need to both set the failure time and set
  //   count to one.  It's possible for the count and time get out
  //   sync due there being no locks.  Therefore the code should
  //   handle this condition.  If this was the result of a retry, we
  //   must update move the failedAt timestamp to now so that we continue
  //   negative cache the parent
  if (pRec->failedAt == 0 || result->retry == true) {
    // Reread the current time.  We want this to be accurate since
    //   it relates to how long the parent has been down.
    now = time(NULL);

    // Mark the parent as down
    ink_atomic_swap(&pRec->failedAt, now);

    // If this is clean mark down and not a failed retry, we
    //   must set the count to reflect this
    if (result->retry == false) {
      new_fail_count = pRec->failCount = 1;
    }

    Note("Parent %s marked as down %s:%d", (result->retry) ? "retry" : "initially", pRec->hostname, pRec->port);

  } else {
    int old_count = ink_atomic_increment(&pRec->failCount, 1);

    Debug("parent_select", "Parent fail count increased to %d for %s:%d", old_count + 1, pRec->hostname, pRec->port);
    new_fail_count = old_count + 1;
  }

  if (new_fail_count > 0 && new_fail_count == FailThreshold) {
    Note("Failure threshold met, http parent proxy %s:%d marked down", pRec->hostname, pRec->port);
    pRec->available = false;
    Debug("parent_select", "Parent marked unavailable, pRec->available=%d", pRec->available);
  }
}

uint32_t
ParentConsistentHash::numParents(ParentResult *result)
{
  uint32_t n = 0;

  switch (result->last_lookup) {
  case PRIMARY:
    n = parent_record->num_parents;
    break;
  case SECONDARY:
    n = parent_record->num_secondary_parents;
    break;
  }

  return n;
}

void
ParentConsistentHash::recordRetrySuccess(ParentResult *result)
{
  pRecord *pRec;

  //  Make sure that we are being called back with with a
  //   result structure with a parent that is being retried
  ink_release_assert(result->retry == true);
  ink_assert(result->r == PARENT_SPECIFIED || result->r == PARENT_ORIGIN);
  if (result->r != PARENT_SPECIFIED && result->r != PARENT_ORIGIN) {
    return;
  }
  // If we were set through the API we currently have not failover
  //   so just return fail
  if (result->rec == extApiRecord) {
    ink_assert(0);
    return;
  }

  ink_assert((last_parent[result->last_lookup]) < numParents(result));
  pRec = parents[result->last_lookup] + last_parent[result->last_lookup];
  pRec->available = true;

  ink_atomic_swap(&pRec->failedAt, (time_t)0);
  int old_count = ink_atomic_swap(&pRec->failCount, 0);

  if (old_count > 0) {
    Note("http parent proxy %s:%d restored", pRec->hostname, pRec->port);
  }
}
