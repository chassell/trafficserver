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

ParentConsistentHash::ParentConsistentHash(ParentRecord *parent_record)
{
  int i;

  ink_assert(parent_record->num_parents > 0);
  parents[PRIMARY]   = parent_record->parents;
  parents[SECONDARY] = parent_record->secondary_parents;
  ignore_query       = parent_record->ignore_query;
  ink_zero(foundParents);
  last_unavailable = 0;

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
  delete chash[PRIMARY];
  delete chash[SECONDARY];
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

  if (!ignore_query) {
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
ParentConsistentHash::selectParent(const ParentSelectionPolicy *policy, bool first_call, ParentResult *result, RequestData *rdata)
{
  ATSHash64Sip24 hash;
  HttpRequestData *request_info = static_cast<HttpRequestData *>(rdata);
  bool firstCall                = first_call;
  bool wrap_around[2]           = {false, false};
  uint64_t path_hash            = 0;
  uint32_t last_lookup;
  pRecord *prtmp = NULL, *pRec = NULL;
  URL *url = request_info->hdr->url_get();
  time_t now;

  result->retry = false;
  Debug("parent_select", "ParentConsistentHash::%s(): Using a consistent hash parent selection strategy.", __func__);
  ink_assert(numParents(result) > 0 || result->rec->go_direct == true);

  // Should only get into this state if we are supposed to go direct.
  if (parents[PRIMARY] == NULL && parents[SECONDARY] == NULL) {
    if (result->rec->go_direct == true && result->rec->parent_is_proxy == true) {
      result->result = PARENT_DIRECT;
    } else {
      result->result = PARENT_FAIL;
    }
    result->hostname = NULL;
    result->port     = 0;
    return;
  }

  // findParent() call if firstCall.
  if (firstCall) {
    last_lookup = PRIMARY;
    path_hash   = getPathHash(request_info, (ATSHash64 *)&hash);
    if (path_hash) {
      prtmp = (pRecord *)chash[PRIMARY]->lookup_by_hashval(path_hash, &result->chashIter[PRIMARY], &wrap_around[PRIMARY]);
      if (prtmp) {
        pRec = (parents[PRIMARY] + prtmp->idx);
      }
    }
    // else called by nextParent().
  } else {
    if (chash[SECONDARY] != NULL) {
      last_lookup = SECONDARY;
      path_hash   = getPathHash(request_info, (ATSHash64 *)&hash);
      prtmp = (pRecord *)chash[SECONDARY]->lookup_by_hashval(path_hash, &result->chashIter[SECONDARY], &wrap_around[SECONDARY]);
      if (prtmp) {
        pRec = (parents[SECONDARY] + prtmp->idx);
      }
    } else {
      last_lookup = PRIMARY;
      do { // search until we've selected a different parent.
        prtmp = (pRecord *)chash[PRIMARY]->lookup(NULL, &result->chashIter[PRIMARY], &wrap_around[PRIMARY], &hash);
        if (prtmp) {
          pRec = (parents[PRIMARY] + prtmp->idx);
        } else {
          pRec = NULL;
        }
      } while (prtmp && strcmp(prtmp->hostname, result->hostname) == 0);
    }
  }

  // Didn't find a parent or the parent is marked unavailable.
  //
  // Check if the unavailable parent can be retried or search again for an available parent.
  //
  if (!pRec || (pRec && !pRec->available)) {
    do {
      // check if selected host is available for retry.
      if (pRec && !pRec->available) {
        Debug("parent_select", "Parent.failedAt = %u, retry = %u, xact_start = %u", (unsigned int)pRec->failedAt,
              (unsigned int)policy->ParentRetryTime, (unsigned int)request_info->xact_start);
        // check to see if the host is available for retry, last failure is outside of the retry window.
        if ((pRec->failedAt + policy->ParentRetryTime) < request_info->xact_start) {
          // host is availble for retry, make sure that the proper state is recorded in the result structure
          result->last_parent = pRec->idx;
          result->last_lookup = last_lookup;
          result->retry       = true;
          result->result      = PARENT_SPECIFIED;
          Debug("parent_select", "Down parent %s is now retryable, marked it available.", pRec->hostname);
          break;
        }
      }
      // if there is a seoncdary ring, search it if searching has not wrapped.
      if (chash[SECONDARY] && !wrap_around[SECONDARY]) {
        last_lookup = SECONDARY;
        // if this is the first call search the secondary ring using the url hash.
        if (firstCall) {
          prtmp = (pRecord *)chash[SECONDARY]->lookup_by_hashval(path_hash, &result->chashIter[SECONDARY], &wrap_around[SECONDARY]);
          firstCall = false;
          if (prtmp) {
            pRec = (parents[SECONDARY] + prtmp->idx);
          }
          continue;
        } else { // otherwise search for the next parent.
          prtmp = (pRecord *)chash[SECONDARY]->lookup(NULL, &result->chashIter[SECONDARY], &wrap_around[SECONDARY], &hash);
          if (prtmp) {
            pRec = (parents[SECONDARY] + prtmp->idx);
          }
          continue;
        }
      } else if (!wrap_around[PRIMARY]) { // no secondary or we've wrapped on the secondary ring.
        last_lookup = PRIMARY;
        prtmp       = (pRecord *)chash[PRIMARY]->lookup(NULL, &result->chashIter[PRIMARY], &wrap_around[PRIMARY], &hash);
        if (prtmp) {
          pRec = (parents[PRIMARY] + prtmp->idx);
        }
        continue;
      }
      // if we haven't found an available or retryable parent and we've wrapped around the ring(s), no further searching is useful.
      if (chash[SECONDARY] != NULL) {
        if (wrap_around[SECONDARY] && wrap_around[PRIMARY]) {
          Debug("parent_select", "wrapped around both rings without finding an available or retryable parent.");
          break;
        }
      } else if (wrap_around[PRIMARY]) {
        Debug("parent_select", "wrapped around both rings without finding an available or retryable parent.");
        break;
      }
    } while (!pRec || !pRec->available);
  }

  // use the available parent.
  if (pRec && (pRec->available || result->retry)) {
    result->result      = PARENT_SPECIFIED;
    result->hostname    = pRec->hostname;
    result->port        = pRec->port;
    result->last_parent = pRec->idx;
    result->last_lookup = last_lookup;
    ink_assert(result->hostname != NULL);
    ink_assert(result->port != 0);
    Debug("parent_select", "Chosen parent: %s.%d", result->hostname, result->port);
  } else {
    // no available parent was found, check if we can go direct to the origin.
    if (result->rec->go_direct == true && result->rec->parent_is_proxy) {
      result->result = PARENT_DIRECT;
    } else { // no available parent and unable to go direct, set result to PARENT_FAIL.
      now = time(NULL);
      // limit logging to no more than one message per second.
      if (now > last_unavailable) {
        int len           = 0;
        char *request_str = url->string_get_ref(&len);
        if (request_str) {
          Note("No available parents for request: %*s.", len, request_str);
        }
        ink_atomic_swap(&last_unavailable, now);
      }
      result->result = PARENT_FAIL;
    }
    result->hostname = NULL;
    result->port     = 0;
    result->retry    = false;
  }

  return;
}

void
ParentConsistentHash::markParentDown(const ParentSelectionPolicy *policy, ParentResult *result)
{
  time_t now;
  pRecord *pRec;
  int new_fail_count = 0;

  Debug("parent_select", "Starting ParentConsistentHash::markParentDown()");

  //  Make sure that we are being called back with with a
  //   result structure with a parent
  ink_assert(result->result == PARENT_SPECIFIED);
  if (result->result != PARENT_SPECIFIED) {
    return;
  }
  // If we were set through the API we currently have not failover
  //   so just return fail
  if (result->is_api_result()) {
    return;
  }

  ink_assert((result->last_parent) < numParents(result));
  pRec = parents[result->last_lookup] + result->last_parent;

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
    int old_count = 0;

    now = time(NULL);

    // if the last failure was outside the retry window, clear and set the failcount to 1.
    if ((pRec->failedAt + policy->ParentRetryTime) < now) {
      ink_atomic_swap(&pRec->failCount, 1);
    } else {
      old_count = ink_atomic_increment(&pRec->failCount, 1);
    }

    Debug("parent_select", "Parent fail count increased to %d for %s:%d", old_count + 1, pRec->hostname, pRec->port);
    new_fail_count = old_count + 1;
  }

  if (new_fail_count > 0 && new_fail_count >= policy->FailThreshold) {
    Note("Failure threshold met, http parent proxy %s:%d marked down", pRec->hostname, pRec->port);
    ink_atomic_swap(&pRec->available, false);
    Debug("parent_select", "Parent %s:%d marked unavailable, pRec->available=%d", pRec->hostname, pRec->port, pRec->available);
  }
}

uint32_t
ParentConsistentHash::numParents(ParentResult *result) const
{
  uint32_t n = 0;

  switch (result->last_lookup) {
  case PRIMARY:
    n = result->rec->num_parents;
    break;
  case SECONDARY:
    n = result->rec->num_secondary_parents;
    break;
  }

  return n;
}

void
ParentConsistentHash::markParentUp(ParentResult *result)
{
  pRecord *pRec;

  //  Make sure that we are being called back with with a
  //   result structure with a parent that is being retried
  ink_release_assert(result->retry == true);
  ink_assert(result->result == PARENT_SPECIFIED);
  if (result->result != PARENT_SPECIFIED) {
    return;
  }
  // If we were set through the API we currently have not failover
  //   so just return fail
  if (result->is_api_result()) {
    ink_assert(0);
    return;
  }

  ink_assert((result->last_parent) < numParents(result));
  pRec = parents[result->last_lookup] + result->last_parent;
  ink_atomic_swap(&pRec->available, true);
  Debug("parent_select", "%s:%s(): marked %s:%d available.", __FILE__, __func__, pRec->hostname, pRec->port);

  ink_atomic_swap(&pRec->failedAt, (time_t)0);
  int old_count = ink_atomic_swap(&pRec->failCount, 0);

  if (old_count > 0) {
    Note("http parent proxy %s:%d restored", pRec->hostname, pRec->port);
  }
}
