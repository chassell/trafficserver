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

#include <cstdio>
#include <sys/mman.h>
#include "Diags.h"
#include "ink_align.h"

#define DEBUG_TAG "hugepages"
#define MEMINFO_PATH "/proc/meminfo"
#define LINE_SIZE 256
#define TOKEN "Hugepagesize:"
#define TOKEN_SIZE (strlen(TOKEN))

static int hugepage_size = -1;
static bool hugepage_enabled;

size_t
ats_hugepage_size(void)
{
#ifdef MAP_HUGETLB
  return hugepage_size;
#else
  Debug(DEBUG_TAG, "MAP_HUGETLB not defined");
  return 0;
#endif
}

bool
ats_hugepage_enabled(void)
{
#ifdef MAP_HUGETLB
  return hugepage_enabled;
#else
  return false;
#endif
}

void
ats_hugepage_init(int enabled)
{
#ifdef MAP_HUGETLB
  FILE *fp;
  char line[LINE_SIZE];
  char *p, *ep;

  hugepage_size = 0;

  if (!enabled) {
    Debug(DEBUG_TAG, "hugepages not enabled");
    return;
  }

  fp = fopen(MEMINFO_PATH, "r");

  if (fp == NULL) {
    Debug(DEBUG_TAG, "Cannot open file %s", MEMINFO_PATH);
    return;
  }

  while (fgets(line, sizeof(line), fp)) {
    if (strncmp(line, TOKEN, TOKEN_SIZE) == 0) {
      p = line + TOKEN_SIZE;
      while (*p == ' ') {
        p++;
      }
      hugepage_size = strtol(p, &ep, 10);
      // What other values can this be?
      if (strncmp(ep, " kB", 4)) {
        hugepage_size *= 1024;
      }
      break;
    }
  }

  fclose(fp);

  if (hugepage_size) {
    hugepage_enabled = true;
  }

  Debug(DEBUG_TAG, "Hugepage size = %d", hugepage_size);
#else
  Debug(DEBUG_TAG, "MAP_HUGETLB not defined");
#endif
}

void *
ats_alloc_hugepage(size_t s ATS_UNUSED)
{
#ifdef MAP_HUGETLB
  size_t size;
  void *mem;

  size = INK_ALIGN(s, ats_hugepage_size());

  mem = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);

  if (mem == NULL) {
    Debug(DEBUG_TAG, "Could not allocate hugepages size = %zu", size);
  }

  return mem;
#else
  (void)s;
  Debug(DEBUG_TAG, "MAP_HUGETLB not defined");
  return NULL;
#endif
}
