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

#include "ts/Diags.h"

#define DEBUG_TAG "hugepages"

#if ! MAP_HUGETLB 

bool ats_hugepage_enabled() { return false; }
void ats_hugepage_init(int)            { Debug(DEBUG_TAG "_init", "MAP_HUGETLB not defined"); }
size_t ats_hugepage_size()             { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return 0; }
void * ats_alloc_hugepage(size_t)      { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return nullptr; }
bool ats_free_hugepage(void *, size_t) { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return false; }

#else

// XXX don't need to ifdef these out as much as hide them

#include <cstdio>
#include <sys/mman.h>
#include "ts/Diags.h"
#include "ts/ink_align.h"

#define MEMINFO_PATH "/proc/meminfo"
#define LINE_SIZE 256
#define HUGEPAGESIZE_TOKEN      "Hugepagesize:"
#define HUGEPAGESIZE_TOKEN_SIZE countof(HUGEPAGESIZE_TOKEN)

static int hugepage_size = -1;
static bool hugepage_enabled;

size_t
ats_hugepage_size()
{
  return hugepage_size;
}

bool
ats_hugepage_enabled()
{
  return hugepage_enabled;
}

void
ats_hugepage_init(int enabled)
{
  FILE *fp;
  char line[LINE_SIZE];
  char *p, *ep;

  hugepage_size = 0;

  if (!enabled) {
    Debug(DEBUG_TAG "_init", "hugepages not enabled");
    return;
  }

  fp = fopen(MEMINFO_PATH, "r");

  if (fp == nullptr) {
    Debug(DEBUG_TAG "_init", "Cannot open file %s", MEMINFO_PATH);
    return;
  }

  while (fgets(line, sizeof(line), fp)) {
    if (strncmp(line, HUGEPAGESIZE_TOKEN, HUGEPAGESIZE_TOKEN_SIZE) == 0) {
      p = line + HUGEPAGESIZE_TOKEN_SIZE;
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

  Debug(DEBUG_TAG "_init", "Hugepage size = %d", hugepage_size);
}

void *
ats_alloc_hugepage(size_t s)
{
  size_t size;
  void *mem;

  size = INK_ALIGN(s, ats_hugepage_size());

  mem = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);

  if (mem == MAP_FAILED) {
    Debug(DEBUG_TAG, "Could not allocate hugepages size = %zu", size);
    return nullptr;
  }

  Debug(DEBUG_TAG, "Request/Allocation (%zu/%zu) {%p}", s, size, mem);
  return mem;
}

bool
ats_free_hugepage(void *ptr, size_t s)
{
  size_t size;

  size = INK_ALIGN(s, ats_hugepage_size());
  return (munmap(ptr, size) == 0);
}

#endif
