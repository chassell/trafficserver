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
#include "ink_memory.h"

#define DEBUG_TAG "hugepages"

#if ! MAP_HUGETLB 

bool ats_hugepage_enabled() { return false; }
void ats_hugepage_init(int)            { Debug(DEBUG_TAG "_init", "MAP_HUGETLB not defined"); }
size_t ats_hugepage_size()             { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return 0; }
void * ats_alloc_hugepage(size_t)      { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return nullptr; }
bool ats_free_hugepage(void *, size_t) { Debug(DEBUG_TAG, "MAP_HUGETLB not defined"); return false; }

#else

// XXX don't need to ifdef these out as much as hide them

#include "ts/Diags.h"
#include "ts/ink_align.h"

#include <cstdio>
#include <sys/mman.h>
#include <sys/user.h>

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

/*
   void *(chunk_alloc_t)  (void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind);
   bool (chunk_dalloc_t)  (void *chunk, size_t size, bool committed, unsigned arena_ind);
   bool (chunk_commit_t)  (void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);
   bool (chunk_decommit_t)(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);
   bool (chunk_purge_t)   (void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);
   bool (chunk_split_t)   (void *chunk, size_t size, size_t size_a, size_t size_b, bool committed, unsigned arena_ind);
   bool (chunk_merge_t)   (void *chunk_a, size_t size_a, void *chunk_b, size_t size_b, bool committed, unsigned arena_ind);
*/


namespace {

static void *huge_normal_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind=0);
static void *huge_nodump_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind=0);
static bool huge_dalloc(void *chunk, size_t size, bool committed, unsigned arena_ind=0);
static bool huge_commit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind=0);
static bool huge_decommit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind=0);
static bool huge_purge(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind=0);
static bool huge_split(void *chunk, size_t size, size_t size_a, size_t size_b, bool committed, unsigned arena_ind=0);
static bool huge_merge(void *chunk_a, size_t size_a, void *chunk_b, size_t size_b, bool committed, unsigned arena_ind=0);

#if HAVE_LIBJEMALLOC
const chunk_hooks_t jemallctl_huge_hooks = { 
  &huge_normal_alloc, &huge_dalloc,
  &huge_commit, &huge_decommit,
  &huge_purge,
  &huge_split, &huge_merge
};

const chunk_hooks_t jemallctl_huge_nodump_hooks = { 
  &huge_nodump_alloc, &huge_dalloc,
  &huge_commit, &huge_decommit,
  &huge_purge,
  &huge_split, &huge_merge
};
#endif


#if defined(linux)

//
// perform allocation or over-allocation of a hugepage at a valid point (big enough) in memory
//    if a point of memory is re-allocated larger, return immediately if it fits within the hugepage
//

void *huge_alloc_and_madvise(void *chunk, size_t csize, size_t hpgsz, 
                              bool *zero, bool *commit, unsigned madvflags=MADV_NORMAL)
{
  // NOTE: if chunk was NULL then it is hugepage-identical and startpg is NULL too
  void *nxtchunk = static_cast<char*>(chunk) + csize;
  void *startpg = align_pointer_backward(chunk,hpgsz);
  void *nextpg = align_pointer_forward(nxtchunk, hpgsz);
  size_t opsize = static_cast<char*>(nextpg) - static_cast<char*>(startpg); 

  bool fakecommit = false; // assume false if nullptr is passed
  bool fakezero = false; // assume false if nullptr is passed

  zero = ( zero ? : &fakezero );
  commit = ( commit ? : &fakecommit );

  // is this getting a totally new segment?
  if ( ! startpg ) 
  {
    // create oversized new segment first (oversize by full hugepage so end always has >=1 unmapped page)
    chunk = mmap(nullptr, opsize + hpgsz, PROT_NONE, (MAP_PRIVATE|MAP_ANONYMOUS), -1, 0);
    nxtchunk = static_cast<char*>(startpg) + opsize + hpgsz; // past the end of request

    if ( ! chunk ) {
      return nullptr; // could not get unmapped open segment!                           //// ERROR alloc-mmap fail
    }

    startpg = align_pointer_forward(chunk,hpgsz); // final new mapping start
    nextpg = static_cast<char*>(startpg) + opsize;  // final new mapping end

    // finally, commit the mapping with usable hugepages 
    //    (or with small pages until they are replaced)
    if ( ( *commit || *zero ) && huge_commit(startpg,opsize,0,opsize) == true ) 
    {
      munmap(chunk,opsize + hpgsz); // undo prev. mmap
      return nullptr; // could not commit this many pages!                             //// ERROR alloc-commit fail
    }

    // clean up excess edges
    munmap(chunk, static_cast<char*>(startpg) - static_cast<char*>(chunk));
    munmap(nxtchunk, static_cast<char*>(nxtchunk) - static_cast<char*>(nextpg));

    // if we've committed .. we also zeroed (and opposite too)
    *zero = *commit = ( *commit || *zero );

    // successfully created map 

    madvise(startpg,opsize,madvflags | (MAP_NORESERVE|MADV_HUGEPAGE)); // apply flags to the new map remaining
    return startpg;                                                                  //// SUCCESS alloc 
  }

  //
  // expansion-realloc (at same address) is being requested 
  //    [XXX: must assume last-hugepage included small-pages were *never* mapped]
  //

  // will assert empty-protections for new realloc range?
  if ( ! *zero && ! *commit && chunk == startpg ) 
  {
    // map a hugepage-aligned prot-none range of memory
    if ( huge_decommit(startpg,opsize,0,opsize) == true ) { 
      return nullptr; // mmap with prot-none failed                                   //// ERROR realloc-decommit fail
    }
    // apply flags over entire map range owned
    madvise(startpg,opsize,madvflags|(MAP_NORESERVE|MADV_HUGEPAGE)); 
    return chunk; // realloc is completed with no-zero/no-commit permissions          //// SUCCESS realloc-emptied
  }

  // will assert open protections for new realloc range

  // commit to writable memory allowed for new pages and old
  if ( huge_commit(startpg,opsize,0,opsize) == true ) {
    return nullptr; // cannot realloc this segment safely                             //// ERROR realloc-commit fail
  }

  // memory is writable and committed by OS

  *commit = true;
  madvflags |= ( *zero ? MADV_DONTNEED : 0 ); // add purge flag in if needed..

  // apply flags to the new map remaining

  if ( chunk == startpg ) // allow passed flags for realloc'ed hugepages?
  {
    madvise(startpg, opsize, madvflags|(MAP_NORESERVE|MADV_HUGEPAGE)); 
  } 
  else if ( *zero ) // add safest flags and attempt partial purge to realloc'ed hugepage?
  {
    huge_purge(chunk,csize,0,csize);
    madvise(startpg, opsize, (MAP_NORESERVE|MADV_HUGEPAGE)); 
  } 
  else // just add safest flags to realloc'ed hugepage?
  {
    madvise(startpg, opsize, (MAP_NORESERVE|MADV_HUGEPAGE)); // add safe flags to unaligned hugepages
  }

  return chunk; // realloc is complete                                                //// SUCCESS realloc-ready
}

//
// all page allocation and advise for normal memory use
//

void *huge_normal_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind)
{
  size_t hpgsize = std::max(ats_hugepage_size(),alignment);
  return huge_alloc_and_madvise(chunk, size, hpgsize, zero, commit);
}

//
// all page allocation and advise for cache-storage memory use
//

void *huge_nodump_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind)
{
  size_t hpgsize = std::max(ats_hugepage_size(),alignment);
  return huge_alloc_and_madvise(chunk, size, hpgsize, zero, commit, MADV_DONTDUMP);
}

//
// perform de-allocation of integer numbers of hugepages (only) and reject all else
//    FIXME: this may prevent *ever* releasing large swaths of pages
//

bool huge_dalloc(void *chunk, size_t size, bool committed, unsigned arena_ind)
{
  const size_t hpgsz = ats_hugepage_size();
  void *startpg = align_pointer_backward(chunk, hpgsz);
  void *nextpg   = align_pointer_forward(static_cast<char*>(chunk) + size, hpgsz);
  size_t opsize = static_cast<char*>(nextpg) - static_cast<char*>(startpg); 

  if ( chunk != startpg )
  {
    ink_warning("huge-alloc: unaligned dalloc %lx ignored", reinterpret_cast<intptr_t>(chunk) );
    return true; // misaligned
  }

  // chunk is aligned

  // is partially-alloced last huge-page just before an unmapped one?
  if ( opsize > size && madvise(nextpg, PAGE_SIZE, MADV_NORMAL) == -1
                     && errno == ENOMEM )
  {
    // next hugepage is unmapped.. so it seems we can free the last one
    size = opsize;
  }

  if ( opsize > size ) 
  {
    ink_warning("huge-alloc: unaligned dalloc size %lx ignored", size);
    return true; // partial segment
  }

  // size will remove an integral number of hugepages

  munmap(chunk,size);
  return false;
}

// oversize-aligned use of PROT_READ|PROT_WRITE

bool huge_commit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind)
{
  const size_t hpgsz = ats_hugepage_size();
  // safely over-commit the small pages before and after [promoting use of hugepages]
  void *startpg = align_pointer_backward(static_cast<char*>(chunk) + offset, hpgsz);
  void *endpg   = align_pointer_forward(static_cast<char*>(chunk) + offset + length, hpgsz);
  size_t opsize = static_cast<char*>(endpg) - static_cast<char*>(startpg); 

  chunk = mmap(startpg, opsize, (PROT_READ|PROT_WRITE), (MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED), -1, 0);

  return !! chunk;
}

// undersize-aligned use of PROT_NONE

bool huge_decommit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind)
{
  const size_t hpgsz = ats_hugepage_size();
  // only decommit a subset of pages in the center that can be
  void *startpg = align_pointer_forward(static_cast<char*>(chunk) + offset, hpgsz);
  void *endpg   = align_pointer_backward(static_cast<char*>(chunk) + offset + length, hpgsz);
  size_t opsize = static_cast<char*>(endpg) - static_cast<char*>(startpg); 

  // nothing to do that will free any pages at all
  if ( endpg == startpg ) {
    return true;
  }

  chunk = mmap(startpg, opsize, PROT_NONE, (MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED), -1, 0);

  return !! chunk;
}

// undersize-aligned use of madvise

bool huge_purge(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind0)
{
  const size_t hpgsz = ats_hugepage_size();
  // only purge a subset of pages in the center that can be
  void *startpg = align_pointer_forward(static_cast<char*>(chunk) + offset, hpgsz);
  void *endpg   = align_pointer_backward(static_cast<char*>(chunk) + offset + length, hpgsz);
  size_t opsize = static_cast<char*>(endpg) - static_cast<char*>(startpg); 

  // nothing to do that will free any pages at all
  if ( ! opsize ) {
    return true;
  }

  return madvise(startpg, opsize, MADV_DONTNEED);
}

bool huge_split(void *chunk, size_t size, size_t size_a, size_t size_b, bool committed, unsigned arena_ind)
{
  const size_t hpgsz = ats_hugepage_size();
  void *startpg = align_pointer_backward(chunk, hpgsz);

  // pointer is aligned .. and spacing is aligned in size? okay to do it.

  if ( startpg != chunk ) {
    return true; // cannot split misaligned
  }

  if ( aligned_spacing(size_a,hpgsz) != size_a ) {
    return true; // cannot split partial pages
  }

  if ( aligned_spacing(size_b,hpgsz) != size_b ) {
    return true; // cannot split partial pages
  }

  return false; // guess it's okay!
}

bool huge_merge(void *chunk_a, size_t size_a, void *chunk_b, size_t size_b, bool committed, unsigned arena_ind)
{
  return false; // they were safely split?  then they were whole page extents
}

#endif // linux

}

#endif
