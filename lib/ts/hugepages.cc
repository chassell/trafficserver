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
#include "hugepages.h"

#include "ts/Diags.h"
#include "ink_memory.h"

#include <sys/mman.h>

#define DEBUG_TAG "hugepages"

#undef Debug
#define Debug(a,b ...)

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
#define HUGEPAGESIZE_TOKEN_SIZE ( countof(HUGEPAGESIZE_TOKEN)-1 )

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

  hugepage_size = PAGE_SIZE;

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

bool huge_commit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);
bool huge_decommit(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);
bool huge_purge(void *chunk, size_t size, size_t offset, size_t length, unsigned arena_ind);

#if defined(linux)

//
// perform allocation or over-allocation of a hugepage at a valid point (big enough) in memory
//    if a point of memory is re-allocated larger, return immediately if it fits within the hugepage
//

void *huge_alloc_and_madvise(void *chunk, size_t csize, size_t hpgsz, 
                              bool *zero, bool *commit, unsigned madvflags, unsigned arena_ind)
{
  int r = 0;
  // NOTE: if chunk was NULL then it is hugepage-identical and startpg is NULL too
  void *nxtchunk = static_cast<char*>(chunk) + csize;
  void *startpg = align_pointer_backward(chunk,hpgsz);
  void *nextpg = align_pointer_forward(nxtchunk, hpgsz);
  size_t opsize = static_cast<char*>(nextpg) - static_cast<char*>(startpg); 

  bool fakecommit = false; // assume false if nullptr is passed
  bool fakezero = false; // assume false if nullptr is passed

  zero = ( zero ? : &fakezero );
  commit = ( commit ? : &fakecommit );

  Debug(DEBUG_TAG,"%p %#lx @%#lx %d/%d arena=%d",chunk,csize,hpgsz,*zero,*commit,arena_ind);

  // is this getting a totally new segment?
  if ( ! startpg ) 
  {
    // create oversized new segment (empty) first (oversize by full hugepage so end always has >=1 unmapped page)
    chunk = mmap(nullptr, opsize + hpgsz, PROT_NONE, (MAP_PRIVATE|MAP_ANONYMOUS), -1, 0);
    nxtchunk = static_cast<char*>(chunk) + opsize + hpgsz; // past the end of request
    Debug(DEBUG_TAG,"noprot: %p - %p : n=%lu",chunk,nxtchunk,
                     ( static_cast<char*>(nxtchunk) - static_cast<char*>(chunk) ) / PAGE_SIZE );

    if ( ! chunk ) {
      return nullptr; // could not get unmapped open segment!                           //// ERROR alloc-mmap fail
    }

    startpg = align_pointer_forward(chunk,hpgsz); // final new mapping start
    nextpg = static_cast<char*>(startpg) + opsize;  // final new mapping end
    Debug(DEBUG_TAG,"aligned: %p -> %p %p",chunk,startpg,nextpg);

    // finally, commit the mapping with usable hugepages 
    //    (or with small pages until they are replaced)
    if ( ( *commit || *zero ) && huge_commit(startpg,opsize,0,opsize,arena_ind) == true ) 
    {
      r = munmap(chunk,opsize + hpgsz); // undo prev. mmap
      ink_release_assert( ! r );
      return nullptr; // could not commit this many pages!                             //// ERROR alloc-commit fail
    }

    // clean up excess edges
    if ( startpg != chunk ) 
    {
      r = munmap(chunk, static_cast<char*>(startpg) - static_cast<char*>(chunk));
      Debug(DEBUG_TAG,"munmap-front: %p - %p : n=%lu",chunk,startpg,
                     ( static_cast<char*>(startpg) - static_cast<char*>(chunk) ) / PAGE_SIZE );
      ink_release_assert( ! r );
    }

    if ( nxtchunk != nextpg ) 
    {
      r = munmap(nextpg, static_cast<char*>(nxtchunk) - static_cast<char*>(nextpg));
      Debug(DEBUG_TAG,"munmap-tail: %p - %p : n=%lu",nextpg,nxtchunk,
                     ( static_cast<char*>(nxtchunk) - static_cast<char*>(nextpg)) / PAGE_SIZE );
      ink_release_assert( ! r );
    }

    // if we've committed .. we also zeroed (and opposite too)
    *zero = *commit = ( *commit || *zero );

    // successfully created map 

    r = madvise(startpg,opsize,MADV_HUGEPAGE);
    ink_release_assert( ! r );
    if ( madvflags ) {
      r = madvise(startpg,opsize,madvflags);
      ink_release_assert( ! r );
    }
    Debug(DEBUG_TAG,"return: %lx -> %p ",csize,startpg);
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
    if ( huge_decommit(startpg,opsize,0,opsize,arena_ind) == true ) { 
      return nullptr; // mmap with prot-none failed                                   //// ERROR realloc-decommit fail
    }
    // apply flags over entire map range owned
    r = madvise(startpg,opsize,MADV_HUGEPAGE); 
    ink_release_assert( ! r );
    if ( madvflags ) {
      r = madvise(startpg,opsize,madvflags);
      ink_release_assert( ! r );
    }
    Debug(DEBUG_TAG,"realloc-empty: %p %lx",chunk,csize);
    return chunk; // realloc is completed with no-zero/no-commit permissions          //// SUCCESS realloc-emptied
  }

  // will assert open protections for new realloc range

  // commit to writable memory allowed for new pages and old
  Debug(DEBUG_TAG,"realloc-commit: %p %lx",startpg,csize);
  if ( huge_commit(startpg,opsize,0,opsize,arena_ind) == true ) {
    return nullptr; // cannot realloc this segment safely                             //// ERROR realloc-commit fail
  }

  // memory is writable and committed by OS

  *commit = true;
  if ( *zero ) {
    r = madvise(startpg,opsize,MADV_DONTNEED);
  }

  // apply flags to the new map remaining

  if ( chunk == startpg ) // allow passed flags for realloc'ed hugepages?
  {
    Debug(DEBUG_TAG,"realloc-madvflags: %p %lx",startpg,opsize);
    r = madvise(startpg, opsize, MADV_HUGEPAGE); 
    ink_release_assert( ! r );
    if ( madvflags ) {
      r = madvise(startpg, opsize, madvflags); 
      ink_release_assert( ! r );
    }
  } 
  else if ( *zero ) // add safest flags and attempt partial purge to realloc'ed hugepage?
  {
    Debug(DEBUG_TAG,"realloc-purge: %p %lx",chunk,csize);
    huge_purge(chunk,csize,0,csize,arena_ind);
    r = madvise(startpg, opsize, MADV_HUGEPAGE); 
    ink_release_assert( ! r );
  } 
  else // just add safest flags to realloc'ed hugepage?
  {
    Debug(DEBUG_TAG,"realloc-nomadvflags: %p %lx",startpg,opsize);
    r = madvise(startpg, opsize, MADV_HUGEPAGE); // add safe flags to unaligned hugepages
    ink_release_assert( ! r );
  }

  Debug(DEBUG_TAG,"realloc return: %p %lx",chunk,csize);
  return chunk; // realloc is complete                                                //// SUCCESS realloc-ready
}

//
// all page allocation and advise for normal memory use
//

void *huge_normal_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind)
{
  size_t hpgsize = std::max(sizeof(MemoryPageHuge),alignment);
  return huge_alloc_and_madvise(chunk, size, hpgsize, zero, commit, MADV_NORMAL, arena_ind);
}

//
// all page allocation and advise for cache-storage memory use
//

void *huge_nodump_alloc(void *chunk, size_t size, size_t alignment, bool *zero, bool *commit, unsigned arena_ind)
{
  size_t hpgsize = std::max(sizeof(MemoryPageHuge),alignment);
  return huge_alloc_and_madvise(chunk, size, hpgsize, zero, commit, MADV_DONTDUMP, arena_ind);
}

//
// perform de-allocation of integer numbers of hugepages (only) and reject all else
//    FIXME: this may prevent *ever* releasing large swaths of pages
//

bool huge_dalloc(void *chunk, size_t size, bool committed, unsigned)
{
  int r = 0;
  Debug(DEBUG_TAG,"%p %lx",chunk,size);
  const size_t hpgsz = sizeof(MemoryPageHuge);
  void *startpg = align_pointer_backward(chunk, hpgsz);
  void *nextpg   = align_pointer_forward(static_cast<char*>(chunk) + size, hpgsz);
  size_t opsize = static_cast<char*>(nextpg) - static_cast<char*>(startpg); 

  if ( chunk != startpg )
  {
    ink_warning("huge-dealloc: unaligned dalloc %lx ignored", reinterpret_cast<intptr_t>(chunk) );
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
    ink_warning("huge-dealloc: unaligned dalloc size %lx ignored", size);
    return true; // partial segment
  }

  // size will remove an integral number of hugepages

  // NOTE: no concern about NUMA for unmap
  Debug(DEBUG_TAG,"munmap-full: %p %#lx",chunk,size);
  r = munmap(chunk,size); 
  ink_release_assert( ! r );
  return false;
}

// oversize-aligned use of PROT_READ|PROT_WRITE

bool huge_commit(void *chunk, size_t size, size_t offset, size_t length, unsigned)
{
//  Debug(DEBUG_TAG,"huge-commit: %p %#lx %#lx %#lx",chunk,size,offset,length);
  const size_t hpgsz = sizeof(MemoryPageHuge);
  // safely over-commit the small pages before and after [promoting use of hugepages]
  void *startpg = align_pointer_backward(static_cast<char*>(chunk) + offset, hpgsz);
  void *endpg   = align_pointer_forward(static_cast<char*>(chunk) + offset + length, hpgsz);
  size_t opsize = static_cast<char*>(endpg) - static_cast<char*>(startpg); 

  // NOTE: no concern about NUMA for unmap
//  Debug(DEBUG_TAG,"huge-mmap-commit: %p - %p",startpg,endpg);
  return mprotect(startpg, opsize, PROT_READ|PROT_WRITE);
}

// undersize-aligned use of PROT_NONE

bool huge_decommit(void *chunk, size_t size, size_t offset, size_t length, unsigned)
{
  Debug(DEBUG_TAG,"%p %#lx %#lx %#lx",chunk,size,offset,length);
  const size_t hpgsz = sizeof(MemoryPageHuge);
  // only decommit a subset of pages in the center that can be
  void *startpg = align_pointer_forward(static_cast<char*>(chunk) + offset, hpgsz);
  void *endpg   = align_pointer_backward(static_cast<char*>(chunk) + offset + length, hpgsz);
  size_t opsize = static_cast<char*>(endpg) - static_cast<char*>(startpg); 

  // nothing to do that will free any pages at all
  if ( endpg == startpg ) {
    return true; // return failure
  }

  Debug(DEBUG_TAG,"full: %p - %p",startpg,endpg);
  return mprotect(startpg, opsize, PROT_NONE);
}

// undersize-aligned use of madvise

bool huge_purge(void *chunk, size_t size, size_t offset, size_t length, unsigned)
{
  Debug(DEBUG_TAG,"%p %#lx %#lx %#lx",chunk,size,offset,length);
  const size_t hpgsz = sizeof(MemoryPageHuge);
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

bool huge_split(void *chunk, size_t size, size_t size_a, size_t size_b, bool committed, unsigned)
{
  Debug(DEBUG_TAG,"%p %#lx a=%#lx b=%#lx",chunk,size,size_a,size_b);
  const size_t hpgsz = sizeof(MemoryPageHuge);
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

  Debug(DEBUG_TAG,"success %p %#lx a=%#lx b=%#lx",chunk,size,size_a,size_b);
  return false; // guess it's okay!
}

bool huge_merge(void *chunk_a, size_t size_a, void *chunk_b, size_t size_b, bool committed, unsigned)
{
  Debug(DEBUG_TAG,"%p %p a=%#lx b=%#lx",chunk_a,chunk_b,size_a,size_b);
  return false; // they were safely split before?  always successful
}

void *
ats_alloc_hugepage_stack(size_t stacksize)
{
  Debug(DEBUG_TAG,"#%lu",stacksize);

  // get aligned mapping, unpopulated and madvise for hugepage
  void *alignedMapping = huge_normal_alloc(nullptr, stacksize, 0, nullptr, nullptr, 0);
  Debug(DEBUG_TAG,"%p - %p",alignedMapping, static_cast<char *>(alignedMapping)+stacksize);

  // re-map it .. but with the grows-down flag now
  auto r = mmap(alignedMapping, stacksize, PROT_READ|PROT_WRITE, (MAP_PRIVATE|MAP_ANONYMOUS|MAP_FIXED|MAP_GROWSDOWN), -1, 0);
  ink_release_assert( r == alignedMapping );

  auto rr = madvise(alignedMapping, stacksize,MADV_HUGEPAGE); // re-apply flags to the new map remaining
  ink_release_assert( ! rr );

  return alignedMapping;
}

#endif // linux

#endif
