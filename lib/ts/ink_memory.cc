/** @file

  Memory allocation routines for libts

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
#include "ts/jemallctl.h"
#include "hugepages.h"

#include "ts/ink_platform.h"
#include "ts/ink_memory.h"
#include "ts/ink_defs.h"
#include "ts/ink_stack_trace.h"
#include "ts/Diags.h"
#include "ts/ink_atomic.h"

#if defined(freebsd)
#include <malloc_np.h> // for malloc_usable_size
#endif

#include <atomic>
#include <algorithm>

#include <cassert>
#if defined(linux) && !defined(_XOPEN_SOURCE)
#define _XOPEN_SOURCE 600
#endif

#include <vector>
#include <cstdlib>
#include <cstring>

#include <string>

void *
ats_malloc(size_t size)
{
  void *ptr = NULL;

  /*
   * There's some nasty code in libts that expects
   * a MALLOC of a zero-sized item to work properly. Rather
   * than allocate any space, we simply return a NULL to make
   * certain they die quickly & don't trash things.
   */

  // Useful for tracing bad mallocs
  // ink_stack_trace_dump();
  if (likely(size > 0)) {
    if (unlikely((ptr = malloc(size)) == NULL)) {
      ink_stack_trace_dump();
      ink_fatal("ats_malloc: couldn't allocate %zu bytes", size);
    }
  }
  return ptr;
} /* End ats_malloc */

void *
ats_calloc(size_t nelem, size_t elsize)
{
  void *ptr = calloc(nelem, elsize);
  if (unlikely(ptr == NULL)) {
    ink_stack_trace_dump();
    ink_fatal("ats_calloc: couldn't allocate %zu %zu byte elements", nelem, elsize);
  }
  return ptr;
} /* End ats_calloc */

void *
ats_realloc(void *ptr, size_t size)
{
  void *newptr = realloc(ptr, size);
  if (unlikely(newptr == NULL)) {
    ink_stack_trace_dump();
    ink_fatal("ats_realloc: couldn't reallocate %zu bytes", size);
  }
  return newptr;
} /* End ats_realloc */

// TODO: For Win32 platforms, we need to figure out what to do with memalign.
// The older code had ifdef's around such calls, turning them into ats_malloc().
void *
ats_memalign(size_t alignment, size_t size)
{
  void *ptr;

#if HAVE_POSIX_MEMALIGN || TS_HAS_JEMALLOC
  if (alignment <= 8)
    return ats_malloc(size);

#if defined(openbsd)
  if (alignment > PAGE_SIZE)
    alignment = PAGE_SIZE;
#endif

  int retcode = posix_memalign(&ptr, alignment, size);

  if (unlikely(retcode)) {
    if (retcode == EINVAL) {
      ink_fatal("ats_memalign: couldn't allocate %zu bytes at alignment %zu - invalid alignment parameter", size, alignment);
    } else if (retcode == ENOMEM) {
      ink_fatal("ats_memalign: couldn't allocate %zu bytes at alignment %zu - insufficient memory", size, alignment);
    } else {
      ink_fatal("ats_memalign: couldn't allocate %zu bytes at alignment %zu - unknown error %d", size, alignment, retcode);
    }
  }
#else
  ptr = memalign(alignment, size);
  if (unlikely(ptr == NULL)) {
    ink_fatal("ats_memalign: couldn't allocate %zu bytes at alignment %zu", size, alignment);
  }
#endif
  return ptr;
} /* End ats_memalign */

void
ats_free(void *ptr)
{
  if (likely(ptr != NULL))
    free(ptr);
} /* End ats_free */

void *
ats_free_null(void *ptr)
{
  if (likely(ptr != NULL))
    free(ptr);
  return NULL;
} /* End ats_free_null */

void
ats_memalign_free(void *ptr)
{
  if (likely(ptr))
    free(ptr);
}

// This effectively makes mallopt() a no-op (currently) when tcmalloc
// or jemalloc is used. This might break our usage for increasing the
// number of mmap areas (ToDo: Do we still really need that??).
//
// TODO: I think we might be able to get rid of this?
int
ats_mallopt(int param ATS_UNUSED, int value ATS_UNUSED)
{
#if HAVE_LIBJEMALLOC
  // TODO: jemalloc code ?
  return 0;
#elif TS_HAS_TCMALLOC
  // TODO: tcmalloc code ?
  return 0;
#elif defined(linux)
  return mallopt(param, value);
#else
  return 0;
#endif
}

int
ats_msync(caddr_t addr, size_t len, caddr_t end, int flags)
{
  size_t pagesize = ats_pagesize();

  // align start back to page boundary
  caddr_t a = (caddr_t)(((uintptr_t)addr) & ~(pagesize - 1));
  // align length to page boundry covering region
  size_t l = (len + (addr - a) + (pagesize - 1)) & ~(pagesize - 1);
  if ((a + l) > end)
    l = end - a; // strict limit
#if defined(linux)
/* Fix INKqa06500
   Under Linux, msync(..., MS_SYNC) calls are painfully slow, even on
   non-dirty buffers. This is true as of kernel 2.2.12. We sacrifice
   restartability under OS in order to avoid a nasty performance hit
   from a kernel global lock. */
#if 0
  // this was long long ago
  if (flags & MS_SYNC)
    flags = (flags & ~MS_SYNC) | MS_ASYNC;
#endif
#endif
  int res = msync(a, l, flags);
  return res;
}

int
ats_madvise(caddr_t addr, size_t len, int flags)
{
#if HAVE_POSIX_MADVISE
  return posix_madvise(addr, len, flags);
#else
  return madvise(addr, len, flags);
#endif
}

int
ats_mlock(caddr_t addr, size_t len)
{
  size_t pagesize = ats_pagesize();

  caddr_t a = (caddr_t)(((uintptr_t)addr) & ~(pagesize - 1));
  size_t l  = (len + (addr - a) + pagesize - 1) & ~(pagesize - 1);
  int res   = mlock(a, l);
  return res;
}

void *
ats_track_malloc(size_t size, uint64_t *stat)
{
  void *ptr = ats_malloc(size);
#ifdef HAVE_MALLOC_USABLE_SIZE
  ink_atomic_increment(stat, malloc_usable_size(ptr));
#endif
  return ptr;
}

void *
ats_track_realloc(void *ptr, size_t size, uint64_t *alloc_stat, uint64_t *free_stat)
{
#ifdef HAVE_MALLOC_USABLE_SIZE
  const size_t old_size = malloc_usable_size(ptr);
  ptr                   = ats_realloc(ptr, size);
  const size_t new_size = malloc_usable_size(ptr);
  if (old_size < new_size) {
    // allocating something bigger
    ink_atomic_increment(alloc_stat, new_size - old_size);
  } else if (old_size > new_size) {
    ink_atomic_increment(free_stat, old_size - new_size);
  }
  return ptr;
#else
  return ats_realloc(ptr, size);
#endif
}

void
ats_track_free(void *ptr, uint64_t *stat)
{
  if (ptr == NULL) {
    return;
  }

#ifdef HAVE_MALLOC_USABLE_SIZE
  ink_atomic_increment(stat, malloc_usable_size(ptr));
#endif
  ats_free(ptr);
}

/*-------------------------------------------------------------------------
  Moved from old ink_resource.h
  -------------------------------------------------------------------------*/
char *
_xstrdup(const char *str, int length, const char * /* path ATS_UNUSED */)
{
  char *newstr;

  if (likely(str)) {
    if (length < 0)
      length = strlen(str);

    newstr = (char *)ats_malloc(length + 1);
    // If this is a zero length string just null terminate and return.
    if (unlikely(length == 0)) {
      *newstr = '\0';
    } else {
      strncpy(newstr, str, length); // we cannot do length + 1 because the string isn't
      newstr[length] = '\0';        // guaranteeed to be null terminated!
    }
    return newstr;
  }
  return NULL;
}

void *
ats_alloc_stack(size_t stacksize)
{
  // get memory that grows down and is not populated until needed
  //    [but prefer hugepage alignment and request if possible]
  return ats_hugepage_enabled() ? ats_alloc_hugepage_stack(stacksize) : mmap(nullptr, stacksize, PROT_READ | PROT_WRITE,
                                                                             MAP_ANONYMOUS | MAP_GROWSDOWN | MAP_PRIVATE, -1, 0);
}

#if !TS_USE_HWLOC
using CpuSetVector_t = std::vector<void *>;
#else
using CpuSetVector_t  = std::vector<hwloc_const_cpuset_t>;
using NodeSetVector_t = std::vector<hwloc_const_nodeset_t>;
using NodesIDVector_t = std::vector<unsigned>;
using ArenaIDVector_t = std::vector<unsigned>;

static auto const kHwlocBitmapDeleter = [](hwloc_bitmap_t map) { map ? hwloc_bitmap_free(map) : (void)0; };

struct hwloc_bitmap : public std::unique_ptr<hwloc_bitmap_s, decltype(kHwlocBitmapDeleter)> {
  using super = std::unique_ptr<hwloc_bitmap_s, decltype(kHwlocBitmapDeleter)>;
  hwloc_bitmap() : super{hwloc_bitmap_alloc(), kHwlocBitmapDeleter} {}
  hwloc_bitmap(hwloc_const_bitmap_t map) : super{hwloc_bitmap_dup(map), kHwlocBitmapDeleter} {}
  operator hwloc_const_bitmap_t() const { return get(); }
  operator hwloc_bitmap_t() { return get(); }
};

////////////////////////////////// namespace numa
namespace numa
{
extern hwloc_const_cpuset_t const kCpusAllowed;
extern NodeSetVector_t const kUniqueNodeSets;
extern NodeSetVector_t g_nodesByArena;
extern ArenaIDVector_t g_arenaByNodesID;

hwloc_const_cpuset_t get_cpuset_by_affinity(hwloc_obj_type_t objtype, unsigned affid);
NodesIDVector_t::value_type get_nodes_id_by_affinity(hwloc_obj_type_t objtype, unsigned affid);

ArenaIDVector_t::value_type
get_arena_by_affinity(hwloc_obj_type_t objtype, unsigned affid)
{
  auto nsid = get_nodes_id_by_affinity(objtype, affid);

  if (!nsid) {
    return 0;
  }

  if (g_arenaByNodesID.size() >= kUniqueNodeSets.size() && g_arenaByNodesID[nsid]) {
    return g_arenaByNodesID[nsid];
  }

  ink_release_assert(kUniqueNodeSets.size() > nsid);

  static ink_mutex s_mutex = PTHREAD_MUTEX_INITIALIZER;

  ink_mutex_acquire(&s_mutex);

  g_arenaByNodesID.resize(kUniqueNodeSets.size());

  if (g_arenaByNodesID[nsid]) {
    return g_arenaByNodesID[nsid];
  }

  // need a new arena for this set of nodes

  unsigned newArena = jemallctl::do_arenas_extend();

  Debug("memory", "extending arena to %u", newArena);

  int callerArena = jemallctl::thread_arena(); // push current arena (for a moment)

  jemallctl::set_thread_arena(newArena);
  jemallctl::set_thread_arena_hooks(jemallctl::get_hugepage_hooks()); // init hooks for hugepage

  // re-use caller's arena for allocations
  jemallctl::set_thread_arena(callerArena);

  // store the node-set that this arena is going to partition off
  if (g_nodesByArena.size() < newArena + 1) {
    g_nodesByArena.resize(newArena + 1); // filled with nullptr if needed
  }

  g_nodesByArena[newArena] = hwloc_bitmap{kUniqueNodeSets[nsid]}.release();
  g_arenaByNodesID[nsid]   = newArena;

  ink_mutex_release(&s_mutex);

  return newArena; // affid/cpuset now leads to this arena
}

unsigned
new_affinity_id()
{
  static std::atomic_uint g_affinityId{1}; // zero is 'unset'
  return ++g_affinityId;
}

bool
is_same_thread_memory_affinity(hwloc_obj_type_t objtype, unsigned affid)
{
  return get_arena_by_affinity(objtype, affid) == jemallctl::thread_arena();
}

int assign_thread_memory_by_affinity(hwloc_obj_type_t objtype, unsigned affid) // limit new pages to specific nodes
{
  // keep using old arena for a moment...

  auto arena = get_arena_by_affinity(objtype, affid);

  if (arena >= g_nodesByArena.size()) {
    return -1;
  }

  auto nodes = g_nodesByArena[arena];

  if (!nodes || hwloc_bitmap_iszero(nodes)) {
    return -1;
  }

  // only get new pages from this nodeset ... (all if arena == 0)
  auto r = hwloc_set_membind_nodeset(curr(), nodes, HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);
  if (r) {
    return -1;
  }

  // thread-wide change in place
  jemallctl::set_thread_arena(arena); // make it active now
  return 0;
}

int assign_thread_cpuset_by_affinity(hwloc_obj_type_t objtype, unsigned affid) // limit usable cpus to specific cpuset
{
  return hwloc_set_cpubind(curr(), get_cpuset_by_affinity(objtype, affid), HWLOC_CPUBIND_STRICT);
}

static void reorder_interleaved(CpuSetVector_t const &supers, CpuSetVector_t &subs);

//
// produce a list of cpusets associated with the object passed
//
auto
get_obj_cpusets(hwloc_obj_type_t objtype, CpuSetVector_t const &supers = CpuSetVector_t{}) -> CpuSetVector_t
{
  auto n = hwloc_get_nbobjs_by_type(curr(), objtype);

  // is there no partition at all?
  if (n < 2) {
    return std::move(CpuSetVector_t({kCpusAllowed})); // one set.. of all cpus
  }

  CpuSetVector_t sets;

  while (n--) {
    hwloc_obj_t obj = hwloc_get_obj_by_type(curr(), objtype, n);
    sets.emplace_back(obj ? obj->cpuset : kCpusAllowed);
  }

  // reorder any neighbors to interleave between superset-matching cpusets

  if (sets.size() >= 2 && supers.size() >= 2) {
    reorder_interleaved(supers, sets);
  }

  return std::move(sets);
}

//
// use newVect cpusets and NUMA nodesets (may require new arenas)
//
auto
cpusets_to_nodes_id(const NodeSetVector_t &uniqueSets, CpuSetVector_t const &newVect) -> NodesIDVector_t
{
  // no differences for all threads?
  if (newVect.size() <= 1) {
    return std::move(NodesIDVector_t(1)); ///// RETURN (default single)
  }

  // same NUMA node for all affinities?
  if (hwloc_get_nbobjs_by_type(curr(), HWLOC_OBJ_NUMANODE) < 2) {
    return std::move(NodesIDVector_t(newVect.size())); ///// RETURN (defaulted)
  }

  // may create new nodes-id if nodesets are different

  hwloc_bitmap nodeset;

  NodesIDVector_t nodesMap;

  for (auto &&cpuset : newVect) {
    auto equalNodesChk = [cpuset, &nodeset](hwloc_const_bitmap_t knownset) {
      hwloc_cpuset_to_nodeset(curr(), cpuset, nodeset);
      return hwloc_bitmap_isequal(nodeset, knownset);
    };

    // rend() - [rbegin()/rend()] gives [size(),1] .. and 0 only if nothing found
    unsigned i = uniqueSets.rend() - std::find_if(uniqueSets.rbegin(), uniqueSets.rend(), equalNodesChk);

    // found a equal-nodes match?  chg i to nodes-index.
    if (!i--) {
      i = uniqueSets.size(); // new index to use
      const_cast<NodeSetVector_t &>(uniqueSets).push_back(hwloc_bitmap{nodeset.get()}.release());
    }

    nodesMap.push_back(i); // affid-cpuset now matches this nodes ID
  }
  return std::move(nodesMap);
}

static inline auto
find_superset_bitmap(CpuSetVector_t const &supers, CpuSetVector_t::const_iterator const &hint, hwloc_const_bitmap_t sub)
  -> CpuSetVector_t::const_iterator
{
  auto IfSuperset = [&](hwloc_const_bitmap_t super) { return hwloc_bitmap_isincluded(sub, super); };

  CpuSetVector_t::const_iterator sup = std::find_if(hint, supers.end(), IfSuperset);

  if (sup == supers.end()) {
    sup = std::find_if(supers.begin(), hint, IfSuperset);
  }

  return sup; // may be equal to hint
}

static inline auto
find_first_non_overlap_bitmap(CpuSetVector_t::iterator begin, CpuSetVector_t::iterator end, hwloc_const_bitmap_t super)
  -> CpuSetVector_t::iterator
{
  return std::find_if(begin, end, [&](hwloc_const_bitmap_t sub) { return !hwloc_bitmap_isincluded(sub, super); });
}

static inline auto
find_first_non_subset_bitmap(CpuSetVector_t::iterator begin, CpuSetVector_t::iterator end, hwloc_const_bitmap_t super)
  -> CpuSetVector_t::iterator
{
  return std::find_if(begin, end, [&](hwloc_const_bitmap_t sub) { return !hwloc_bitmap_isincluded(sub, super); });
}

static void
reorder_interleaved(CpuSetVector_t const &supers, CpuSetVector_t &subs)
{
  // use rotating search-window
  auto adjSuper = supers.begin();

  // remove superset-matching neighbors

  // get original
  auto adjSet = subs.begin();

  // needs to work with begin+1 to last-1 [can swap with begin+2 to last]
  for (auto set = adjSet + 1; set + 1 != subs.end(); (adjSet = set), ++set) {
    adjSuper = find_superset_bitmap(supers, adjSuper, *adjSet);

    auto swapFrom = set;
    // first swap in the an *entirely* adjSuper-external cpuset,
    //    or (failing that) the a partly adjSuper-external cpuset
    if ((swapFrom = find_first_non_overlap_bitmap(set, subs.end(), *adjSuper)) == subs.end() &&
        (swapFrom = find_first_non_subset_bitmap(set, subs.end(), *adjSuper)) == subs.end()) {
      continue; // give up if no cpus used outside this super-obj cpuset
    }

    // some neighbor has a different super-obj cpuset

    if (swapFrom != set) {
      std::swap(*set, *swapFrom);
    }
  }
}

hwloc_const_cpuset_t const kCpusAllowed   = hwloc_topology_get_allowed_cpuset(curr());
hwloc_const_nodeset_t const kNodesAllowed = hwloc_topology_get_allowed_nodeset(curr());

CpuSetVector_t const kCPUSets = CpuSetVector_t{kCpusAllowed}; // valid base cpuset

CpuSetVector_t const kNumaCPUSets = get_obj_cpusets(HWLOC_OBJ_NUMANODE); // cpusets for each memory node
CpuSetVector_t const kSocketCPUSets =
  get_obj_cpusets(HWLOC_OBJ_SOCKET, kNumaCPUSets); // cpusets for each socket, in alternated order of memory nodes
CpuSetVector_t const kCoreCPUSets =
  get_obj_cpusets(HWLOC_OBJ_CORE, kNumaCPUSets); // cpusets for each core, in alternated order of memory nodes
CpuSetVector_t const kProcCPUSets =
  get_obj_cpusets(HWLOC_OBJ_PU, kNumaCPUSets); // cpusets for each processor-unit, in alternated order of memory nodes

hwloc_const_cpuset_t
get_cpuset_by_affinity(hwloc_obj_type_t objtype, unsigned affid)
{
  switch (objtype) {
  case HWLOC_OBJ_NUMANODE:
    return kNumaCPUSets[affid % kNumaCPUSets.size()];
  case HWLOC_OBJ_SOCKET:
    return kSocketCPUSets[affid % kSocketCPUSets.size()];
  case HWLOC_OBJ_CORE:
    return kCoreCPUSets[affid % kCoreCPUSets.size()];
  case HWLOC_OBJ_PU:
    return kProcCPUSets[affid % kProcCPUSets.size()];
  default:
    break;
  }

  return kCPUSets.front();
}

// unique nodesets
NodeSetVector_t const kUniqueNodeSets = {kNodesAllowed};
// unique nodeset index mapping to arenas
ArenaIDVector_t g_arenaByNodesID = {0}; // lookup with same index as kUniqueNodeSets

// arena indexed map to actual nodeset [pointers]
NodeSetVector_t g_nodesByArena = {kNodesAllowed}; // lookup with same index as Arena id

// lists of indexes into kUniqueNodeSets and
NodesIDVector_t const kNumaAffNodes   = cpusets_to_nodes_id(kUniqueNodeSets, kNumaCPUSets);
NodesIDVector_t const kSocketAffNodes = cpusets_to_nodes_id(kUniqueNodeSets, kSocketCPUSets);
NodesIDVector_t const kCoreAffNodes   = cpusets_to_nodes_id(kUniqueNodeSets, kCoreCPUSets);
NodesIDVector_t const kProcAffNodes   = cpusets_to_nodes_id(kUniqueNodeSets, kProcCPUSets);

unsigned
get_nodes_id_by_affinity(hwloc_obj_type_t objtype, unsigned affid)
{
  switch (objtype) {
  case HWLOC_OBJ_NUMANODE:
    return kNumaAffNodes[affid % kNumaAffNodes.size()];
  case HWLOC_OBJ_SOCKET:
    return kSocketAffNodes[affid % kSocketAffNodes.size()];
  case HWLOC_OBJ_CORE:
    return kCoreAffNodes[affid % kCoreAffNodes.size()];
  case HWLOC_OBJ_PU:
    return kProcAffNodes[affid % kProcAffNodes.size()];
  default:
    break;
  }

  return 0;
}

void reset_thread_memory_by_cpuset() // limit new pages to specific nodes as the cpu was set (earlier)
{
  // there aren't any choices?
  if (kUniqueNodeSets.size() < 2) {
    auto r = assign_thread_memory_by_affinity(HWLOC_OBJ_MACHINE, 0); // set to default arena
    ink_release_assert(!r);
    return; // simple
  }

  hwloc_bitmap cpuset;

  auto r = hwloc_get_cpubind(curr(), cpuset, HWLOC_CPUBIND_THREAD);
  ink_release_assert(!r);

  // search for matches to earlier memory-nodesets
  //
  auto list = cpusets_to_nodes_id(kUniqueNodeSets, CpuSetVector_t{cpuset.get()});
  if (list.empty() || list.front() >= g_arenaByNodesID.size()) {
    r = assign_thread_memory_by_affinity(HWLOC_OBJ_MACHINE, 0); // set to default arena
    ink_release_assert(!r);
    return;
  }

  // reset limited nodes to use
  hwloc_set_membind_nodeset(curr(), kUniqueNodeSets[list.front()], HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);
  // assign arena that matches
  jemallctl::set_thread_arena(g_arenaByNodesID[list.front()]);
}

} // namespace numa

#endif
