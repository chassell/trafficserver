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
#include "ts/ink_platform.h"
#include "ts/ink_memory.h"
#include "ts/ink_defs.h"
#include "ts/ink_stack_trace.h"
#include "ts/Diags.h"
#include "ts/ink_atomic.h"

#if defined(freebsd)
#include <malloc_np.h> // for malloc_usable_size
#endif

#include <cassert>
#if defined(linux) && ! defined(_XOPEN_SOURCE)
#define _XOPEN_SOURCE 600
#endif

#include <vector>
#include <cstdlib>
#include <cstring>

#include <string>

void *
ats_malloc(size_t size)
{
  void *ptr = nullptr;

  /*
   * There's some nasty code in libts that expects
   * a MALLOC of a zero-sized item to work properly. Rather
   * than allocate any space, we simply return a nullptr to make
   * certain they die quickly & don't trash things.
   */

  // Useful for tracing bad mallocs
  // ink_stack_trace_dump();
  if (likely(size > 0)) {
    if (unlikely((ptr = malloc(size)) == nullptr)) {
      ink_abort("couldn't allocate %zu bytes", size);
    }
  }
  return ptr;
} /* End ats_malloc */

void *
ats_calloc(size_t nelem, size_t elsize)
{
  void *ptr = calloc(nelem, elsize);
  if (unlikely(ptr == nullptr)) {
    ink_abort("couldn't allocate %zu %zu byte elements", nelem, elsize);
  }
  return ptr;
} /* End ats_calloc */

void *
ats_realloc(void *ptr, size_t size)
{
  void *newptr = realloc(ptr, size);
  if (unlikely(newptr == nullptr)) {
    ink_abort("couldn't reallocate %zu bytes", size);
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
  if (alignment <= 8) {
    return ats_malloc(size);
  }

#if defined(openbsd)
  if (alignment > PAGE_SIZE)
    alignment = PAGE_SIZE;
#endif

  int retcode = posix_memalign(&ptr, alignment, size);

  if (unlikely(retcode)) {
    if (retcode == EINVAL) {
      ink_abort("couldn't allocate %zu bytes at alignment %zu - invalid alignment parameter", size, alignment);
    } else if (retcode == ENOMEM) {
      ink_abort("couldn't allocate %zu bytes at alignment %zu - insufficient memory", size, alignment);
    } else {
      ink_abort("couldn't allocate %zu bytes at alignment %zu - unknown error %d", size, alignment, retcode);
    }
  }
#else
  ptr = memalign(alignment, size);
  if (unlikely(ptr == nullptr)) {
    ink_abort("couldn't allocate %zu bytes at alignment %zu", size, alignment);
  }
#endif
  return ptr;
} /* End ats_memalign */

void
ats_free(void *ptr)
{
  if (likely(ptr != nullptr)) {
    free(ptr);
  }
} /* End ats_free */

void *
ats_free_null(void *ptr)
{
  if (likely(ptr != nullptr)) {
    free(ptr);
  }
  return nullptr;
} /* End ats_free_null */

void
ats_memalign_free(void *ptr)
{
  if (likely(ptr)) {
    free(ptr);
  }
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
  if ((a + l) > end) {
    l = end - a; // strict limit
  }
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
  if (ptr == nullptr) {
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
    if (length < 0) {
      length = strlen(str);
    }

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
  return nullptr;
}

#if TS_USE_HWLOC
auto get_cpu_sets(int afftype) -> std::vector<hwloc_const_cpuset_t>;
auto create_numa_arenas(unsigned) -> std::vector<unsigned>;

static const auto kNumaCPUSets = get_cpu_sets(HWLOC_OBJ_NUMANODE);

static const auto kNumaArenas = create_numa_arenas(kNumaCPUSets.size());
#endif

#if HAVE_LIBJEMALLOC
namespace jemallctl {
#if HAVE_LIBJEMALLOC
static const auto kNumaArenas = create_numa_arenas(kNumaCPUSets.size());
#endif

// int hwloc_bitmap_intersects(a,b)
// int hwloc_bitmap_isincluded(le,ge)

static const auto kCpus_ = hwloc_topology_get_topology_cpuset( ink_get_topology() );
static const auto kCpusCnt_ = hwloc_bitmap_weight( kCpus_ );
static const auto kCpusAllowed_ = hwloc_topology_get_allowed_cpuset( ink_get_topology() );

static const auto kNumas_ = hwloc_topology_get_topology_nodeset( ink_get_topology() );
static const auto kNumasCnt_ = hwloc_bitmap_weight( kNumas_ );

// internal read/write functions

using objpath_t = std::vector<size_t>;

int mallctl_void(const objpath_t &oid);

template <typename T_VALUE>
auto mallctl_get(const objpath_t &oid) -> T_VALUE;

template <typename T_VALUE>
auto mallctl_set(const objpath_t &oid, T_VALUE &v) -> int;

// define object functors (to allow instances below)

objpath_t objpath(const std::string &path);

struct ObjBase {
   ObjBase(const char *name) : oid_{objpath(name)} 
      { }
 protected:
   const objpath_t oid_;
};

template <typename T_VALUE, size_t N_DIFF=0>
struct GetObjFxn : public ObjBase 
  { using ObjBase::ObjBase; auto operator()(void) const -> T_VALUE; };

template <typename T_VALUE, size_t N_DIFF=0>
struct SetObjFxn : public ObjBase 
  { using ObjBase::ObjBase; auto operator()(T_VALUE &) const -> int; };

using DoObjFxn = GetObjFxn<void,0>;

template <typename T_VALUE, size_t N_DIFF> auto 
 GetObjFxn<T_VALUE,N_DIFF>::operator()(void) const -> T_VALUE
   { return std::move( jemallctl::mallctl_get<T_VALUE>(ObjBase::oid_)); } 

template <typename T_VALUE, size_t N_DIFF> auto 
 SetObjFxn<T_VALUE,N_DIFF>::operator()(T_VALUE &v) const -> int 
   { return mallctl_set(ObjBase::oid_,v); } 

template <>
struct GetObjFxn<void,0> : public ObjBase
  {
    using ObjBase::ObjBase;
    int operator()(void) const 
       { return jemallctl::mallctl_void(ObjBase::oid_); } 
  };

const GetObjFxn<chunk_hooks_t> thread_arena_hooks{"arena.0.chunk_hooks"};
const SetObjFxn<chunk_hooks_t> set_thread_arena_hooks{"arena.0.chunk_hooks"};

// request-or-sense new values in statistics 
const GetObjFxn<uint64_t>         epoch{"epoch"};

// request separated page sets for each NUMA node (when created)
const GetObjFxn<unsigned>         do_arenas_extend{"arenas.extend"}; // unsigned r-

// assigned arena for local thread
const GetObjFxn<unsigned>         thread_arena{"thread.arena"}; // unsigned rw
const SetObjFxn<unsigned>         set_thread_arena{"thread.arena"}; // unsigned rw
const DoObjFxn                    do_thread_tcache_flush{"thread.tcache.flush"};

const GetObjFxn<bool>             config_thp{"config.thp"};
const GetObjFxn<std::string>      config_malloc_conf{"config.malloc_conf"};

const GetObjFxn<std::string>      thread_prof_name{"thread.prof.name"};
const SetObjFxn<std::string>      set_thread_prof_name{"thread.prof.name"};

const GetObjFxn<bool>             thread_prof_active{"thread.prof.active"};
const SetObjFxn<bool>             set_thread_prof_active{"thread.prof.active"};

} // namespace jemallctl



/// implementation of simple oid-translating call

jemallctl::objpath_t jemallctl::objpath(const std::string &path)
{
  objpath_t oid{10}; // longer than any oid target
  size_t len = oid.size();
  mallctlnametomib(path.c_str(), oid.data(), &len);
  oid.resize(len);
  return std::move(oid);
}

/// implementations for ObjBase

auto jemallctl::
   mallctl_void(const objpath_t &oid) -> int
{
  return mallctlbymib(oid.data(),oid.size(),nullptr,nullptr,nullptr,0);
}

/// template implementations for ObjBase
//
template <typename T_VALUE> auto jemallctl::
   mallctl_set(const objpath_t &oid, T_VALUE &v) -> int
{
  return mallctlbymib(oid.data(),oid.size(),nullptr,nullptr,&v,sizeof(v));
}

template <typename T_VALUE> auto jemallctl::
   mallctl_get(const objpath_t &oid) -> T_VALUE
{
  T_VALUE v{}; // init to zero if a pod type
  size_t len = sizeof(v);
  mallctlbymib(oid.data(),oid.size(),&v,&len,nullptr,0);
  return std::move(v);
}

////////////////////////////////// namespace jemallctl
namespace jemallctl {

template <> auto mallctl_get<std::string>(const objpath_t &oid) -> std::string
{
  char buff[256]; // adequate for paths and most things
  size_t len = sizeof(buff);
  mallctlbymib(oid.data(),oid.size(),&buff,&len,nullptr,0);
  std::string v(&buff[0],len); // copy out
  return std::move(v);
}

template <> auto mallctl_set<std::string>(const objpath_t &oid, std::string &v) -> int
{
  return mallctlbymib(oid.data(),oid.size(),nullptr,nullptr,const_cast<char*>(v.c_str()),v.size());
}

template <> auto mallctl_get<chunk_hooks_t>(const objpath_t &baseOid) -> chunk_hooks_t
{
   objpath_t oid = baseOid;
   oid[1] = ::jemallctl::thread_arena();
   return mallctl_get<chunk_hooks_t>(oid);
}

template <> auto mallctl_set<chunk_hooks_t>(const objpath_t &baseOid, chunk_hooks_t &hooks) -> int
{
   objpath_t oid = baseOid;
   oid[1] = ::jemallctl::thread_arena();
   auto ohooks = mallctl_get<chunk_hooks_t>(oid);
   auto nhooks = chunk_hooks_t {
                    ( hooks.alloc ? : ohooks.alloc ),
                    ( hooks.dalloc ? : ohooks.dalloc ),
                    ( hooks.commit ? : ohooks.commit ),
                    ( hooks.decommit ? : ohooks.decommit ),
                    ( hooks.purge ? : ohooks.purge ),
                    ( hooks.split ? : ohooks.split ),
                    ( hooks.merge ? : ohooks.merge )
                 };
   return mallctl_set<chunk_hooks_t>(oid,nhooks);
}

template struct GetObjFxn<uint64_t>;
template struct GetObjFxn<unsigned>;
template struct GetObjFxn<bool>;
template struct GetObjFxn<chunk_hooks_t>;

template struct SetObjFxn<uint64_t>;
template struct SetObjFxn<unsigned>;
template struct SetObjFxn<bool>;
template struct SetObjFxn<chunk_hooks_t>;

} // namespace jemallctl

////////////////////////////////// namespace numa
namespace numa {

auto get_cpu_sets(hwloc_obj_type_t objtype) -> std::vector<hwloc_const_cpuset_t> 
{
  std::vector<hwloc_const_cpuset_t> sets;

  auto n = hwloc_get_nbobjs_by_type(ink_get_topology(), objtype);
  while ( n-- ) 
  {
    hwloc_obj_t obj = hwloc_get_obj_by_type(ink_get_topology(), objtype, n);
    sets.emplace_back( obj ? obj->cpuset : kCpusAllowed_ );
  }
  return std::move(sets);
}

auto create_numa_arenas(unsigned) -> std::vector<unsigned>;

int use_other_thread_memory_arena(const char *thrname) // use non-assigned memory-page arena
{
   return 0;
}

int use_thread_memory_arena()  // use assigned memory-page arena only
{
   hwloc_set_membind_nodeset(ink_get_topology(), thrNUMAs, HWLOC_MEMBIND_INTERLEAVE, HWLOC_MEMBIND_THREAD);

   return 0;
}

int use_thread_arena_cpuset()  // limit to near-memory cpus only
{
   hwloc_set_thread_cpubind(ink_get_topology(), t->tid_, obj->cpuset, HWLOC_CPUBIND_STRICT);
   return 0;
}

int use_thread_socket_cpuset() // limit to near-memory socket cpu subset 
{
   hwloc_set_thread_cpubind(ink_get_topology(), t->tid_, obj->cpuset, HWLOC_CPUBIND_STRICT);
   return 0;
}

int use_thread_core_cpuset()   // limit to near-memory core cpu subset 
{
   hwloc_set_thread_cpubind(ink_get_topology(), t->tid_, obj->cpuset, HWLOC_CPUBIND_STRICT);
   return 0;
}

int use_thread_pu_cpuset()     // limit to near-memory logical cpu subset 
{
   hwloc_set_thread_cpubind(ink_get_topology(), t->tid_, obj->cpuset, HWLOC_CPUBIND_STRICT);
   return 0;
}

} // namespace numa
#endif
