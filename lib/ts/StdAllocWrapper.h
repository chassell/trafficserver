/** @file

  Fast-Allocators

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

  Provides three classes
    - Allocator for allocating memory blocks of fixed size
    - ClassAllocator for allocating objects
    - SpaceClassAllocator for allocating sparce objects (most members uninitialized)

  These class provides a efficient way for handling dynamic allocation.
  The fast allocator maintains its own freepool of objects from
  which it doles out object. Allocated objects when freed go back
  to the free pool.

  @note Fast allocators could accumulate a lot of objects in the
  free pool as a result of bursty demand. Memory used by the objects
  in the free pool never gets freed even if the freelist grows very
  large.

 */

#ifndef _StdAllocWrapper_h_
#define _StdAllocWrapper_h_

#include "ts/ink_queue.h"
#include "ts/ink_defs.h"
#include "ts/ink_resource.h"

#include <execinfo.h>    // for backtrace!

#include <new>
#include <memory>
#include <cstdlib>

#define Allocator      RawAllocator
#define ClassAllocator ObjAllocator
#define ProxyAllocator AllocatorStats

// NOTE: block competing includes after this one
#define _Allocator_h_

class RawAllocator : public std::allocator<uint64_t>
{
  const char *name_ = nullptr;
  size_t      sz_ = 0; // number of int64s

public:
  using std::allocator<uint64_t>::value_type;

  RawAllocator() { }
  RawAllocator(const char *name, unsigned int element_size)
     : name_(name), sz_( (element_size+sizeof(value_type)-1)/sizeof(value_type))
     { }

  void *alloc_void() { return std::calloc(sz_,sizeof(value_type)); }
  void free_void(void *ptr) { deallocate(static_cast<value_type*>(ptr),sz_); }
  void *alloc() { return std::calloc(sz_,sizeof(value_type)); }
  void free(void *ptr) { deallocate(static_cast<value_type*>(ptr),sz_); }

  void re_init(const char *name, unsigned int element_size, unsigned int chunk_size, unsigned int alignment, int advice) 
  {
    name_ = name; 
    sz_ = element_size;
    // XXX ignores alignment and advice!
  }
};

template <typename T_OBJECT>
class ObjAllocator : public std::allocator<T_OBJECT>
{
  const char *name_;
  using std::allocator<T_OBJECT>::allocate; 
  using std::allocator<T_OBJECT>::deallocate; 
  using typename std::allocator<T_OBJECT>::value_type; 
 public: 
  ObjAllocator(const char*name, unsigned chunk_size = 128) : name_(name) { }

  void *alloc_void() { return allocate(1); }
  void free_void(void *ptr) { deallocate(ptr); }
  value_type *alloc() { return allocate(1); }
  void free(value_type *ptr) { deallocate(ptr,1); }
};

class AllocatorStats { };

extern int thread_freelist_high_watermark;
extern int thread_freelist_low_watermark;

#define THREAD_ALLOC(a,thread)         ( ::a.alloc() )
#define THREAD_ALLOC_INIT(a,thread)    ( ::a.alloc() )
#define THREAD_FREE(ptr,a,thread)      ( ::a.free(ptr) )

#endif // _Allocator_h_
