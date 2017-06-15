/** @file

  A brief file description

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

//-*-c++-*-
#ifndef _ink_align_h_
#define _ink_align_h_

#include <memory>

/**
 * Alignment macros
 */

#define INK_MIN_ALIGN 8

/* INK_ALIGN() is only to be used to align on a power of 2 boundary */
#define INK_ALIGN(size, boundary)  aligned_spacing(size,boundary)

/** Default alignment */
#define INK_ALIGN_DEFAULT(size) INK_ALIGN(size, INK_MIN_ALIGN)

static inline size_t
aligned_spacing(size_t len, size_t block=INK_MIN_ALIGN)
{
    void *ptr = reinterpret_cast<void*>(len); // pointer from zero
    // next size >= len to get next a new aligned block
    return reinterpret_cast<size_t>( std::align(block, 0, ptr, block) );
}

//
// Move a pointer forward until it meets the alignment width.
//
static inline void *
align_pointer_forward(const void *pointer_, size_t alignment)
{
    // next ptr >= pointer_ to get next a new aligned block
    return std::align(alignment, 0, const_cast<void*&>(pointer_), alignment); 
}

//
// Move a pointer forward until it meets the alignment width specified,
// and zero out the contents of the space you're skipping over.
//
static inline void *
align_pointer_forward_and_zero(void *pointer_, size_t alignment)
{
    size_t left = alignment;
    void *aptr = std::align(alignment, 0, pointer_, left); 
    memset( pointer_, '\0', alignment - left ); // zero bytes before new block
    return aptr;
}

#endif
