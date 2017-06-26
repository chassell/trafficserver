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
#ifndef _hugepages_h_
#define _hugepages_h_

#include <cstring>

size_t ats_hugepage_size(void);
bool ats_hugepage_enabled(void);
void ats_hugepage_init(int);
void *ats_alloc_hugepage(size_t);
bool ats_free_hugepage(void *, size_t);

#if HAVE_LIBJEMALLOC
extern const chunk_hooks_t jemallct_huge_hooks;
extern const chunk_hooks_t jemallct_huge_nodump_hooks;
#endif

#endif
