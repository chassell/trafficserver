/** @file

  Some small general interest definitions

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

#ifndef _ink_defs_h
#define _ink_defs_h

#include "ts/ink_config.h"
#include <stddef.h>
#include <sys/mman.h>

#ifdef HAVE_STDINT_H
#include <stdint.h>
#else
// TODO: Add "standard" int types?
#endif

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#else
// TODO: add PRI*64 stuff?
#endif

#ifndef INT64_MIN
#define INT64_MAX (9223372036854775807LL)
#define INT64_MIN (-INT64_MAX - 1LL)
#define INT32_MAX (2147483647)
#define INT32_MIN (-2147483647 - 1)
#endif

#define POSIX_THREAD
#define POSIX_THREAD_10031c

#ifndef ETIME
#ifdef ETIMEDOUT
#define ETIME ETIMEDOUT
#endif
#endif

#ifndef ENOTSUP
#ifdef EOPNOTSUPP
#define ENOTSUP EOPNOTSUPP
#endif
#endif

#if defined(darwin)
#define RENTRENT_GETHOSTBYNAME
#define RENTRENT_GETHOSTBYADDR
#endif

#define NUL '\0'

// Determine the element count for an array.
#ifdef __cplusplus

#include <type_traits>

template<typename T_ELEM>
constexpr std::size_t countof()
{
  using T = typename std::remove_reference<T_ELEM>::type;
  static_assert(std::is_array<T>::value, 
                "countof() requires an array argument");
  static_assert(std::extent<T>::value > 0,  // [0]
                "zero- or unknown-size array");
  return std::extent<T>::value;
}

#define countof(a) countof<decltype(a)>()

#else
#define countof(x) ((unsigned)(sizeof(x) / sizeof((x)[0])))
#endif

#define SOCKOPT_ON ((char *)&on)
#define SOCKOPT_OFF ((char *)&off)

#ifndef ABS
#define ABS(x) (((x) < 0) ? (-(x)) : (x))
#endif

#ifndef MAX
#define MAX(x, y) (((x) >= (y)) ? (x) : (y))
#endif

#ifndef MIN
#define MIN(x, y) (((x) <= (y)) ? (x) : (y))
#endif

#ifdef __cplusplus
// We can't use #define for min and max because it will conflict with
// other declarations of min and max functions.  This conflict
// occurs with STL
template <class T>
T
min(const T a, const T b)
{
  return a < b ? a : b;
}

template <class T>
T
max(const T a, const T b)
{
  return a > b ? a : b;
}
#endif

#define ATS_UNUSED __attribute__((unused))
#define ATS_WARN_IF_UNUSED __attribute__((warn_unused_result))
#define ATS_UNUSED_RETURN(x) \
  if (x) {                   \
  }

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

#if TS_USE_HWLOC
#include <hwloc.h>
#elif __cplusplus
// some signatures need this
using hwloc_obj_type_t = int; 
#endif

#ifndef ROUNDUP
#define ROUNDUP(x, y) ((((x) + ((y)-1)) / (y)) * (y))
#endif

#if defined(MAP_NORESERVE)
#define MAP_SHARED_MAP_NORESERVE (MAP_SHARED | MAP_NORESERVE)
#else
#define MAP_SHARED_MAP_NORESERVE (MAP_SHARED)
#endif

/* Variables
*/
extern int debug_level;
extern int off;
extern int on;

/* Functions
*/
int ink_sys_name_release(char *name, int namelen, char *release, int releaselen);
int ink_number_of_processors();
int ink_login_name_max();

#if TS_USE_HWLOC
// Get the hardware topology
hwloc_topology_t ink_get_topology();
#endif

/** Constants.
 */
#ifdef __cplusplus
namespace ts
{
static const int NO_FD = -1; ///< No or invalid file descriptor.
}
#endif

#endif /*__ink_defs_h*/
