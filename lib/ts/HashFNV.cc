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

/*
  http://www.isthe.com/chongo/tech/comp/fnv/

  Currently implemented FNV-1a 32bit and FNV-1a 64bit
 */

#include "HashFNV.h"

#define FNV_INIT_32 ((uint32_t)0x811c9dc5)
#define FNV_INIT_64 ((uint64_t)0xcbf29ce484222325ULL)

// FNV-1a 64bit
ATSHash32FNV1a::ATSHash32FNV1a(void) {
    this->clear();
}

void
ATSHash32FNV1a::update(const void *data, size_t len) {
    uint8_t *bp = (uint8_t *) data;
    uint8_t *be = bp + len;

    while (bp < be) {
        hval ^= (uint32_t) *bp++;
        hval += (hval << 1) + (hval << 4) + (hval << 7) + (hval << 8) + (hval << 24);
    }
}

void
ATSHash32FNV1a::final(void) {
}

uint32_t
ATSHash32FNV1a::get(void) const {
    return hval;
}

void
ATSHash32FNV1a::clear(void) {
    hval = FNV_INIT_32;
}

// FNV-1a 64bit
ATSHash64FNV1a::ATSHash64FNV1a(void) {
    this->clear();
}

void
ATSHash64FNV1a::update(const void *data, size_t len) {
    uint8_t *bp = (uint8_t *) data;
    uint8_t *be = bp + len;

    while (bp < be) {
        hval ^= (uint64_t) *bp++;
        hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
    }
}

void
ATSHash64FNV1a::final(void) {
}

uint64_t
ATSHash64FNV1a::get(void) const {
    return hval;
}

void
ATSHash64FNV1a::clear(void) {
    hval = FNV_INIT_64;
}
