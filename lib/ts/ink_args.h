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

/****************************************************************************
Process arguments

****************************************************************************/

#ifndef _INK_ARGS_H
#define _INK_ARGS_H
#include "ts/ink_defs.h"
#include "ts/ink_apidefs.h"

#if HAVE_SYSEXITS_H
#include <sysexits.h>
#endif

#include <new>

#ifndef EX_USAGE
#define EX_USAGE 64
#endif

#define MAX_FILE_ARGUMENTS 100

struct ArgumentDescription;
class AppVersionInfo;

typedef void ArgumentFunction(const ArgumentDescription *argument_descriptions, unsigned n_argument_descriptions, const char *arg);

struct ArgumentDescription {
  const char *name;
  char key; // set to '-' if no single character key.
            /*
               "I" = integer
               "L" = int64_t
               "D" = double (floating point)
               "T" = toggle
               "F" = set flag to TRUE (default is FALSE)
               "f" = set flag to FALSE (default is TRUE)
               "T" = toggle
               "S80" = read string, 80 chars max
               "S*" = read unbounded string, allocating
             */
  const char *const description;
  const char *const type;
  union {
      int        &u_int;
      int64_t    &u_int64;
      double     &u_double;
      const char *(&u_cstr);
      char *const u_buffer; // i.e. same as (&u_buffer)[]
  };
  const char *const env;
  ArgumentFunction *const pfn;

  ArgumentDescription &operator=(const ArgumentDescription &other) 
  {
      this->~ArgumentDescription();         // a no-op
      new(this) ArgumentDescription{other}; // simple mem copy of const ptrs
      return *this;
  }

};

#define VERSION_ARGUMENT_DESCRIPTION()                                         \
  {                                                                            \
    "version", 'V', "Print version string", nullptr, { .u_buffer=nullptr }, nullptr, nullptr \
  }
#define HELP_ARGUMENT_DESCRIPTION()                                          \
  {                                                                          \
    "help", 'h', "Print usage information", nullptr, { .u_buffer=nullptr }, nullptr, usage \
  }

/* Global Data
*/
extern const char *file_arguments[]; // exported by process_args()
extern unsigned n_file_arguments;    // exported by process_args()
extern const char *program_name;     // exported by process_args()

/* Print out arguments and values
*/
void show_argument_configuration(const ArgumentDescription *argument_descriptions, unsigned n_argument_descriptions);

void usage(const ArgumentDescription *argument_descriptions, unsigned n_argument_descriptions, const char *arg_unused) TS_NORETURN;

/* Process all arguments
*/
void process_args(const AppVersionInfo *appinfo, const ArgumentDescription *argument_descriptions, unsigned n_argument_descriptions,
                  const char **argv, const char *usage_string = 0);

bool process_args_ex(const AppVersionInfo *appinfo, const ArgumentDescription *argument_descriptions,
                     unsigned n_argument_descriptions, const char **argv);

#endif /*_INK_ARGS_H*/
