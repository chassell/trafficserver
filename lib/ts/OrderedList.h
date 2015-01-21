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

#ifndef __ORDERED_LIST_H__
#define __ORDERED_LIST_H__

#include <iostream>
#include <list>

struct ATSOrderedListNode 
{
  bool available;
  char *name;
  float weight;
};

std::ostream &
operator<< (std::ostream & os, ATSOrderedListNode & node);

struct ATSOrderedList
{
  ATSOrderedList ();
  void insert (ATSOrderedListNode *node);
  ATSOrderedListNode *lookup ();
  ATSOrderedListNode *lookupUnavailable ();
  ~ATSOrderedList ();
  void setUnavailable (ATSOrderedListNode *node);
  void setAvailable (ATSOrderedListNode *node);
  void printAll ();

private:
  std::list<ATSOrderedListNode *> OrderedList;
  bool isNodeInList (ATSOrderedListNode *node);
};

#endif
