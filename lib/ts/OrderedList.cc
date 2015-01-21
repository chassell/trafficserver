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

#include "OrderedList.h"

std::ostream &
operator << (std::ostream& os, ATSOrderedListNode& node)
{
  return os << node.name;
}

ATSOrderedList::ATSOrderedList ()
{
}

ATSOrderedList::~ATSOrderedList ()
{
}

void
ATSOrderedList::insert (ATSOrderedListNode *node)
{
  bool inserted = false;

  if (OrderedList.size() == 0) 
  {
    OrderedList.push_front (node);
  }
  else {
    for (std::list<ATSOrderedListNode *>::iterator it = OrderedList.begin(); it != OrderedList.end(); it++) 
    {
      ATSOrderedListNode *n =  *it;
      if (node->weight < n->weight) {
        OrderedList.insert (it, node);
        inserted = true;
        break;
      }
    }
    if (! inserted) OrderedList.push_back (node);
  }
}

ATSOrderedListNode *
ATSOrderedList::lookup ()
{
  ATSOrderedListNode *n;
  std::list<ATSOrderedListNode *>::iterator it = OrderedList.begin();
  do {
    n = *it;
    if (n->available) break;
      it++;
  } while (it != OrderedList.end());

  n->available ?  n = n : n = NULL;

  return n;
}

ATSOrderedListNode *
ATSOrderedList::lookupUnavailable ()
{
    ATSOrderedListNode *n;
    std::list<ATSOrderedListNode *>::iterator it = OrderedList.begin();
    do {
      n = *it;
      if (! n->available) return n;
      it++;
    } while (it != OrderedList.end());

    return NULL;
}

void
ATSOrderedList::setUnavailable (ATSOrderedListNode *node)
{
  if (isNodeInList(node)) node->available = false;
}

void
ATSOrderedList::setAvailable (ATSOrderedListNode *node)
{
  if (isNodeInList(node)) node->available = true;
}

void
ATSOrderedList::printAll ()
{
  for (std::list<ATSOrderedListNode *>::iterator it = OrderedList.begin(); it != OrderedList.end(); it++) 
  {
    ATSOrderedListNode *n = *it;
    std::cout << "Address of node: " << n << std::endl;
    std::cout << "name = " << *n << ", available = " << n->available << ", weight = " << n->weight << std::endl;
  }
}

bool
ATSOrderedList::isNodeInList (ATSOrderedListNode *node)
{
  for (std::list<ATSOrderedListNode *>::iterator it = OrderedList.begin(); it != OrderedList.end(); it++)
  {
    ATSOrderedListNode *n = *it;
    if (n == *it) return true;
  }
  return false;
}
