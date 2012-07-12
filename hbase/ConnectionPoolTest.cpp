/*
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Test case for the ConnectionPool class.

#include <string>
#include <unordered_set>

#include "hbase/ConnectionPool.h"

#include <gtest/gtest.h>

namespace h = facebook::hbase;
using std::string;
using std::unordered_set;

class TestPoolEntry {
public:
  explicit TestPoolEntry(const string& s) : entry_name(s), healthy(true) {
    all_entries.insert(this);
  }
  ~TestPoolEntry() {
    all_entries.erase(this);
  }
  bool isHealthy() const { return healthy; }

  const string entry_name;
  static unordered_set<TestPoolEntry*> all_entries;
  bool healthy;
};

unordered_set<TestPoolEntry*> TestPoolEntry::all_entries;

TestPoolEntry* factory(const string& s) {
  return new TestPoolEntry(s);
}

TestPoolEntry* null_factory(const string& s) {
  return NULL;
}

const string key_maker(const TestPoolEntry* entry) {
  return entry->entry_name;
}

TEST(ConnectionPool, ConnectionNullPoolTest) {
  h::ConnectionPool<string, TestPoolEntry> test_pool(
    100, 0, null_factory, key_maker);
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
  TestPoolEntry* e1 = test_pool.lookup("test");
  EXPECT_TRUE(e1 == NULL);
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
}

TEST(ConnectionPool, ConnectionErrorTest) {
  h::ConnectionPool<string, TestPoolEntry> test_pool(
    100, 0, factory, key_maker);
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
  TestPoolEntry* e1 = test_pool.lookup("test");
  EXPECT_TRUE(e1 != NULL);
  EXPECT_EQ(1, TestPoolEntry::all_entries.size());
  test_pool.release(e1);
  EXPECT_EQ(1, TestPoolEntry::all_entries.size());
  e1 = test_pool.lookup("test");
  e1->healthy = false;
  test_pool.release(e1);
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
}

TEST(ConnectionPool, ConnectionPoolTest) {
  {
    // Some basic tests with a "large" pool.
    h::ConnectionPool<string, TestPoolEntry> test_pool(100, 0, factory,
                                                       key_maker);
    // starts empty...
    EXPECT_EQ(0, TestPoolEntry::all_entries.size());
    // should instantiate a new one by calling our factory method,
    // which we then let go, but confirm it still exists in the pool.
    TestPoolEntry* e1 = test_pool.lookup("test");
    EXPECT_EQ(1, TestPoolEntry::all_entries.size());
    test_pool.release(e1);
    EXPECT_EQ(1, TestPoolEntry::all_entries.size());
    // if we ask for one again, we get our old friend e1 back!
    EXPECT_EQ(e1, test_pool.lookup("test"));
    test_pool.release(e1);
    // confirm adding a second one doesn't give us the same entry
    TestPoolEntry* e2 = test_pool.lookup("test");
    EXPECT_EQ(1, TestPoolEntry::all_entries.size());
    TestPoolEntry* e3 = test_pool.lookup("test");
    EXPECT_NE(e2, e3);
    test_pool.release(e2);
    test_pool.release(e3);
  }
  // confirm ConnectionPool destructor empties out all our pool entries
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
}

TEST(ConnectionPool, ConnectionPoolLimitTest) {
  // Test a pool with a limit of two retained connections.  First
  // check the basic stuff.
  h::ConnectionPool<string, TestPoolEntry> test_pool(2, 100, factory,
                                                     key_maker);
  EXPECT_EQ(0, TestPoolEntry::all_entries.size());
  TestPoolEntry* e1 = test_pool.lookup("test");
  EXPECT_EQ(1, TestPoolEntry::all_entries.size());

  // Instantiate a "lot" of pool entries.  All but 2 will be freed as
  // soon as they are released, as per the construction to test_pool.
  std::vector<TestPoolEntry*> entries;
  for (int i = 0; i < 500; ++i) {
    TestPoolEntry* e = test_pool.lookup("test");
    // e1 plus the first 99 of this loop should get us to the 100 hard
    // limit; so subsequent calls will return NULL.  Verify that here.
    if (i >= 99) {
      EXPECT_TRUE(e == NULL);
    } else {
      EXPECT_TRUE(e != NULL);
      entries.push_back(e);
    }
  }

  // All of 'entries' plus e1.  Since we tried to create 500 entries
  // with a max retain count of 100, 400 were null so all_entries
  // should be limited in size.
  EXPECT_EQ(entries.size() + 1, TestPoolEntry::all_entries.size());

  // Release them all; should go back down to 2 entries in the pool
  // (e1 and a now unreferenced entry that was in the 'entries'
  // vector).
  for (TestPoolEntry* entry : entries) {
    test_pool.release(entry);
  }
  EXPECT_EQ(2, TestPoolEntry::all_entries.size());

  test_pool.release(e1);
}

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}
