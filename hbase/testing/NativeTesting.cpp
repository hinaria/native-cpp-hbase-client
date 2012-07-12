/* Copyright 2012 Facebook, Inc.
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

// A program that tests hbc functionality against an HBase cluster.
//
// This test is a work in progress.  As it is quite difficult to
// instantiate an hbase cell as part of a unit test (from C++, anyway), we
// instead rely on the Eris cluster by default.
//
// However, the preferred way to run this test would be from HBase's
// TestNativeThriftClient unit test. TestNativeThriftClient instantiates a
// Mini HBase cluster and runs this test against that mini-cluster.
// If you are making changes here, you can test them with TestNativeThriftClient
// as follows:
//
// export FBCODE_DIR=<your_fbcode_dir>
// mvn -Dtest=TestNativeThriftClient test

#include <string>
#include <vector>
#include <map>

#include "folly/Hash.h"
#include "folly/String.h"

#include "hbase/NativeHbaseClient.h"

#include "hbase/hbase_constants.h"

using namespace facebook;
using namespace std;
using apache::hadoop::hbase::thrift::g_hbase_constants;
namespace h = facebook::hbase;

DEFINE_string(hbase, "eris002-snc4-hbase-zookeepers",
              "SMC tier or zookeeper quorum for the cell to run against");
DEFINE_string(test_table_name, "hbase_client_test_table",
              "Table to create, mangle, and destroy for this test");
DEFINE_string(permanent_test_table_name, "hbase_client_permanent_test_table",
              "Permanent table known to conform to a certain structure, "
              "used during testing to ensure proper region boundary behavior");
DEFINE_int32(num_test_rows, 25000, "number of rows to test with");
DEFINE_int32(test_batch_size, 1000, "number of rows to batch per change");
DECLARE_int32(zookeeper_port);

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  if (FLAGS_permanent_test_table_name == FLAGS_test_table_name) {
    LOG(FATAL) << "Do not set --test_table_name to be the same as "
               << "--permanent_test_table_name!";
  }

  LOG(INFO) << "Connecting to " << FLAGS_hbase;

  h::NativeHbaseClient client(FLAGS_hbase);
  if (!client.connect()) {
    LOG(FATAL) << "Unable to connect to zookeeper: " << FLAGS_hbase;
  }

  // Delete the test table if it already exists
  vector<h::Text> tables;
  client.listTables(&tables);
  for (int i = 0; i < tables.size(); ++i) {
    if (tables[i] == FLAGS_test_table_name) {
      LOG(INFO) << "Disabling existing table " << tables[i];
      client.disableTable(tables[i]);
      LOG(INFO) << "Dropping table " << tables[i];
      client.deleteTable(tables[i]);
      break;
    }
  }

  {
    LOG(INFO) << "Creating " << FLAGS_test_table_name;
    vector<h::ColumnDescriptor> columns;
    columns.resize(2);
    columns[0].name = "cfam1:";
    columns[1].name = "cfam2:";
    client.createTable(FLAGS_test_table_name, columns);
  }

  {
    vector<h::Text> columns;
    std::unique_ptr<h::Scanner> scanner(client.getScanner(FLAGS_test_table_name,
                                                          "", columns));
    scanner->scan();
    h::TRowResult *result;
    if (scanner->next(&result)) {
      LOG(FATAL) << "Table should be empty!";
    }
  }

  map<string, string> table_contents;
  {
    LOG(INFO) << "Inserting rows";
    vector<h::BatchMutation> mutations(FLAGS_test_batch_size);
    for (uint64_t i = 0; i < FLAGS_num_test_rows; ++i) {
      mutations.resize(mutations.size() + 1);
      h::BatchMutation &batch = mutations.back();
      batch.row = folly::stringPrintf("%016lx", folly::hash::twang_mix64(i));

      h::Mutation mutation;
      mutation.column = folly::stringPrintf("cfam%ld:col%c", (i & 1) + 1,
                                            'a' + int(i & 0xf));
      mutation.value = folly::stringPrintf("some value %ld", i);
      table_contents[batch.row] = mutation.value;
      mutation.isDelete = false;
      mutation.writeToWAL = bool(i % 2);
      batch.mutations.push_back(mutation);
      if (mutations.size() > FLAGS_test_batch_size) {
        if (!client.mutateRows(FLAGS_test_table_name, mutations)) {
          LOG(FATAL) << "One or more mutation failed";
        }
        mutations.clear();
      }
    }
    if (!client.mutateRows(FLAGS_test_table_name, mutations)) {
      LOG(FATAL) << "One or more mutation failed";
    }
  }

  {
    LOG(INFO) << "Checking unused scanner destruction";
    vector<h::Text> columns;
    columns.push_back("cfam1:");
    std::unique_ptr<h::Scanner> scanner(client.getScanner(FLAGS_test_table_name,
                                                          "", columns));
  }

  string third_row_key;
  {
    LOG(INFO) << "Checking rows";
    vector<h::Text> columns;
    columns.push_back("cfam1:");
    std::unique_ptr<h::Scanner> scanner(client.getScanner(FLAGS_test_table_name,
                                                          "", columns));
    scanner->scan();
    h::TRowResult *result;
    int num_rows = 0;
    while (scanner->next(&result)) {
      num_rows++;
      if (num_rows == 3) {
        third_row_key = result->row;
      }
    }
    if (num_rows != FLAGS_num_test_rows / 2) {
      LOG(FATAL) << "Expected " << FLAGS_num_test_rows << " rows, got "
                 << num_rows;
    }
  }

  {
    CHECK(!third_row_key.empty());
    LOG(INFO) << "Testing end row";
    vector<h::Text> columns;
    columns.push_back("cfam1:");
    std::unique_ptr<h::Scanner> scanner(client.getScanner(FLAGS_test_table_name,
                                                          "", columns));
    scanner->setEndRow(third_row_key);
    scanner->scan();
    h::TRowResult *result;
    int num_rows = 0;
    string last_row_key;
    while (scanner->next(&result)) {
      ++num_rows;
      last_row_key = result->row;
      CHECK_LE(last_row_key, third_row_key);
    }
    CHECK_EQ(num_rows, 3);
    CHECK_EQ(third_row_key, last_row_key);
  }
  {
    try {
      LOG(INFO) << "Querying invalid table";
      vector<h::Text> columns;
      std::unique_ptr<h::Scanner> scanner(
        client.getScanner("table_that_should_not_exist", "", columns));
      scanner->scan();
      h::TRowResult *result;
      scanner->next(&result);
      LOG(FATAL) << "Query should have failed!";
    } catch (const h::TableNotFound& e) {
      LOG(INFO) << "Properly could not query bad table";
    }
  }

  if (!FLAGS_permanent_test_table_name.empty()) {
    const string split_key("6e915c835d215625");
    {
      vector<h::TRegionInfo> regions;
      client.listTableRegions(&regions, FLAGS_permanent_test_table_name);
      if (regions.size() != 2) {
        LOG(FATAL) << FLAGS_permanent_test_table_name << " should have exactly "
                   << "two regions, not " << regions.size();
      }
      if (regions[1].startKey != split_key) {
        LOG(FATAL) << "Expected splitpoint of "
                   << FLAGS_permanent_test_table_name
                   << "to be " << split_key << ", not " << regions[1].startKey;
      }
      vector<h::Text> columns;
      std::unique_ptr<h::Scanner> scanner(
        client.getScanner(FLAGS_permanent_test_table_name, "", columns));

      scanner->scan();
      h::TRowResult *result;
      int num_rows = 0;
      while (scanner->next(&result)) {
        num_rows++;
      }
      if (num_rows != FLAGS_num_test_rows) {
        LOG(FATAL) << "Expected " << FLAGS_num_test_rows << " rows, got "
                   << num_rows;
      }
    }

    {
      LOG(INFO) << "Testing scan with end row that crosses region boundaries";
      vector<h::Text> columns;
      std::unique_ptr<h::Scanner> scanner(
        client.getScanner(FLAGS_permanent_test_table_name, "", columns));
      scanner->setEndRow(split_key + "Z");
      scanner->scan();

      h::TRowResult *result;
      string last_row_key;
      int num_rows = 0;
      while (scanner->next(&result)) {
        ++num_rows;
        last_row_key = result->row;
      }
      CHECK_EQ(last_row_key, split_key);
    }
  }

  {
    LOG(INFO) << "Checking single operations";
    vector<h::TRowResult> results;
    client.getRow(&results, FLAGS_test_table_name,
                  "non-existent row");
    CHECK_EQ(0, results.size());

    vector<h::Mutation> mutations;
    mutations.resize(2);
    mutations[0].column = "cfam1:testcol1";
    mutations[0].value = "value 1";
    mutations[1].column = "cfam1:testcol2";
    mutations[1].value = "value 2";
    CHECK(client.mutateRow(FLAGS_test_table_name, "new row", mutations));

    client.getRow(&results, FLAGS_test_table_name,
                  "new row");
    CHECK_EQ(1, results.size());
    CHECK_EQ("value 1", results[0].columns.begin()->second.value);
  }

  {
    LOG(INFO) << "Checking batch row fetch";
    vector<h::TRowResult> results;
    vector<string> keys;
    for (auto it = table_contents.begin(); it != table_contents.end(); ++it) {
      keys.push_back(it->first);
    }
    keys.push_back("non-existent row key");

    client.getRow(&results, FLAGS_test_table_name, keys[0]);
    CHECK_EQ(results.size(), 1);
    CHECK_EQ(table_contents[keys[0]],
             results[0].columns.begin()->second.value);

    unordered_map<string, h::TRowResult*> rows_by_key;
    CHECK(client.getRows(&results, &rows_by_key, FLAGS_test_table_name, keys));
    CHECK_EQ(results.size(), keys.size() - 1 /* non-existent key */);
    for (int i = 0; i < results.size(); ++i) {
      CHECK_EQ(table_contents[results[i].row],
               results[i].columns.begin()->second.value);
      CHECK(rows_by_key[results[i].row] != NULL);
    }
    CHECK(rows_by_key.find("non-existent row key") != rows_by_key.end());
    CHECK(rows_by_key["non-existent row key"] == NULL);

    LOG(INFO) << "Testing getRowsWithColumns";
    results.clear();
    rows_by_key.clear();
    vector<string> columns { "cfam1:cola", "cfam1:badcolumn" };
    CHECK(client.getRowsWithColumns(&results, &rows_by_key,
                                    FLAGS_test_table_name, keys, columns));
    CHECK_GT(results.size(), 0);
    for (int i = 0; i < results.size(); ++i) {
      CHECK_EQ(results[i].columns.size(), 1);
      CHECK_EQ(results[i].columns.begin()->first,
               "cfam1:cola");
      CHECK_EQ(table_contents[results[i].row],
               results[i].columns.begin()->second.value);
      CHECK(rows_by_key[results[i].row] != NULL);
    }
    CHECK(rows_by_key.find("non-existent row key") != rows_by_key.end());
    CHECK(rows_by_key["non-existent row key"] == NULL);
  }

  {
    LOG(INFO) << "Checking per-mutation timestamps";
    vector<h::Mutation> mutations;
    const int64_t custom_ts = 2 * 1000 * 1000 * 1000;
    const int64_t default_ts = 1000 * 1000 * 1000;
    mutations.resize(2);

    // A mutation with custom timestamp
    mutations[0].column = "cfam1:c1";
    mutations[0].value = "v1";
    mutations[0].timestamp = custom_ts;

    // A mutation with default timestamp
    mutations[1].column = "cfam1:c2";
    mutations[1].value = "v2";
    CHECK_EQ(g_hbase_constants.LATEST_TIMESTAMP, mutations[1].timestamp);

    const string row("per_mutation_ts");
    CHECK(client.mutateRowTs(FLAGS_test_table_name, row, mutations,
        default_ts));

    vector<h::TRowResult> results;
    client.getRow(&results, FLAGS_test_table_name, row);
    CHECK_EQ(1, results.size());
    const h::TRowResult &r = results[0];
    CHECK_EQ(row, r.row);
    CHECK_EQ(2, r.columns.size());
    for(const auto& m : mutations) {
      auto col_iter(r.columns.find(m.column));
      CHECK(col_iter != r.columns.end());
      auto &c(col_iter->second);
      CHECK_EQ(m.value, c.value);
      const int64_t expected_ts =
          m.timestamp == g_hbase_constants.LATEST_TIMESTAMP ?
              default_ts : custom_ts;
      CHECK_EQ(expected_ts, c.timestamp);
    }
  }

  LOG(INFO) << "All done!";

  return 0;
}
