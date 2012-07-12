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

#include "folly/String.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include "folly/Logging.h"
#include "folly/Range.h"
#include "folly/Conv.h"
#include "folly/String.h"
#include "hbase/HbcExtensions.h"
#include "hbase/NativeHbaseClient.h"

const char* usage =
  "Usage: hbc [params] command [command params]\n"
  "\n"
  "Note: all commands rely on either the --zookeeper option or the \n"
  "HBC_ZOOKEEPER environment variable, which contains the ZooKeeper \n"
  "instance where the HBase cluster stores its state.\n"
  "\n"
  "For more details, see:\n"
  "  https://www.intern.facebook.com/intern/wiki/index.php/HbaseClientTool\n";

using namespace facebook;
using namespace std;

using folly::backslashify;
using folly::humanify;
using folly::StringPiece;
using folly::to;

DEFINE_string(zookeeper, getenv("HBC_ZOOKEEPER") ? : "",
              "SMC tier containing the zookeeper hosts, or directly a comma "
              "separated list of host:ports for our zookeeper instance");
DEFINE_string(start_row, "", "Row to begin with in 'scan' mode");
DEFINE_string(end_row, "", "Row to end with in 'scan' mode");
DEFINE_string(column, "", "Column to print (regex)");
DEFINE_int32(num_scan_rows, -1, "Max number of rows to return when scanning; "
             "-1 for all rows");
DEFINE_string(timestamp, "", "Timestamp to restrict returned rows to (uint64)");

DEFINE_bool(print_timestamp, false, "Include timestamps in output");
DEFINE_bool(bypass_special_row_display, false, "always display row results as "
            "key/value rather than using extended output processing");
DEFINE_bool(smart_row_interpretation, true, "if set, hbc will attempt to "
           "guess row key meaning based on prefixes rather than literally; "
            "for instance, keys starting with 0x will be interpreted as hex");
DECLARE_int32(hbase_port);
DEFINE_bool(disable_wal, false,
            "whether Mutation write-ahead-log will be disabled on the server");

namespace facebook { namespace hbase {
const string smartKeyGuess(const string& input) {
  if (!FLAGS_smart_row_interpretation) return input;

  string ret(input);
  if (boost::starts_with(input, "0x")) {
    if (!folly::unhexlify(string(input, 2), ret)) {
      LOG(FATAL) << "Invalid hex string: " << input;
    }
    return ret;
  }
  // TODO(chip): also support backslash encoding, perhaps binary
  // encoding as well?

  return ret;
}

static vector<RowPrinter> row_printers;
bool invokeRowPrinter(const TRowResult* row) {
  if (FLAGS_bypass_special_row_display) return false;

  for (auto& rp : row_printers) {
    if (rp(row)) return true;
  }
  return false;
}

class RegionsCommand : public Subcommand {
public:
  RegionsCommand() : Subcommand("regions") { }

  void Usage(const string& prefix) {
    cout << prefix << "regions [ <table> <table> ... ]" << endl;
  }

  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    vector<Text> tables;

    client->listTables(&tables);
    for (int i = 0; i < tables.size(); ++i) {
      if (args.size() > 0 &&
          find(args.begin(), args.end(), tables[i]) == args.end()) {
        continue;
      }
      cout << "Table: " << tables[i] << endl;

      vector<TRegionInfo> regions;
      client->listTableRegions(&regions, tables[i]);
      for (int j = 0; j < regions.size(); ++j) {
        cout << "  Region " << backslashify(regions[j].name) << endl
             << "    Host : " << regions[j].serverName << ":" << regions[j].port
             << endl
             << "    Start: " << humanify(regions[j].startKey) << endl
             << "    End  : " << humanify(regions[j].endKey) << endl;
      }
      cout << endl;
    }

    return 0;
  }
};

class TablesCommand : public Subcommand {
public:
  TablesCommand() : Subcommand("tables") { }
  void Usage(const string& prefix) {
    cout << prefix << "tables" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    vector<Text> tables;

    client->listTables(&tables);
    for (int i = 0; i < tables.size(); ++i) {
      cout << "Table: " << tables[i] << endl;
    }

    return 0;
  }
};

class RegionServersCommand : public Subcommand {
public:
  RegionServersCommand() : Subcommand("region_servers") { }
  void Usage(const string& prefix) {
    cout << prefix << "region_servers" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    vector<pair<string, int>> region_servers;

    client->listRegionServers(&region_servers);
    cout << "Region servers:" << endl;
    for (int i = 0; i < region_servers.size(); ++i) {
      cout << "  " << region_servers[i].first << ":"
           << region_servers[i].second << endl;
    }

    return 0;
  }
};

// A relatively simple routine that attempts to format a cell for
// display.  For printable strings, it just returns the string in
// quotes; for 1, 2, 4, or 8 byte unprintable strings, it turns them
// into the appropriate type of integer, performs an endian swap, and
// returns that.  Otherwise, unprintable strings are hexdumped.
static string FormatCellValue(const TCell& cell) {
  const string& input = cell.value;
  bool has_binary = false;
  for (unsigned char c : input) {
    if (!isprint(c)) {
      has_binary = true;
      break;
    }
  }
  if (!has_binary) {
    return '"' + input + '"';
  }

  string output;
  if (input.size() == 1) {
    int8_t value;
    memcpy(&value, &input[0], 1);
    output = folly::stringPrintf("int8:%d", value);
  } else if (input.size() == 2) {
    int16_t value;
    memcpy(&value, &input[0], 2);
    output =  folly::stringPrintf("int16:%d", htons(value));
  } else if (input.size() == 4) {
    int32_t value;
    memcpy(&value, &input[0], 4);
    output =  folly::stringPrintf("int32_t:%d", htonl(value));
  } else if (input.size() == 8) {
    int64_t value;
    memcpy(&value, &input[0], 8);
    output = folly::stringPrintf("int64_t:%lu", htonll(value));
  } else {
    CHECK(folly::hexlify(cell.value, output));
    output = "0x" + output;
  }

  if (FLAGS_print_timestamp) {
    output += folly::stringPrintf(" (%lu)", cell.timestamp);
  }

  return output;
}

class GetCommand : public Subcommand {
public:
  GetCommand() : Subcommand("get") { }
  void Usage(const string& prefix) {
    cout << prefix << "get <table> <row>" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() < 2) {
      throw UsageException();
    }

    if (!FLAGS_timestamp.empty() && FLAGS_column.empty()) {
      LOG(ERROR) << "Must include --column when using --timestamp";
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));
    const string row_key(smartKeyGuess(args[1]));

    vector<TRowResult> rows;
    if (FLAGS_column.empty()) {
      client->getRow(&rows, table, row_key);
    } else {
      vector<string> columns;
      columns.push_back(FLAGS_column);
      if (!FLAGS_timestamp.empty()) {
        uint64_t timestamp;
        timestamp = to<uint64_t>(FLAGS_timestamp);
        client->getRowWithColumnsTs(&rows, table, row_key, columns, timestamp);
      } else {
        client->getRowWithColumns(&rows, table, row_key, columns);
      }
    }
    for (TRowResult &row : rows) {
      if (!invokeRowPrinter(&row)) {
        for (auto& kv : row.columns) {
          cout << humanify(kv.first) << ": " << humanify(kv.second.value)
               << endl;
        }
      }
    }

    return 0;
  }
};

class SetCommand : public Subcommand {
public:
  SetCommand() : Subcommand("set") { }
  void Usage(const string& prefix) {
    cout << prefix
         << "set [--disable_wal] <table> <row> column=valuue column=value ..."
         << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() < 3) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));
    const string row_key(smartKeyGuess(args[1]));
    vector<BatchMutation> mutations;

    bool write_to_wal = !FLAGS_disable_wal;
    for (int i = 2; i < args.size(); ++i) {
      vector<string> assignment;
      folly::split("=", args[i], assignment);
      if (assignment.size() != 2) {
        LOG(ERROR) << "Invalid assignment: " << args[i];
        throw UsageException();
      }

      mutations.resize(mutations.size() + 1);
      BatchMutation &batch = mutations.back();
      batch.row = row_key;

      Mutation mutation;
      mutation.column = assignment[0];
      mutation.value = assignment[1];
      mutation.isDelete = false;
      mutation.writeToWAL = write_to_wal;
      batch.mutations.push_back(mutation);
    }
    if (!client->mutateRows(table, mutations)) {
      LOG(ERROR) << "One or more mutation failed";
    }

    return 0;
  }
};

class DeleteCommand : public Subcommand {
public:
  DeleteCommand() : Subcommand("delete") { }
  void Usage(const string& prefix) {
    cout << prefix << "delete <table> <row> [column column ...]"
         << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() < 2) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));
    const string row_key(smartKeyGuess(args[1]));

    // Use batch mutation just to exercise the more complex codepath.
    vector<BatchMutation> mutations;

    if (args.size() == 2) {
      if (!client->deleteAllRow(table, row_key)) {
        LOG(ERROR) << "Unable to delete entire row: " << row_key;
      }
    } else {
      for (int i = 2; i < args.size(); ++i) {
        if (!client->deleteAll(table, row_key, args[i])) {
          LOG(ERROR) << "Delete failed, row: " << row_key << " cell: "
                     << args[i];
        }
      }
    }

    return 0;
  }
};

class ScanCommand : public Subcommand {
public:
  ScanCommand() : Subcommand("scan") { }
  void Usage(const string& prefix) {
    cout << prefix << "scan [ --start_row R ] [ --end_row R ] "
         << "[ --column C ] [--scan_limit N] <table>" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() < 1) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));
    vector<string> columns;
    if (FLAGS_column.size()) {
      columns.push_back(FLAGS_column);
    }
    std::unique_ptr<Scanner> scanner(
      client->getScanner(table, smartKeyGuess(FLAGS_start_row), columns));

    if (FLAGS_end_row.size()) {
      scanner->setEndRow(smartKeyGuess(FLAGS_end_row));
    }
    scanner->scan();
    TRowResult *result;
    int rows_remaining = FLAGS_num_scan_rows;
    while (scanner->next(&result) && rows_remaining != 0) {
      if (!invokeRowPrinter(result)) {
        cout << "Row " << humanify(result->row) << endl;
        Text column;
        TCell cell;
        for(auto& kv : result->columns) {
          cout << "  " << humanify(kv.first) << " - "
               << FormatCellValue(kv.second) << endl;
        }
      }
      --rows_remaining;
    }

    return 0;
  }
};

class SchemaCommand : public Subcommand {
public:
  SchemaCommand() : Subcommand("schema") { }
  void Usage(const string& prefix) {
    cout << prefix << "schema <table>" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() != 1) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));
    map<string, ColumnDescriptor> columns;
    client->getColumnDescriptors(&columns, table);

    cout << "# Table schema for " << table << endl;
    time_t now = time(NULL);
    char buf[26];
    CHECK(ctime_r(&now, buf));
    cout << "# Dumped on " << buf;
    cout << endl;

#define FIELD_PRINT(f) cout << "  " #f ": " << kv.second.f << endl
    for (auto& kv : columns) {
      cout << "<" << endl;
      FIELD_PRINT(name);
      FIELD_PRINT(maxVersions);
      FIELD_PRINT(compression);
      FIELD_PRINT(inMemory);
      FIELD_PRINT(bloomFilterType);
      FIELD_PRINT(bloomFilterVectorSize);
      FIELD_PRINT(bloomFilterNbHashes);
      FIELD_PRINT(blockCacheEnabled);
      FIELD_PRINT(timeToLive);
      cout << ">" << endl;
    }
#undef FIELD_PRINT

    return 0;
  }
};

class CreateCommand : public Subcommand {
public:
  CreateCommand() : Subcommand("create") { }
  void Usage(const string& prefix) {
    cout << prefix << "create <table> /path/to/schema" << endl;
    cout << prefix << "create <table> (schema read from stdin)" << endl;
  }
  bool parseLine(ColumnDescriptor* column, const string& line) {
    size_t pos = line.find(":");
    if (pos == 0 || pos == string::npos) {
      return false;
    }
    string field(line, 0, pos);
    ++pos;
    while (pos < line.length() && isspace(line[pos])) {
      ++pos;
    }
    string value(line, pos);

    if (field == "name") {
      column->name = value;
    } else if (field == "maxVersions") {
      column->maxVersions = to<int32_t>(value);
    } else if (field == "compression") {
      column->compression = value;
    } else if (field == "inMemory") {
      column->inMemory = to<bool>(value);
    } else if (field == "bloomFilterType") {
      column->bloomFilterType = value;
    } else if (field == "bloomFilterVectorSize") {
      column->bloomFilterVectorSize = to<int32_t>(value);
    } else if (field == "bloomFilterNbHashes") {
      column->bloomFilterNbHashes = to<int32_t>(value);
    } else if (field == "blockCacheEnabled") {
      column->blockCacheEnabled = to<int32_t>(value);
    } else if (field == "timeToLive") {
      column->timeToLive = to<int32_t>(value);
    } else {
      return false;
    }

    return true;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() != 1 && args.size() != 2) {
      throw UsageException();
    }

    const string table(args[0]);

    int fd;
    if (args.size() == 1) {
      fd = STDIN_FILENO;
    }
    else {
      fd = open(args.at(1).c_str(), O_RDONLY);
      if (fd < 0) {
        PLOG(FATAL) << "Unable to open file: " << args[1];
      }
    }

    string contents(65536, '\0');
    size_t size = read(fd, &contents[0], contents.size());
    if (size <= 0 || size == contents.size()) {
      throw FatalException("Unable to read schema definition");
    }
    contents.resize(size);

    vector<StringPiece> lines;
    folly::split("\n", contents, lines);

    vector<ColumnDescriptor> columns;
    bool in_column_definition = false;
    ColumnDescriptor current_column;
    for (auto raw_line : lines) {
      string line = raw_line.toString();
      boost::trim(line);
      if (line.size() == 0) continue;
      if (line[0] == '#') continue;
      if (line == "<") {
        CHECK(!in_column_definition);
        in_column_definition = true;
      } else if (line == ">") {
        CHECK(in_column_definition);
        in_column_definition = false;
        CHECK(!current_column.name.empty());
        columns.push_back(current_column);
      } else {
        CHECK(in_column_definition);
        if (!parseLine(&current_column, line)) {
          return 1;
        }
      }
    }
    CHECK(!in_column_definition);

    client->createTable(table, columns);
    cout << "Create successful!" << endl;

    return 0;
  }
};

class DeleteTableCommand : public Subcommand {
public:
  DeleteTableCommand() : Subcommand("drop") { }
  void Usage(const string& prefix) {
    cout << prefix << "drop <table>" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() != 1) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));

    try {
      client->deleteTable(table);
      cout << "Table deleted." << endl;
      return 0;
    }
    catch (const IOError& e) {
      cout << "Unable to drop table: " << e.message << endl;
    }
    return 1;
  }
};

class TableStatusCommand : public Subcommand {
public:
  TableStatusCommand() : Subcommand("status") { }
  void Usage(const string& prefix) {
    cout << prefix << "status <table>" << endl;
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() != 1) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));

    bool result = client->isTableEnabled(table);
    if (result) {
      cout << "Table is enabled." << endl;
    } else {
      cout << "Table is disabled." << endl;
    }

    return !result;
  }
};

class EnableDisableTableCommand : public Subcommand {
public:
  explicit EnableDisableTableCommand(bool enable) :
      Subcommand(enable ? "enable" : "disable"),
      enable_(enable) { }
  void Usage(const string& prefix) {
    if (enable_) {
      cout << prefix << "enable <table>" << endl;
    } else {
      cout << prefix << "disable <table>" << endl;
    }
  }
  int Execute(NativeHbaseClient* client, const vector<string>& args) {
    if (args.size() != 1) {
      throw UsageException();
    }

    const string table(extractTable(client, args, 0));

    if (enable_) {
      client->enableTable(table);
      cout << "Table enabled." << endl;
    } else {
      client->disableTable(table);
      cout << "Table disabled." << endl;
    }

    return 0;
  }
private:
  bool enable_;
};

static vector<CommandGroup*> extension_command_groups;
void RegisterExtensionCommands(CommandGroup* group) {
  extension_command_groups.push_back(group);
}

static ConnectionMaker default_connection_maker =
              [](const string& zk) { return new NativeHbaseClient(zk); };

void RegisterConnectionMaker(ConnectionMaker maker) {
  static bool first_invocation = true;
  CHECK(first_invocation)
    << "Only one call to RegisterConnectionMaker is allowed; are you linking "
    << "incompatible hbc extensions?";
  first_invocation = false;
  default_connection_maker = maker;
}

void RegisterSpecialRowPrinter(RowPrinter printer) {
  row_printers.push_back(printer);
}

} } // namespace facebook::hbase

namespace h = facebook::hbase;

void Usage(const vector<h::CommandGroup*>& command_groups) {
  cout << usage << endl;
  if (!FLAGS_zookeeper.empty()) {
    cout << "Current ZooKeeper instance: " << FLAGS_zookeeper << endl;
  }
  else {
    cout << "No ZooKeeper instance set via command line or environment."
         << endl;
  }
  cout << endl;

  cout << "Subcommands:" << endl;
  for (h::CommandGroup* group : command_groups) {
    cout << "  " << group->name << ": " << endl;
    for (h::Subcommand* command : group->commands) {
      command->Usage("    hbc ");
    }
    cout << endl;
  }
}

// This function is implemented externally; it is different for
// Facebook to tie in our hooks but generally will register new
// subfunctions and connection creation objects.
namespace facebook { namespace hbase {
void initHbcExtensions();
#if NHC_OPEN_SOURCE
void initHbcExtensions() { }
#endif
} }

int main(int argc, char **argv) {
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::hbase::initHbcExtensions();

  vector<h::CommandGroup*> command_groups;

  h::CommandGroup* inspection = new h::CommandGroup("Table/Cell inspection");
  inspection->addSubcommand(new h::TablesCommand());
  inspection->addSubcommand(new h::SchemaCommand());
  inspection->addSubcommand(new h::RegionsCommand());
  inspection->addSubcommand(new h::RegionServersCommand());
  command_groups.push_back(inspection);

  h::CommandGroup* data = new h::CommandGroup("Data manipulation");
  data->addSubcommand(new h::ScanCommand());
  data->addSubcommand(new h::GetCommand());
  data->addSubcommand(new h::SetCommand());
  data->addSubcommand(new h::DeleteCommand());
  command_groups.push_back(data);

  h::CommandGroup* schema = new h::CommandGroup("Schema manipulation");
  schema->addSubcommand(new h::CreateCommand());
  schema->addSubcommand(new h::DeleteTableCommand());
  schema->addSubcommand(new h::EnableDisableTableCommand(true));
  schema->addSubcommand(new h::EnableDisableTableCommand(false));
  schema->addSubcommand(new h::TableStatusCommand());
  command_groups.push_back(schema);

  // Extension groups always come after core groups.
  std::copy(h::extension_command_groups.begin(),
            h::extension_command_groups.end(),
            std::back_inserter(command_groups));

  if (argc < 2) {
    Usage(command_groups);
    return 1;
  }

  vector<string> args(argc - 2);
  copy(&(argv[2]), &(argv[argc]), args.begin());

  for (h::CommandGroup* group : command_groups) {
    for (h::Subcommand* command : group->commands) {
      if (command->name == argv[1]) {
        try {
          unique_ptr<h::NativeHbaseClient> client(
            h::default_connection_maker(FLAGS_zookeeper));
          if (!client->connect()) {
            cerr << "Unable to connect to zookeeper: " << FLAGS_zookeeper
                 << endl;
            return 2;
          }
          return command->Execute(client.get(), args);
        } catch (const h::UsageException& e) {
          command->Usage("Error: Usage: hbc ");
          return 1;
        } catch (const h::FatalException& e) {
          cerr << e.what() << endl;
          return 2;
        }
      }
    }
  }

  Usage(command_groups);

  return 1;
}

