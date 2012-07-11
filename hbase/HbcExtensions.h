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

// This file describes the interface for extending hbc.  It contains
// the necessary classes for defining commands and command groups as
// well as registering special printers and alternative connection
// classes (for using derivations of NativeHbaseClient).

#ifndef HBASE_SRC_HBCEXTENSIONS_H
#define HBASE_SRC_HBCEXTENSIONS_H

#include <string>
#include <vector>

#include "boost/noncopyable.hpp"

#include "hbase/src/NativeHbaseClient.h"

namespace facebook { namespace hbase {

namespace h = facebook::hbase;
using std::string;

// A simple set of classes to handle subcommands, grouped by theme.
// Each subcommand such as "get" or "scan" is an instance of a
// subclass of Subcommand.  Whenever an instance of Subcommand is
// created, it adds itself to the most recently created command
// group's commands vector, which is used for iteration over all
// commands (such as for usage or to dispatch the command).
class CommandGroup;

// Thrown during execution when there is an issue with the supplied
// arguments.
class UsageException : public std::exception { };

// A simple exception that indicates something went wrong that is not
// recoverable.  The intention is for the message to be printed (with
// nothing else) and the process terminate.
class FatalException : public std::exception {
public:
  explicit FatalException(const string& s) : what_(s) { }
  virtual ~FatalException() throw() { }
  virtual const char* what() const throw() {
    return what_.c_str();
  }
private:
  const string what_;
};

class Subcommand;
class CommandGroup : boost::noncopyable {
public:
  explicit CommandGroup(const string& name_in) : name(name_in) { }
  void addSubcommand(Subcommand* subcommand) {
    commands.push_back(subcommand);
  }

  const string name;
  vector<Subcommand*> commands;
};

// Subcommand itself; very simple, just two virtual methods and a
// string name.  Each subcommand will subclass this class.
class Subcommand : boost::noncopyable {
public:
  explicit Subcommand(const string& name_in) : name(name_in) { }

  const string extractTable(h::NativeHbaseClient* client,
                            const vector<string>& args, int pos) {
    if (pos >= args.size()) {
      throw FatalException("Table not specified");
    }

    const string &table = args[pos];
    vector<string> all_tables;
    client->listTables(&all_tables);
    auto it = find(all_tables.begin(), all_tables.end(), table);
    if (it == all_tables.end()) {
      throw FatalException("Table does not exist: " + table);
    }

    return table;
  }

  virtual ~Subcommand() { }

  virtual void Usage(const string& prefix) = 0;
  virtual int Execute(h::NativeHbaseClient* client,
                      const vector<string>& args) = 0;

  const string name;
};

typedef std::function<h::NativeHbaseClient*(const string&)> ConnectionMaker;
typedef std::function<bool(const h::TRowResult*)> RowPrinter;
void RegisterExtensionCommands(CommandGroup* group);
void RegisterConnectionMaker(ConnectionMaker maker);
void RegisterSpecialRowPrinter(RowPrinter printer);

} } // facebook::hbase

#endif // HBASE_SRC_HBCEXTENSIONS_H
