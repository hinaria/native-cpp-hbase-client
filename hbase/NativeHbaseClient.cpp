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

#include "hbase/src/NativeHbaseClient.h"
#include "hbase/src/NativeHbaseClient-inl.cpp"

#include <algorithm>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "folly/Bits.h"
#include "folly/Logging.h"
#include "folly/Conv.h"
#include "folly/String.h"
#include "folly/ScopeGuard.h"
#include "folly/String.h"

using namespace std;
using apache::hadoop::hbase::thrift::IOError;
using apache::hadoop::hbase::thrift::IllegalArgument;
using folly::Endian;
using folly::makeGuard;

DEFINE_int32(hbase_port, 9091, "Port to use when connecting to Hbase servers");
DEFINE_int32(hbase_metadata_rows_per_fetch, 65536,
             "default number of rows to fetch per RPC for hbase metadata");
DEFINE_int32(thrift_proxy_port, 9090, "Port the HBase Thrift Proxy runs on");
DEFINE_bool(force_thrift_proxy_mode, false, "if set, communicate with standard "
            "thrift proxies rather than zookeeper and region servers.  Do "
            "not enable unless absolutely necessary!  Experimental!");
DEFINE_int32(max_retained_connections_per_region_server, 2,
             "maximum number of connections to retain/cache per region server "
             "(note: actual number of connections may surpass this, up to "
             "--max_total_connections_per_region_server, to avoid blocking "
             "threads)");
DEFINE_int32(max_total_connections_per_region_server, 0,
             "maximum number of connections to cache per region server "
             "(hard limit, 0 for unlimited; will refuse connections if the "
             "total number of connections would surpass this amount)");
DEFINE_int32(hbase_max_row_buffer_size, 512,
             "maximum number of rows to buffer client side");
DEFINE_string(hbase_thrift_transport, "framed",
              "default transport to use when connecting to region servers; "
              "valid values: buffered header framed compact_framed");
DEFINE_string(hbase_thrift_compression, "none",
              "use compression for thrift communication; only valud if "
              "--hbase_thrift_transport is 'header'.  Valid options: "
              "none zlib");
DEFINE_string(zookeeper_hbase_root_path, "/hbase/root-region-server",
              "zookeeper znode containing the root hbase path");
DEFINE_string(zookeeper_hbase_region_server_path, "/hbase/rs",
              "zookeeper znode containing region servers");
DEFINE_int32(zookeeper_reconnect_standoff_ms, 5000, "minimum time between "
             "attempts to connect to zookeeper");
DEFINE_bool(zookeeper_fast_fail, true, "if set, zookeeper reconnects are "
            "either instant or fail; if not set, retries will block");
DEFINE_int32(zookeeper_port, 2181, "port for communicating with zookeeper");
DEFINE_int32(hbase_default_timeout_ms, 60000,
             "timeout, in ms, for hbase RPCs");

namespace facebook { namespace hbase {

TableNotFound::TableNotFound(const string& table, const string& row)
  : NativeHbaseException() {
  message_ = folly::stringPrintf("Table not found: %s (row %s)",
                                 table.c_str(), humanify(row).c_str());
}

RegionServerConnectionError::RegionServerConnectionError(
  const string& host, int port) : NativeHbaseException() {
  message_ = folly::stringPrintf("Could not connect to region server: %s:%d",
                                 host.c_str(), port);
}

// This class is basically just a struct (for now).  It contains the
// start and end key for a given region, the host, and our internal
// region number.  It is immutable.
class RegionServerInfo : private boost::noncopyable {
public:
  RegionServerInfo(const TRegionInfo& r, int region_num) :
      start_row(r.startKey), end_row(r.endKey), host_valid(true),
      host(r.serverName), port(r.port), region_num(region_num)
    { }

  const string start_row;
  const string end_row;
  const bool host_valid;
  const string host;
  const int port;
  const int region_num;
private:
};


// A simple helper class for grouping a set of "things" by the region
// server that serves a set of those "things".  In this case, "things"
// is any type that has a public "row" member (such as a
// BatchMutation).  Once constructed, this grouping can be iterated
// over, resulting in one entry for all "things" served by the same
// connection.
//
// For now, this is only used for RowMutation but it can be extended
// for any kind of operation we wish to batch per region server.
// Eventually it will be possible to dispatch operations in parallel
// to different servers.
template<typename T>
const string& extractKey(const T& t) { return t.row; }
template<>
const string& extractKey<string>(const string& s) { return s; }

template<typename T>
class ConnectionGroup : private boost::noncopyable {
public:
  typedef std::unordered_map<const RegionServerInfo*, vector<T> > Group;
  typedef typename Group::const_iterator Iter;

  ConnectionGroup(NativeHbaseClient* client,
                  const string& table,
                  const vector<T>& entities) : client_(client) {
    group_.clear();
    for (const T& e : entities) {
      auto conn = client->findRegion(table, extractKey<T>(e), false);
      regions_known_.push_back(conn);
      group_[conn.get()].push_back(e);
    }
  }

  ~ConnectionGroup() {
  }

  Iter begin() {
    return group_.begin();
  }

  Iter end() {
    return group_.end();
  }

private:
  NativeHbaseClient* client_;
  Group group_;
  vector<shared_ptr<const RegionServerInfo> > regions_known_;
};

// Record RegionServerInfo
shared_ptr<const RegionServerInfo>
TableRegions::addRegionServerInfo(const TRegionInfo& region,
                                  std::atomic<uint32_t>* num_regions) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  auto ret = tableRegions_[region.startKey];
  if (ret.get() == NULL) {
    // Not in the table, add it.
    ret.reset(new RegionServerInfo(region, ++*num_regions));
    tableRegions_[region.startKey] = ret;
  }
  return ret;
}

// Find TableRegions in the map
shared_ptr<const RegionServerInfo>
TableRegions::findRegionServerInfoInCache(const string& key) const {
  std::lock_guard<std::mutex> lock(map_mutex_);
  // Maybe we got lucky and hit the exact row; return it.
  auto iter = tableRegions_.lower_bound(key);
  if (iter != tableRegions_.end()) {
    if (key >= iter->second->start_row &&
        (key <= iter->second->end_row || iter->second->end_row.empty())) {
      return iter->second;
    }
  }

  // Otherwise the entry we found was the first one after the row we
  // sought; so, move the iterator back, unless we're already at the
  // first row, which means the row isn't in the table in question/
  if (iter == tableRegions_.begin()) {
    return shared_ptr<const RegionServerInfo>();
  }
  --iter;
  if (key >= iter->second->start_row &&
      (key <= iter->second->end_row || iter->second->end_row.empty())) {
    return iter->second;
  }
  return shared_ptr<const RegionServerInfo>();
}

void TableRegions::removeRegionServerInfo(const string& key) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  tableRegions_.erase(key);
}

// Given a string of the form host:port, split the host and port into
// host_out and port_out.  Returns true if the input string has the
// right form.
//
// Magic processing is based on hbase 89's
// org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper's
// removeMetaData method.

static bool ParseHostPort(const string& hostport, string* host_out,
                          int* port_out) {
  // hbase 89 uses some magic "gunk" around the data it reads and
  // writes to zookeeper; the indicator of this gunk is the first byte
  // being the magic byte, 0xff.  If we encounter this signifier, we
  // parse as per removeMetaData.  If we do not, we treat it directly
  // as host:port (consistent with hbase 90).
  const char RECOVERABLE_ZOOKEEPER_MAGIC = 0xff;
  string real_hostport = hostport;
  if (real_hostport[0] == RECOVERABLE_ZOOKEEPER_MAGIC) {
    const int32_t MAGIC_OFFSET = sizeof(RECOVERABLE_ZOOKEEPER_MAGIC);
    const int32_t ID_OFFSET = sizeof(int32_t);
    const int32_t id_length =
      Endian::big32(*reinterpret_cast<const int32_t*>(hostport.data() +
                                                    MAGIC_OFFSET));
    const int32_t data_length =
      hostport.size() - MAGIC_OFFSET - ID_OFFSET - id_length;
    const int32_t data_offset = MAGIC_OFFSET + ID_OFFSET + id_length;
    real_hostport = string(hostport.data() + data_offset, data_length);
  }
  vector<string> pieces;
  // Format: hbase 89 and 90 use a colon-separates 2-tuple of
  // host:port.  hbase 92 uses a 3-tuple of host,port,timestamp.
  folly::split(",", real_hostport, pieces);
  if (pieces.size() == 1) {
    // No commas, that means hbase 89/90 that is just host:port.
    pieces.clear();
    folly::split(":", real_hostport, pieces);
  }
  if (pieces.size() < 2) return false;

  *host_out = pieces[0];
  *port_out = to<int>(pieces[1]);
  return true;
}

const char* NativeHbaseClient::kWormholeHlogAttribute = "_walMeta";

static NativeHbaseConnection*
make_connection(const NativeHbaseClient* client,
                const HostMapKey& key) {
  auto ret = new NativeHbaseConnection(key.first, key.second);
  if (!ret->connect(client->transport(), client->timeout())) {
    client->getCounters()->incrementCounter("connect_failures");
    LOG(ERROR) << "Connect to region server failed: "
               << key.first << ":" << key.second;
    delete ret;
    return NULL;
  }
  client->getCounters()->incrementCounter("connect_successes");
  return ret;
}

static HostMapKey
key_from_connection(const NativeHbaseConnection* conn) {
  return make_pair(conn->host(), conn->port());
}

NativeHbaseClient::NativeHbaseClient(const string& hosts,
                                     HbaseClientTransport transport)
    : healthy_(false), hosts_(hosts),
      zh_(NULL), transport_(transport), root_conn_(nullptr), num_regions_(0),
      rng_(time(NULL)),
      connection_pool_(FLAGS_max_retained_connections_per_region_server,
                       FLAGS_max_total_connections_per_region_server,
                       [=](const HostMapKey& key) {
                         return make_connection(this, key);
                       },
                       key_from_connection) {
  timeout_ms_ = FLAGS_hbase_default_timeout_ms;
  if (FLAGS_force_thrift_proxy_mode) {
    thrift_port_ = FLAGS_thrift_proxy_port;
  } else {
    thrift_port_ = FLAGS_hbase_port;
  }

  if (transport_ == DEFAULT_TRANSPORT) {
    if (FLAGS_hbase_thrift_transport == "buffered") {
      transport_ = BUFFERED_TRANSPORT;
    } else if (FLAGS_hbase_thrift_transport == "framed") {
      transport_ = FRAMED_TRANSPORT;
    } else if (FLAGS_hbase_thrift_transport == "compact_framed") {
      transport_ = COMPACT_FRAMED_TRANSPORT;
    }
#if !NHC_OPEN_SOURCE
    else if (FLAGS_hbase_thrift_transport == "header") {
      transport_ = HEADER_TRANSPORT;
      if (FLAGS_hbase_thrift_compression == "zlib") {
        transport_ = HEADER_TRANSPORT_WITH_ZLIB;
      }
    }
#endif
    else {
      LOG(FATAL) << "Invalid --hbase_thrift_transport: "
                 << FLAGS_hbase_thrift_transport << ", must be "
                 << "buffered or framed";
    }
  }
}

void NativeHbaseClient::setCounter(shared_ptr<CounterBase> counter) {
  stats_counters_ = counter;
}

static void empty_watcher(zhandle_t *zh, int type, int state, const char *path,
                          void *context) {}


bool
NativeHbaseClient::expandConnectHosts(const string& specified_hosts,
                                      string* host_port_string,
                                      HostPortVector* host_ports) {
  vector<string> pieces;
  folly::split(',', specified_hosts, pieces);
  for (const auto& host_port_str : pieces) {
    vector<string> host_port_pieces;
    int port;
    folly::split(":", host_port_str, host_port_pieces);
    if (host_port_pieces.size() == 1) {
      port = FLAGS_zookeeper_port;
    } else if (host_port_pieces.size() == 2) {
      port = to<int>(host_port_pieces[1]);
    } else {
      LOG(ERROR) << "host:port looks malformed: " << host_port_str;
      return false;
    }
    host_ports->push_back(make_pair(host_port_pieces[0], port));
  }

  host_port_string->clear();
  for (int i = 0; i < host_ports->size(); ++i) {
    if (i > 0) host_port_string->append(",");
    host_port_string->append(
      folly::stringPrintf(
        "%s:%d", (*host_ports)[i].first.c_str(), (*host_ports)[i].second));
  }

  return true;
}

bool NativeHbaseClient::connectUnproteced() {
  if (!stats_counters_.get()) {
    stats_counters_.reset(new SimpleCounter());
  }

  CHECK(!root_conn_mutex_.try_lock()) << "root connection mutex must be held!";
  auto now = system_clock::now();

  if (now - last_zookeeper_connect_time_ <
      milliseconds(FLAGS_zookeeper_reconnect_standoff_ms)) {
    if (FLAGS_zookeeper_fast_fail) {
      VLOG(1) << "Refusing to attempt reconnect to zookeeper, too recent";
      return false;
    } else {
      VLOG(1) << "zk fastfail disabled, sleeping until allowed reconnect";
      std::this_thread::sleep_until(
        milliseconds(FLAGS_zookeeper_reconnect_standoff_ms + 1) +
        last_zookeeper_connect_time_);
        now = system_clock::now();
    }
  }
  last_zookeeper_connect_time_ = now;

  string hosts;
  if (!expandConnectHosts(hosts_, &hosts, &host_ports_)) {
    LOG(ERROR) << "Unable to resolve specified host string: "
               << hosts_;
    return false;
  }

  if (!FLAGS_force_thrift_proxy_mode) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
    zh_ = zookeeper_init(hosts.c_str(), empty_watcher, 10000, 0, 0, 0);
    if (!zh_) {
      LOG(ERROR) << "Unable to connect to zookeeper";
      return false;
    }
  }

  return initRootConnection();
}

bool NativeHbaseClient::connect() {
  std::lock_guard<std::mutex> g1(root_conn_mutex_);
  return connectUnproteced();
}

bool NativeHbaseClient::initRootConnection() {
  CHECK(!root_conn_mutex_.try_lock()) << "root connection mutex must be held!";
  string host;
  int port;
  if (FLAGS_force_thrift_proxy_mode) {
    std::uniform_int_distribution<uint32_t> picker(0, host_ports_.size());
    const auto &chosen = host_ports_[picker(rng_)];
    host = chosen.first;
    port = chosen.second;
    LOG(INFO) << "Choosing " << host << ":" << port << " as fake root region "
              << "server";
  } else {
    if (!zh_) {
      if (!connectUnproteced()) {
        return false;
      }
      CHECK(zh_);
    }

    char contents[256];
    int contents_length = sizeof(contents);
    int result = zoo_get(zh_, FLAGS_zookeeper_hbase_root_path.c_str(), 0,
                         contents, &contents_length, NULL);
    if (result != ZOK) {
      LOG(ERROR) << "Error getting contents of "
                 << FLAGS_zookeeper_hbase_root_path << ", result :"
                 << zerror(result) << "; resetting zookeeper connection...";
      zookeeper_close(zh_);
      zh_ = NULL;
      return false;
    }

    if (!ParseHostPort(string(contents, contents_length), &host, &port)) {
      LOG(ERROR) << "Unable to parse contents of "
                 << FLAGS_zookeeper_hbase_root_path
                 << ": " << string(contents, contents_length);
      return false;
    }
    port = thrift_port_;
  }

  root_conn_.reset(new NativeHbaseConnection(host, port));

  if (root_conn_->connect(transport_, timeout_ms_)) {
    healthy_ = true;
    return true;
  } else {
    return false;
  }
}

NativeHbaseClient::~NativeHbaseClient() {
  if (zh_) {
    zookeeper_close(zh_);
  }

  if (VLOG_IS_ON(1)) {
    map<string, int64_t> stats;
    map<string, string> histogram_stats;
    stats_counters_->snapshot(&stats, &histogram_stats);
    VLOG(1) << "Client stats dump:";
    for (auto& it : stats) {
      VLOG(1) << "  " << it.first << ": " << it.second;
    }
    VLOG(1) << "Client histogram dump:";
    for (auto& it : histogram_stats) {
      VLOG(1) << "  " << it.first << ": " << it.second;
    }
  }
}

bool
NativeHbaseClient::listRegionServers(vector<pair<string, int>>* output) {
  output->clear();
  if (FLAGS_force_thrift_proxy_mode) {
    *output = host_ports_;
    return true;
  } else {
    CHECK(zh_);
    struct String_vector sv;
    sv.count = 0;  // all children
    int result = zoo_get_children(
      zh_, FLAGS_zookeeper_hbase_region_server_path.c_str(), 0, &sv);
    if (result != ZOK) {
      LOG(ERROR) << "Unable to retrieve list of region servers from zookeeper";
      return false;
    }
    for (int i = 0; i < sv.count; ++i) {
      vector<string> pieces;
      folly::split(',', sv.data[i], pieces);
      CHECK_GE(pieces.size(), 2) << "malformed region server entry: "
                                 << sv.data[i];
      output->push_back(make_pair(pieces[0],
                                  to<int>(pieces[1])));
    }
    deallocate_String_vector(&sv);
    return true;
  }
}

void NativeHbaseClient::listTables(vector<Text>* output) {
 invokeRootOperation<void>(
    this, "getTableNames",
    [=](HbaseClient* client) {
      client->getTableNames(*output);
    });
}

void NativeHbaseClient::getColumnDescriptors(
  map<string, ColumnDescriptor> *output, const string& table) {
  invokeRootOperation<void>(
    this, "getColumnDescriptors",
    [=](HbaseClient* client) {
      return client->getColumnDescriptors(*output, table);
    });
}

void NativeHbaseClient::createTable(
  const string& table, const vector<ColumnDescriptor>& columns) {
  invokeRootOperation<void>(
    this, "createTable",
    [=, &columns](HbaseClient* client) {
      return client->createTable(table, columns);
    });
}

void NativeHbaseClient::deleteTable(const string& table) {
  invokeRootOperation<void>(
    this, "deleteTable",
    [=](HbaseClient* client) {
      return client->deleteTable(table);
    });
}

void NativeHbaseClient::enableTable(const string& table) {
  invokeRootOperation<void>(
    this, "enableTable",
    [=](HbaseClient* client) {
      return client->enableTable(table);
    });
}

void NativeHbaseClient::disableTable(const string& table) {
  invokeRootOperation<void>(
    this, "disableTable",
    [=](HbaseClient* client) {
      return client->disableTable(table);
    });
}

bool NativeHbaseClient::isTableEnabled(const string& table) {
  return invokeRootOperation<bool>(
    this, "isTableEnabled",
    [=](HbaseClient* client) {
      return client->isTableEnabled(table);
    });
}

void NativeHbaseClient::listTableRegions(vector<TRegionInfo>* output,
                                         const string& table) {
  invokeRootOperation<void>(
    this, "getTableRegions",
    [=](HbaseClient* client) {
      return client->getTableRegions(*output, table);
    });
}

void NativeHbaseClient::getRow(vector<TRowResult>* output,
                               const string& table, const string& row) {
  stats_counters_->incrementCounter("queries");
  invokeRowOperation<void>(
    table, row, "getRow",
    [=](HbaseClient* client) {
      return client->getRow(*output, table, row);
    });
}

void NativeHbaseClient::getRowWithColumns(
  vector<TRowResult>* output,
  const string& table,
  const string& row,
  const vector<string>& column_specifiers) {
  stats_counters_->incrementCounter("queries");
  invokeRowOperation<void>(
    table, row, "getRowWithColumns",
    [=, &column_specifiers](HbaseClient* client) {
      return client->getRowWithColumns(*output, table, row, column_specifiers);
    });
}

void NativeHbaseClient::getRowWithColumnsTs(
  vector<TRowResult>* output,
  const string& table,
  const string& row,
  const vector<string>& column_specifiers,
  const int64_t timestamp) {
  stats_counters_->incrementCounter("queries");
  invokeRowOperation<void>(
    table, row, "getRowWithColumnsTs",
    [=, &column_specifiers](HbaseClient* client) {
      return client->getRowWithColumnsTs(*output, table, row,
                                         column_specifiers, timestamp);
    });
}

bool NativeHbaseClient::getRowsHelper(
  vector<TRowResult>* output,
  unordered_map<string, TRowResult*>* output_by_key,
  const string& table,
  const vector<string>& keys,
  const char* opname,
  GetRowsHelperOp op)
{
  stats_counters_->incrementCounter("queries");
  ConnectionGroup<string> mutations(this, table, keys);

  output->clear();
  // We want pointers into output to be stable across all passes, so
  // we pre-allocate the entries.  TODO(chip): make this API cleaner;
  // andrei suggests possibly using a boost::multi_index.  Other
  // options are making the api cleaner by having two calls (one with
  // a vector, one with a map) or making copies of values for the row
  // output_by_key results.
  output->reserve(keys.size());
  bool okay = true;
  for (ConnectionGroup<string>::Iter it = mutations.begin();
       it != mutations.end(); ++it) {
    bool failed_query = true;
    const vector<string>& keys = it->second;
    try {
      vector<TRowResult> batch_results;
      invokeRowOperation<void>(
        table, it->first->start_row, opname,
        [&op, &batch_results, &table, &keys](HbaseClient* client) {
          return op(client, batch_results, table, keys);
        });


      for (auto& result : batch_results) {
        output->push_back(result);
        if (output_by_key) {
          // output->back() is stable across inserts into output
          // because of the earlier resize.
          (*output_by_key)[result.row] = &output->back();
        }
      }

      if (output_by_key) {
        for (auto& key : keys) {
          // insert doesn't overwrite, so fill in all keys not present
          // in our result set with nullptr's.
          output_by_key->insert(make_pair(key, nullptr));
        }
      }

      failed_query = false;
    }
    catch (const IllegalArgument& e) {
      LOG(ERROR) << opname << " failed: " << e.message;
    }
    catch (const IOError& e) {
      LOG(ERROR) << opname << " failed: " << e.message;
    }
    catch (const std::exception& e) {
      LOG(ERROR) << opname << " failed: " << e.what();
    }

    if (failed_query && output_by_key) {
      for (auto& key : keys) {
        output_by_key->erase(key);
      }
    }

    okay &= !failed_query;
  }

  return okay;
}

bool NativeHbaseClient::getRows(
  vector<TRowResult>* output,
  unordered_map<string, TRowResult*>* output_by_key,
  const string& table,
  const vector<string>& keys)
{
  GetRowsHelperOp op =
    [](HbaseClient* client,
       vector<TRowResult>& batch_results,
       const string& table,
       const vector<string>& keys) {
    return client->getRows(batch_results, table, keys);
  };

  return getRowsHelper(output, output_by_key, table, keys, "getRows", op);
}

bool NativeHbaseClient::getRowsWithColumns(
  vector<TRowResult>* output,
  unordered_map<string, TRowResult*>* output_by_key,
  const string& table,
  const vector<string>& keys,
  const vector<string>& columns)
{
  GetRowsHelperOp op =
    [&columns](HbaseClient* client,
               vector<TRowResult>& batch_results,
               const string& table,
               const vector<string>& keys) {
    return client->getRowsWithColumns(batch_results, table,
                                      keys, columns);
  };

  return getRowsHelper(output, output_by_key, table, keys,
                       "getRowsWithColumns", op);
}

Scanner* NativeHbaseClient::getScanner(const string& table,
                                       const string& start_row,
                                       const vector<string>& columns) {
  auto region_info = findRegion(table, start_row, false);
  return new Scanner(this, table, start_row,
                     region_info->start_row, region_info->end_row, columns);
}

bool NativeHbaseClient::mutateRow(const string& table, const string& row,
                                  const vector<Mutation>& mutations) {
  const string empty_string;
  return mutateRow(table, row, mutations, empty_string);
}

bool NativeHbaseClient::mutateRow(const string& table, const string& row,
                                  const vector<Mutation>& mutations,
                                  const string& transaction_annotation) {
  NativeHbaseConnection* conn = findConnection(table, row, false);
  ScopeGuard g = makeGuard([=] { connection_pool_.release(conn); } );

  try {
    map<string, string> attributes;
    if (!transaction_annotation.empty()) {
      attributes[kWormholeHlogAttribute] = transaction_annotation;
    }
    invokeRowOperation<void>(
      table, row, "mutateRow",
      [=, &mutations, &attributes](HbaseClient* client) {
        return client->mutateRow(table, row, mutations, attributes);
      });
    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "mutateRow failed: " << e.message;
  }
  catch (const IllegalArgument& e) {
    LOG(ERROR) << "mutateRow failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "mutateRow failed: " << e.what();
  }
  return false;
}

bool NativeHbaseClient::mutateRowTs(const string& table, const string& row,
                                    const vector<Mutation>& mutations,
                                    const int64_t timestamp) {
  const string empty_string;
  return mutateRowTs(table, row, mutations, timestamp, empty_string);
}

bool NativeHbaseClient::mutateRowTs(const string& table, const string& row,
                                    const vector<Mutation>& mutations,
                                    const int64_t timestamp,
                                    const string& transaction_annotation) {
  try {
    map<string, string> attributes;
    if (!transaction_annotation.empty()) {
      attributes[kWormholeHlogAttribute] = transaction_annotation;
    }
    invokeRowOperation<void>(
      table, row, "mutateRowTs",
      [=, &mutations, &attributes](HbaseClient* client) {
        return client->mutateRowTs(table, row, mutations,
                                   timestamp, attributes);
      });

    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "mutateRowTs failed: " << e.message;
  }
  catch (const IllegalArgument& e) {
    LOG(ERROR) << "mutateRowTs failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "mutateRowTs failed: " << e.what();
  }
  return false;
}

bool NativeHbaseClient::mutateRows(const string& table,
                                   const vector<BatchMutation>& rowBatches) {
  const string empty_string;
  return mutateRows(table, rowBatches, empty_string);
}

bool NativeHbaseClient::mutateRows(const string& table,
                                   const vector<BatchMutation>& rowBatches,
                                   const string& transaction_annotation) {
  ConnectionGroup<BatchMutation> mutations(this, table, rowBatches);

  bool okay = true;
  for (ConnectionGroup<BatchMutation>::Iter it = mutations.begin();
       it != mutations.end(); ++it) {
    try {
      map<string, string> attributes;
      if (!transaction_annotation.empty()) {
        attributes[kWormholeHlogAttribute] = transaction_annotation;
      }
      invokeRowOperation<void>(
        table, it->first->start_row, "mutateRows",
        [=, &it, &attributes](HbaseClient* client) {
          return client->mutateRows(table, it->second, attributes);
        });
    }
    catch (const IllegalArgument& e) {
      LOG(ERROR) << "mutateRows failed: " << e.message;
      okay = false;
    }
    catch (const IOError& e) {
      LOG(ERROR) << "mutateRows failed: " << e.message;
      okay = false;
    }
    catch (const std::exception& e) {
      LOG(ERROR) << "mutateRows failed: " << e.what();
      okay = false;
    }
  }

  return okay;
}

bool NativeHbaseClient::mutateRowsTs(const string& table,
                                     const vector<BatchMutation>& rowBatches,
                                     const int64_t timestamp) {
  const string empty_string;
  return mutateRowsTs(table, rowBatches, timestamp, empty_string);
}

bool NativeHbaseClient::mutateRowsTs(const string& table,
                                     const vector<BatchMutation>& rowBatches,
                                     const int64_t timestamp,
                                     const string& transaction_annotation) {
  ConnectionGroup<BatchMutation> mutations(this, table, rowBatches);

  bool okay = true;
  for (ConnectionGroup<BatchMutation>::Iter it = mutations.begin();
       it != mutations.end(); ++it) {
    try {
      map<string, string> attributes;
      if (!transaction_annotation.empty()) {
        attributes[kWormholeHlogAttribute] = transaction_annotation;
      }
      invokeRowOperation<void>(
        table, it->first->start_row, "mutateRowsTs",
        [=, &it, &attributes](HbaseClient* client) {
          return client->mutateRowsTs(table, it->second,
                                      timestamp, attributes);
        });
    }
    catch (const IllegalArgument& e) {
      LOG(ERROR) << "mutateRowsTs failed: " << e.message;
      okay = false;
    }
    catch (const IOError& e) {
      LOG(ERROR) << "mutateRowsTs failed: " << e.message;
      okay = false;
    }
    catch (const std::exception& e) {
      LOG(ERROR) << "mutateRowsTs failed: " << e.what();
      okay = false;
    }
  }

  return okay;
}

bool NativeHbaseClient::deleteAll(const string& table, const string& row,
                                  const string& column) {
  try {
    invokeRowOperation<void>(
      table, row, "deleteAll",
      [=](HbaseClient* client) {
        client->deleteAll(table, row, column);
      });

    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "deleteAll failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "deleteAll failed: " << e.what();
  }
  return false;
}

bool NativeHbaseClient::deleteAllTs(const string& table, const string& row,
                                    const string& column,
                                    const int64_t timestamp) {
  try {
    invokeRowOperation<void>(
      table, row, "deleteAllTs",
      [=](HbaseClient* client) {
        client->deleteAllTs(table, row, column, timestamp);
      });

    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "deleteAllTs failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "deleteAllTs failed: " << e.what();
  }
  return false;
}

bool NativeHbaseClient::deleteAllRow(const string& table, const string& row) {
  const string empty_string;
  return deleteAllRow(table, row, empty_string);
}

bool NativeHbaseClient::deleteAllRow(const string& table, const string& row,
                                     const string& transaction_annotation) {
  try {
    map<string, string> attributes;
    if (!transaction_annotation.empty()) {
      attributes[kWormholeHlogAttribute] = transaction_annotation;
    }
    invokeRowOperation<void>(
      table, row, "deleteAllRow",
      [=, &attributes](HbaseClient* client) {
        client->deleteAllRow(table, row, attributes);
      });

    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "deleteAllRow failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "deleteAllRow failed: " << e.what();
  }
  return false;
}

bool NativeHbaseClient::deleteAllRowTs(const string& table, const string& row,
                                       const int64_t timestamp) {
  try {
    invokeRowOperation<void>(
      table, row, "deleteAllRowTs",
      [=](HbaseClient* client) {
        client->deleteAllRowTs(table, row, timestamp);
      });

    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "deleteAllRowTs failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "deleteAllRowTs failed: " << e.what();
  }
  return false;
}

int64_t NativeHbaseClient::atomicIncrement(const string& table,
                                           const string& row,
                                           const string& column,
                                           int64_t delta) {
  return invokeRowOperation<int64_t>(
    table, row, "atomicIncrement",
    [=](HbaseClient* client) {
      return client->atomicIncrement(table, row, column, delta);
    });
}

Scanner::Scanner(NativeHbaseClient* client, const string& table,
                 const string& start_row, const string&region_start_row,
                 const string& region_end_row, const vector<string>& columns)
    : state_(READY), client_(client), table_(table), columns_(columns),
      start_row_(start_row), region_start_row_(region_start_row),
      region_end_row_(region_end_row),
      region_scanner_(make_pair(static_cast<NativeHbaseConnection*>(NULL), -1)),
      row_buffer_capacity_(16), row_cursor_pos_(-1) {
}

Scanner::~Scanner() {
  if (region_scanner_.first) {
    client_->releaseConnection(region_scanner_.first);
  }
}

void Scanner::setEndRow(const string& end_row) {
  DCHECK_EQ(state_, READY);
  // The HBase server doesn't treat this as inclusive, so append a
  // null byte; there are no rows lexigraphically between end_row_ and
  // end_row_ + "\0" so this gets the result we want.
  end_row_ = end_row + string("", 1);
}

void Scanner::setRowBufferCapacity(const int n) {
  DCHECK_EQ(state_, READY);
  row_buffer_capacity_ = n;
}

void Scanner::scan() {
  DCHECK_EQ(state_, READY);
  if (state_ != READY) {
    state_ = ERROR;
    return;
  }

  // There are a few cases to consider with respect to the current
  // region's boundaries and end_row_ (if it is specified).
  //
  // If end_row_ isn't specified, scan to the end of this region.
  //
  // If end_row_ is specified, and it falls within the boundaries of
  // the current region, use it instead of the region end key.
  //

  string this_scan_end_row;
  if (region_end_row_.size() > 0) {
    this_scan_end_row = region_end_row_ + string("", 1);
  }

  if (end_row_.size() > 0) {
    if (start_row_ <= end_row_ &&
        (region_end_row_.empty() || end_row_ <= region_end_row_)) {
      this_scan_end_row = end_row_;
    }
  }

  region_scanner_ = client_->beginScanInternal(table_, columns_,
                                               start_row_, this_scan_end_row);
  state_ = SCANNING;
}

void Scanner::advanceRegion() {
  client_->releaseConnection(region_scanner_.first);

  // If any code before the final assignment in this function throws
  // an exception, including the beginScanInternal call in the
  // assignment statement, we still have a raw pointer inside the
  // region_scanner_ pair that our destructor will release.  That
  // would be a double release!  Aiee.
  region_scanner_.first = NULL;

  // We begin the next row at the region containing the next key after
  // the end of this region.  Use the lexigraphic null byte trick again.
  const string next_row = region_end_row_ + string("", 1);
  auto region_info =
    client_->findRegion(table_, next_row, false);
  region_start_row_ = region_info->start_row;
  region_end_row_ = region_info->end_row;

  string this_scan_end_row = region_end_row_;
  if (this_scan_end_row.size() > 0) {
    this_scan_end_row += string("", 1);
    if (end_row_.size() > 0) {
      this_scan_end_row = min(end_row_, this_scan_end_row);
    }
  }

  region_scanner_ = client_->beginScanInternal(table_, columns_,
                                               next_row, this_scan_end_row);
}

bool Scanner::next(TRowResult** output) {
  if (state_ != SCANNING) {
    return false;
  }
  // Has our cursor run past the end of the buffer?  If so, refill.
  if (row_cursor_pos_ >= row_buffer_.size()) {
    if (row_buffer_.size() < FLAGS_hbase_max_row_buffer_size) {
      row_buffer_capacity_ = std::min(2 * row_buffer_capacity_,
                                      FLAGS_hbase_max_row_buffer_size);
    }

    row_buffer_.clear();
    row_buffer_.reserve(row_buffer_capacity_);
    row_cursor_pos_ = 0;

    // Try to get rows from successive regions until we get rows or hit an error
    while (true) {
      if (!client_->getNextRows(&row_buffer_, region_scanner_,
                                row_buffer_capacity_)) {
        // we hit an error, terminate early
        state_ = ERROR;
        *output = NULL;
        return false;
      } else if (row_buffer_.empty()) {
        // No more rows in this region; either we are done, or we need
        // to advance to the next region.
        if (region_end_row_.empty() ||
            (!end_row_.empty() && region_start_row_ > end_row_)) {
          // No more regions!  We are done.
          *output = NULL;
          state_ = FINISHED;
          return false;
        }
        advanceRegion();
      } else {
        // The buffer has been refilled, carry on
        break;
      }
    }
  }

  *output = &(row_buffer_[row_cursor_pos_++]);
  if (!end_row_.empty() && (*output)->row > end_row_) {
    // Are we past our stop row?  Then we are done.
    *output = NULL;
    state_ = FINISHED;
    return false;
  }
  return true;
}

RegionScanner NativeHbaseClient::beginScanInternal(
  const string& table, const vector<string>& columns,
  const string& start_row, const string& end_row) {
  NativeHbaseConnection* conn = findConnection(table, start_row, false);
  ScopeGuard g = makeGuard([=] { connection_pool_.release(conn); } );

  ScannerID scanner_id = invokeThriftOperation<ScannerID>(
    this,
    conn,
    [=, &columns](HbaseClient* client) {
      return client->scannerOpenWithStop(table, start_row, end_row, columns);
    });

  g.dismiss();
  return make_pair(conn, scanner_id);
}

bool NativeHbaseClient::getNextRows(vector<TRowResult>* output,
                                    RegionScanner region_scanner,
                                    int num_rows) {
  try {
    invokeThriftOperation<void>(
      this,
      region_scanner.first,
      [=, &region_scanner](HbaseClient* client) {
        client->scannerGetList(*output, region_scanner.second, num_rows);
      });
    return true;
  }
  catch (const IOError& e) {
    LOG(ERROR) << "scannerGetlist failed: " << e.message;
  }
  catch (const IllegalArgument& e) {
    LOG(ERROR) << "scannerGetList failed: " << e.message;
  }
  catch (const std::exception& e) {
    LOG(ERROR) << "scannerGetList failed: " << e.what();
  }
  return false;
}

NativeHbaseConnection* NativeHbaseClient::connectionForRegionServer(
  const RegionServerInfo* region_server_info) {
  HostMapKey key(region_server_info->host, thrift_port_);
  auto ret = connection_pool_.lookup(key);
  if (ret == NULL) {
    throw RegionServerConnectionError(region_server_info->host,
                                      thrift_port_);
  }
  return ret;
}

shared_ptr<const RegionServerInfo>
NativeHbaseClient::findRegion(const string& table,
                              const string& row,
                              bool ignore_cache) {
  regions_mutex_.lock();
  TableMap::const_iterator it = regions_.find(table);
  TableRegions* table_entry = NULL;
  if (it == regions_.end()) {
    regions_[table].reset(new TableRegions);
    table_entry = regions_[table].get();
  } else {
    table_entry = it->second.get();
  }
  regions_mutex_.unlock();
  assert(table_entry);

  shared_ptr<const RegionServerInfo> ret =
    table_entry->findRegionServerInfoInCache(row);

  if (ret.get() && !ignore_cache) {
    VLOG(1) << "Cache hit finding region server for " << table
            << " row " << humanify(row) << " (region range is "
            << humanify(ret->start_row) << "-"
            << humanify(ret->end_row) << ")";
    return ret;
  }

  VLOG(1) << "Finding region server for " << table << " row "
          << humanify(row);

  TRegionInfo region;
  static const string kNines = "99999999999999";  // from HConstats.java
  string meta_row_key = table + "," + row + "," + kNines;

  try {
    VLOG(1) << "getRegionInfo " << humanify(meta_row_key);
    // TODO(chip): this should ask the region server containing the
    // correct region of META rather than the root client.
    invokeRootOperation<void>(
      this, "getRegionInfo",
      [=, &region](HbaseClient* client) {
        client->getRegionInfo(region, meta_row_key);
      });
  }
  catch (const IOError& e) {
    LOG(ERROR) << "getRegionInfo failed: " << e.message;
    throw TableNotFound(table, row);
  }

  assert(row >= region.startKey);

  if (row <= region.endKey || region.endKey.empty()) {
    if (ret.get()) {
      table_entry->removeRegionServerInfo(ret->start_row);
    }
    ret = table_entry->addRegionServerInfo(region, &num_regions_);
  }

  if (!ret) {
    throw TableNotFound(table, row);
  }

  return ret;
}

}}  // namespace

std::ostream& operator<<(std::ostream& out,
                         const std::pair<std::string, int>& key) {
  out << key.first << ":" << key.second;
  return out;
}

