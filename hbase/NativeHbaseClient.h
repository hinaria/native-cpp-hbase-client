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

// @author Chip Turner (chip@fb.com)
//
// This is the "Native" HBase client; specifically, it does not
// require the use of the Java Thrift proxy (aka ThriftServer);
// instead, it communicates directly with the servers containing data.
//
// See hbc.cpp for example usage.
//
// This library is designed to be thread safe.  A single connection
// can be instantiated against a given hbase cluster, and this
// connection can safely be used across threads without external
// locking.  Scanners should not be shared across threads; if a thread
// begins a scan, the scanner operation should be completed on that
// thread.  Other threads can acquire their own scanners.  Multiple
// scanners can be used in parallel from the same connection.
//
// DANGER: This client is a work in progress.  If something breaks,
// you get to keep both pieces.
//
// On the use of Mutexes in the implementation of this class: Mutexes
// are rarely held concurrently, but then they are, the order should
// be:
//
// root_conn_mutex_ -> regions_mutex_

#ifndef HBASE_SRC_NATIVEHBASECLIENT_H_
#define HBASE_SRC_NATIVEHBASECLIENT_H_

#include "hbase/nhc-config.h"

#if NHC_OPEN_SOURCE
#include <zookeeper/zookeeper.h>
#else
#include <c-client-src/zookeeper.h>
#endif

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/shared_ptr.hpp>

#include "folly/Conv.h"
#include "folly/ScopeGuard.h"
#include "hbase/Hbase.h"
#include "hbase/BaseHbaseConnection.h"
#include "hbase/ConnectionPool.h"
#include "hbase/Counters.h"
#include "concurrency/Mutex.h"
#include "Thrift.h"

namespace facebook { namespace hbase {

using boost::shared_ptr;

using apache::hadoop::hbase::thrift::AlreadyExists;
using apache::hadoop::hbase::thrift::BatchMutation;
using apache::hadoop::hbase::thrift::ColumnDescriptor;
using apache::hadoop::hbase::thrift::HbaseClient;
using apache::hadoop::hbase::thrift::IOError;
using apache::hadoop::hbase::thrift::IllegalArgument;
using apache::hadoop::hbase::thrift::Mutation;
using apache::hadoop::hbase::thrift::ScannerID;
using apache::hadoop::hbase::thrift::TCell;
using apache::hadoop::hbase::thrift::TRegionInfo;
using apache::hadoop::hbase::thrift::TRowResult;
using apache::hadoop::hbase::thrift::Text;
using apache::thrift::concurrency::Mutex;
using apache::thrift::transport::TTransportException;
#if !NHC_OPEN_SOURCE
using apache::thrift::TLibraryException;
#endif
using folly::humanify;
using folly::ScopeGuard;
using folly::to;
using std::pair;
using std::string;
using std::vector;
using std::unordered_map;
using std::unique_ptr;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::system_clock;

class FacebookHbaseClient;
class NativeHbaseClient;
class RegionServerInfo;

#if NHC_OPEN_SOURCE
typedef TException NativeHbaseException;
#else
typedef TLibraryException NativeHbaseException;
#endif

// Exceptions beyond that which may be thrown by the thrift layers.
// TODO(chip): perhaps we should have a base class for all exceptions
// the client may throw, and wrap thrift exceptions in them?  Unclear
// at this time.
class TableNotFound : public NativeHbaseException {
public:
  TableNotFound(const string& table, const string& row);
};

class RegionServerConnectionError : public NativeHbaseException {
public:
  RegionServerConnectionError(const string& host, int port);
};

// map: first_row -> regionserver with lock protection
class TableRegions : private boost::noncopyable {
public:
  /**
   * Add the region to map. If the region exist in the map, just return the
   * exist data
   * @param region for creating the RegionServerInfo
   * @param num_regions current num of region in current TableMap
   * @returns RegionServerInfo either created or in the map
   */
  shared_ptr<const RegionServerInfo> addRegionServerInfo(
    const TRegionInfo& region,
    std::atomic<uint32_t>* num_regions);

  /**
   * Find this key in the map
   * @param  key key to find the RegionServerInfo
   * @returns RegionServerInfo, otherwise NULL if not found
   */
  shared_ptr<const RegionServerInfo> findRegionServerInfoInCache(
    const string& key) const;

  /**
   * Remove the region server info for the specified start key.
   */
  void removeRegionServerInfo(const string& start_key);

private:
  mutable std::mutex map_mutex_;  // protect local map
  map<const string, shared_ptr<const RegionServerInfo> > tableRegions_;
};

// a vector of host:port's
typedef vector<pair<string, int>> HostPortVector;

// map: table name -> TableRegions
typedef unordered_map<string, unique_ptr<TableRegions>> TableMap;

// pair of host, port
typedef pair<string, int> HostMapKey;

// a handy type for our connections for most hbase RPCs
typedef BaseHbaseConnection<HbaseClient> NativeHbaseConnection;

// an active region scan operation
typedef pair<NativeHbaseConnection*, ScannerID> RegionScanner;

// our connection pool type (for brevity later)
typedef ConnectionPool<HostMapKey, NativeHbaseConnection> HbaseConnectionPool;

template<class T>
class ThriftOperation {
public:
  typedef std::function<T (HbaseClient*)> Type;
};

/**
 * class Scanner
 *
 * Represents a scan operation.  Returned by NativeHbaseClient and
 * used to move forward and access rows returned from the scan.
 */
class Scanner : private boost::noncopyable {
public:
  // No public constructor.
  ~Scanner();

  /// Set the last row to be returned; scan stops no later than this row.
  void setEndRow(const string& end_row);
  /// Set the number of rows to buffer; trade throughput for latency.
  void setRowBufferCapacity(const int n);

  /// Begin the scan itself.
  void scan();

  /**
   * Advance to the next row, returning false if there are no more rows.
   *
   * @param output set to the current row
   */
  bool next(TRowResult** output);

  /// Returns true iff the scanner encountered an error fetching rows.
  bool error() const { return state_ == ERROR; }

private:
  enum State {
    READY,
    SCANNING,
    ERROR,
    FINISHED
  };
  State state_;
  NativeHbaseClient* client_;     // Client used by this scanner.
  const string table_;            // table being scanned
  const vector<string> columns_;  // columns being returned
  const string start_row_;        // where the scan starts
  string end_row_;                // upper bound on rows returned
  string region_start_row_;       // first row in this current region
  string region_end_row_;         // last row in this current region

  RegionScanner region_scanner_;

  // We fetch multiple rows at once, but the API results in a single
  // row; we buffer them here.
  vector<TRowResult> row_buffer_;

  int row_buffer_capacity_;     // buffer capacity
  int row_cursor_pos_;          // current position in the row buffer

  // Move scanner to next region.
  void advanceRegion();

  /**
   * Private constructor used by NativeHbaseClient.
   *
   * @param client the client creating this scanner
   * @param start_row the row to begin the scan at
   * @param columns vector of columns to fetch
   */
  Scanner(NativeHbaseClient* client,
          const string& table,
          const string& start_row,
          const string& region_start_row,
          const string& region_end_row,
          const vector<string>& columns);

  friend class NativeHbaseClient;
};


/**
 * class NativeHbaseClient
 *
 * The C++ client initiates communication with any region server; from
 * there, it locates the relevant metadata to find regions for
 * particular rows.  The host:port specified in the constructor is
 * effectively just a boostrapping region server.
 */
class NativeHbaseClient : private boost::noncopyable {
public:
  /**
   * Constructor.
   *
   * @param smc_tier_or_hosts either an smc tier or a comma separated
   *        list of host:port for our zookeeper instance
   * @param transport a HbaseClientTransport indicating the transport for Thrift
   */
  explicit NativeHbaseClient(
    const string& smc_tier_or_hosts,
    HbaseClientTransport transport = DEFAULT_TRANSPORT);
  virtual ~NativeHbaseClient();

  /**
   * A wrapper that invoked a ThriftOperation for a given table/row.
   * This will retry once if an error is encountered, display a
   * message, etc.
   */
  template<class T>
  T invokeRowOperation(const string& table, const string& row,
                       const char* opname,
                       typename ThriftOperation<T>::Type op);

  /**
   * Similar to the above, but this operation is invoked on the root client.
   */
  template<class T>
  T invokeRootOperation(NativeHbaseClient* client, const char* opname,
                        typename ThriftOperation<T>::Type op);

  /**
   * Log an error message.  Intended to be overridden by subclasses
   * and used for custom logging purposes.
   */
  virtual void logHbaseError(
    const char* operation, const string& remote_host, int remote_port,
    const string& row_key, int timeout_ms, const string& msg) {
    return;
  }

  /**
   * Log an RPC.  Intended to be overridden by subclasses and used for
   * custom logging purposes.
   */
  virtual void logHbaseOperation(
    const char* operation, const string& remote_host, int remote_port,
    const string& row_key, int timeout_ms) {
    return;
  }

  /**
   * Retrieve the list of region servers.
   *
   * @param output the output vector of host:port
   */
  bool listRegionServers(HostPortVector* output);

  /**
   * Connect to the specified zookeeper instance.
   *
   * @returns true if successful
   */
  bool connect();

  /**
   * Use a new counter object for stats tracking; only valid before
   * connect() is called and can only be called once.  A shared_ptr is
   * used as the caller may or may not keep ownership (ie, you may or
   * may not want to share counters among multiple clients).
   *
   * This can be called at any time, but it WILL segfault if you call
   * it while another thread is using the client, so serialization is
   * up to the caller (typically this would be done before many
   * queries are run).
   *
   * @returns nothing
   */
  void setCounter(shared_ptr<CounterBase> counter);

  /**
   * Determine if this client is healthy
   *
   * A client is unhealthy in general if it has had protocol-level
   * errors when conneting to the root region server.
   *
   * @returns true if healthy
   */
  bool isHealthy() { return healthy_; }

  /**
   * Set the timeout for RPC operations.
   *
   * @param ms timeout, in milliseconds
   */
  void setTimeout(int ms) {
    timeout_ms_ = ms;
  }

  int timeout() const {
    return timeout_ms_;
  }

  const HbaseClientTransport transport() const {
    return transport_;
  }

  // Some basic metadata APIs largely identical to their native
  // Hbase.thrift counterparts.

  /**
   * List tables in this Hbase cluster.
   *
   * @param output where the results are stored.
   */
  void listTables(vector<Text> *output);

  /**
   * Get the column descriptors for a table.
   *
   * @param output where the results are stored.
   */
  void getColumnDescriptors(map<string, ColumnDescriptor> *output,
                            const string& table);

  /**
   * Create a table.
   *
   * @param table the name of the table to create
   * @param columns a vector of ColumnDescriptor describing the table
   */
  void createTable(const string& table,
                   const vector<ColumnDescriptor>& columns);

  /**
   * Delete (drop) a table.
   *
   * @param table the name of the table to drop
   */
  void deleteTable(const string& table);

  /**
   * Get table status (enabled or disabled)
   *
   * @param table the name of the table to drop
   */
  bool isTableEnabled(const string& table);

  /**
   * Enable a table.
   *
   * @param table the name of the table to enable
   */
  void enableTable(const string& table);

  /**
   * Disable a table.
   *
   * @param table the name of the table to disable
   */
  void disableTable(const string& table);

  /**
   * List regions for a table.
   *
   * @param output where the results are stored.
   * @param table name of the table
   */
  void listTableRegions(vector<TRegionInfo> *output, const string& table);

  /**
   * Fetch a single row (point lookup)
   *
   * @param output where the results are stored.
   * @param table name of the table
   * @param key the rowkey to fetch
   */
  void getRow(vector<TRowResult>* output,
              const string& table, const string& key);

  /**
   * Fetch the specified columns of a  single row (point lookup)
   *
   * @param output where the results are stored.
   * @param table name of the table
   * @param key the rowkey to fetch
   * @param columns vector of the columns to fetch
   */
  void getRowWithColumns(vector<TRowResult>* output,
                         const string& table, const string& key,
                         const vector<string>& column_specifiers);

  /**
   * Fetch the specified columns of a single row (point lookup) with a
   * specified upper bound on the timestamp
   *
   * @param output where the results are stored.
   * @param table name of the table
   * @param key the rowkey to fetch
   * @param columns vector of the columns to fetch
   * @param timestamp upper bound for the timestamp to fetch
   */
  void getRowWithColumnsTs(vector<TRowResult>* output,
                           const string& table, const string& key,
                           const vector<string>& column_specifiers,
                           const int64_t timestamp);

  /**
   * Fetch multiple rows in a given table (point lookup).
   *
   * @param output where the results are stored.
   * @param output_by_key a map of pointers to TRowResults (see below)
   * @param table name of the table
   * @param keys the rowkeys to fetch
   * @returns true if all region servers could be queried successfully
   *
   * output_by_key is a peculiar parameter.  Unlike output, it can be
   * null'd.  If it is not null, then it will map queried keys to
   * individual entries in the output vector.  If a key is present and
   * the value is NULL, then the row had no result (ie, wasn't present
   * in hbase).  If a key queried wasn't present in the map at all, it
   * indicates there was an error and no information is known about
   * the row (ie, it may or may not exist in hbase).  This case only
   * occurs if getRows returns false.
   */
  bool getRows(vector<TRowResult>* output,
               unordered_map<string, TRowResult*>* output_by_key,
               const string& table,
               const vector<string>& keys);

  bool getRowsWithColumns(vector<TRowResult>* output,
                          unordered_map<string, TRowResult*>* output_by_key,
                          const string& table,
                          const vector<string>& keys,
                          const vector<string>& columns);
  /**
   * Create a scanner to return multiple rows.
   *
   * Get a scanner for the specified columns of the specified table,
   * starting at start_row.  column_specifiers is a vector of column
   * names (typically "family:column") or regular expressions used to
   * select columns.  TODO(chip): once the Thrift API supports the
   * new, non-colon delimited method of specifying columns, use that
   * here.
   *
   * @param table name of the table
   * @param start_row where the scan starts
   * @param column_specifiers which columns to return
   *
   * @returns a pointer to the scanner; caller must free it.
   */
  Scanner* getScanner(const string& table,
                      const string& start_row,
                      const vector<string>& column_specifiers);

  /**
   * Change a specific row in a table.
   *
   * Given a table and row, change the cells as specified in
   * mutations.
   *
   * @param table the table
   * @param row the row
   * @param mutations vector of columns, values, and delete flags to change
   * @param transaction_annotation free-form text annotating the transaction
   *
   * @returns true on success, false on failure
   */
  bool mutateRow(const string& table, const string& row,
                 const vector<Mutation>& mutations);

  bool mutateRow(const string& table, const string& row,
                 const vector<Mutation>& mutations,
                 const string& transaction_annotation);

  /**
   * Change a specific row in a table and set the timestamp.
   *
   * Given a table and row, change the cells as specified in
   * mutations.
   *
   * @param table the table
   * @param row the row
   * @param mutations vector of columns, values, and delete flags to change
   * @param timestamp timestamp to set for all affected cells
   * @param transaction_annotation free-form text annotating the transaction
   *
   * @returns true on success, false on failure
   */
  bool mutateRowTs(const string& table, const string& row,
                   const vector<Mutation>& mutations,
                   const int64_t timestamp);

  bool mutateRowTs(const string& table, const string& row,
                   const vector<Mutation>& mutations,
                   const int64_t timestamp,
                   const string& transaction_annotation);

  /**
   * Change a set of rows in the specified table.
   *
   * Given a table and a set of rows (and, for each row, a set of
   * columns) to change, modify the rows as appropriate.  This is
   * *NOT* transactional across rows.  Updates are batched per region
   * server and should be modestly efficient.
   *
   * @param table the table
   * @param rowBatches vector of BatchMutation operations
   * @param transaction_annotation free-form text annotating the transaction
   *
   * @returns true on success, false on failure of any of the many updates
   */
  bool mutateRows(const string& table, const vector<BatchMutation>& rowBatches);

  bool mutateRows(const string& table, const vector<BatchMutation>& rowBatches,
                  const string& transaction_annotation);

  /**
   * Change a set of rows in the specified table, setting the timestamp.
   *
   * Same as mutateRows, but also sets the timestamp to the specified value.
   *
   * @param table the table
   * @param rowBatches vector of BatchMutation operations
   * @param transaction_annotation free-form text annotating the transaction
   *
   * @returns true on success, false on failure of any of the many updates
   */
  bool mutateRowsTs(const string& table,
                    const vector<BatchMutation>& rowBatches,
                    const int64_t timestamp);

  bool mutateRowsTs(const string& table,
                    const vector<BatchMutation>& rowBatches,
                    const int64_t timestamp,
                    const string& transaction_annotation);

  /**
   * Atomically increment a given value.
   *
   * @param table the table
   * @param row the row
   * @param column the column
   * @param delta the amount to change the column by
   *
   * @returns the new value of the cell
   */
  int64_t atomicIncrement(const string& table, const string& row,
                          const string& column, int64_t delta);

  /**
   * Delete a specific column of a given row in the specified table.
   *
   * @param table the table
   * @param row the row
   * @param column the column
   *
   * @returns true on success, false on failure
   */
  bool deleteAll(const string& table, const string& row, const string& column);

  /**
   * Like deleteAll but only deletes cells with timestamps less than
   * the specified timestamp.
   *
   * @param table the table
   * @param row the row
   * @param column the column
   * @param timestamp delete all timestamps up to and including this
   *
   * @returns true on success, false on failure
   */
  bool deleteAllTs(const string& table, const string& row,
                   const string& column, const int64_t timestamp);

  /**
   * Delete a specific row in the specified table.
   *
   * @param table the table
   * @param row the row
   * @param transaction_annotation free-form text annotating the transaction
   *
   * @returns true on success, false on failure
   */
  bool deleteAllRow(const string& table, const string& row);

  bool deleteAllRow(const string& table, const string& row,
                    const string& transaction_annotation);

  /**
   * Like deleteAllRow, but only deletes cells with a timestamp less
   * than or equal to the specified timestamp.
   *
   * @param table the table
   * @param row the row
   * @param timestamp delete all timestamps up to and including this
   *
   * @returns true on success, false on failure
   */
  bool deleteAllRowTs(const string& table, const string& row,
                      const int64_t timestamp);

  // Indicate a scanner is finished using the connection.
  void releaseConnection(NativeHbaseConnection* connection) {
    connection_pool_.release(connection);
  }

  CounterBase* getCounters() const {
    return stats_counters_.get();
  }

  const string hosts() const {
    return hosts_;
  }

  // The attribute interpreted by the HBase region server as the
  // transaction annotation (aka wormhole comment).
  static const char* kWormholeHlogAttribute;

protected:
  // A helper function to expand specified_hosts into a
  // comma-separated list of host:port's corresponding to the
  // zookeeper instances we should contact.  The host:port's are also
  // placed into host_ports as pair<string,int>.
  //
  // The intention is that specified_hosts could be anything, either
  // directly the list of hosts, or some convenient string that
  // subclasses might look up (say, SMC inside of Facebook).
  virtual bool expandConnectHosts(const string& specified_hosts,
                                  string* host_port_string,
                                  HostPortVector* host_ports);

private:
  // Query zookeeper for our root region server and initialize root_conn_.
  bool initRootConnection();

  // Called by Scanner when scan() is invoked.
  RegionScanner beginScanInternal(const string& table,
                                  const vector<string>& columns,
                                  const string& start_row,
                                  const string& end_row);

  // Fetch a chunk of rows into the Scanner's internal buffer.
  bool getNextRows(vector<TRowResult>* output,
                   RegionScanner region_scanner,
                   int num_rows);

  // Given a table and row, find the RegionServerInfo containing the
  // region; this region server is contacted directly for fetching rows.
  shared_ptr<const RegionServerInfo> findRegion(
    const string& table, const string& row,
    bool ignore_cache);

  // Given a region server, return (or create) a NativeHbaseConnection
  // that communicates with it.  The returned connection must
  // eventually be sent to releaseConnection.
  NativeHbaseConnection* connectionForRegionServer(
    const RegionServerInfo* region_server_info);

  // Helper that combined the above two functions.  The returned
  // connection must eventually be sent to releaseConnection.
  NativeHbaseConnection* findConnection(const string& table,
                                        const string& row,
                                        bool ignore_cache) {
    return connectionForRegionServer(
      findRegion(table, row, ignore_cache).get());
  }

  // A callable intended to be invoked per region server with keys
  // relative to that region server that populates a result set.
  // Hides the difference between getRows and getRowsWithColumns,
  // basically.
  typedef std::function<void (HbaseClient*,
                              vector<TRowResult>&,
                              const string&,
                              const vector<string>& keys)> GetRowsHelperOp;

  // A helper function for getRows and getRowsWithColumns; it invokes
  // the given GetRowsHelperOp against each region server with a
  // subset of the keys that correspond to that region server.
  bool getRowsHelper(vector<TRowResult>* output,
                     unordered_map<string, TRowResult*>* output_by_key,
                     const string& table,
                     const vector<string>& keys,
                     const char* opname,
                     GetRowsHelperOp op);



  /**
   * Connect to the specified zookeeper instance without acquiring the lock.
   * This method has to be called with root_conn_mutex_ locked.
   *
   * @returns true if successful
   */
  bool connectUnproteced();

  bool healthy_;
  HostPortVector host_ports_;
  const string hosts_;         // the hosts of the governing zookeeper
  system_clock::time_point last_zookeeper_connect_time_;
  zhandle_t* zh_;              // zookeeper handle
  HbaseClientTransport transport_;  // transport for this client

  std::mutex root_conn_mutex_;     // see top-level comment about mutex order
  // client for the initial regions server; this client does not get
  // copied into our connection pool so as to not congest metadata
  // operations with queries.
  unique_ptr<NativeHbaseConnection> root_conn_;

  std::atomic<uint32_t> num_regions_;    // internal counter of regions loaded
  std::mutex regions_mutex_;           // see top comment about mutex order
  TableMap regions_;                   // map of table_name to TableRegions

  int timeout_ms_;                     // timeout for RPCs, in ms
  int thrift_port_;                    // Port used for all thrift communication
  std::mt19937 rng_;                   // RNG, use must be protected by
                                       // root_conn_mutex_

  // Connection pool for our connections
  HbaseConnectionPool connection_pool_;

  mutable shared_ptr<CounterBase> stats_counters_;

  friend class Scanner;
  friend class FacebookHbaseClient;
  template<typename T> friend class ConnectionGroup;
};

} }  // namespaces

#endif  // HBASE_SRC_NATIVEHBASECLIENT_H_
