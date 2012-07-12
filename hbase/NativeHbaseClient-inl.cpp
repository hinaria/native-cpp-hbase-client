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

// In-line template declarations.  Included in a couple of headers as
// a hack to get around a gcc 4.6.2 segfault.

#ifndef HBASE_SRC_NATIVEHBASECLIENT_INL_H_
#define HBASE_SRC_NATIVEHBASECLIENT_INL_H_

namespace facebook { namespace hbase {

// A generic wrapper for thrift operations; if a Thrift protocol error
// occurs, the connection is marked unhealthy.
template<class T>
T invokeThriftOperation(NativeHbaseClient* client,
                        NativeHbaseConnection* conn,
                        typename ThriftOperation<T>::Type op) {
  client->getCounters()->incrementCounter("regionserver_operations");
  if (!conn->isHealthy()) {
    throw NativeHbaseException("Invoked operation on unhealthy connection");
  }
  try {
    return op(conn->thrift_client());
  }
  catch (const TTransportException& te) {
    conn->markUnhealthy();
    LOG(ERROR) << "Transport Exception received, retrying."
               << " Error detail:" << te.what()
               << " Server:" << conn->host() << ":" << conn->port();
    throw;
  }
}

// A wrapper for operations on the connection to the root server.  We
// attempt one retry; if it fails, we mark ourselves unhealthy and
// propagate the exception.
template<class T>
T NativeHbaseClient::invokeRootOperation(NativeHbaseClient* client,
                                         const char* opname,
                                         typename ThriftOperation<T>::Type op) {
  stats_counters_->incrementCounter("root_operations");
  std::lock_guard<std::mutex> g(root_conn_mutex_);
  if (!client->isHealthy() || !client->root_conn_->isHealthy()) {
    stats_counters_->incrementCounter("unhealthy_root_operations");
    throw NativeHbaseException(
      "Invoked operation on unhealthy NativeHbaseClient");
  }
  for (int attempt = 0; attempt < 2; ++attempt) {
    try {
      auto now = system_clock::now();
      ScopeGuard g = folly::makeGuard(
        [=] {
          stats_counters_->addHistogramValue(
            "root_rpc_time_micros",
            microseconds(system_clock::now() - now).count());
        }
      );
      logHbaseOperation(opname,
                        client->root_conn_->host(), client->root_conn_->port(),
                        "", timeout_ms_);
      return op(client->root_conn_->thrift_client());
    }
    catch (const TTransportException& te) {
      stats_counters_->incrementCounter("root_operation_failures");
      const string error_message =
        string("Transport Exception received when communicating to root region "
               "server: ") + te.what();
      logHbaseError(opname,
                    client->root_conn_->host(), client->root_conn_->port(),
                    "", timeout_ms_, error_message);
      LOG(ERROR) << error_message;
      if (attempt == 0) {
        LOG(ERROR) << "Retrying once";
        if (!initRootConnection()) {
          LOG(ERROR) << "Re-initializing root connection failed, aborting";
          logHbaseError(
            opname, client->root_conn_->host(), client->root_conn_->port(),
            "", timeout_ms_, "Error retrying root operation, aborting");
          throw;
        }
        continue;
      }
      client->root_conn_->markUnhealthy();
      healthy_ = false;
      throw;
    }
    catch(...) {
      throw;
    }
  }
  CHECK(false) << "should never reach this";
}

// A generic wrapper for interacting with the Thrift client objects.
// We retry some operations, possibly re-reading META to locate a
// region server (in the case of an IOError that looks like "Region is
// not online:").  For TTransportExceptions, we simply re-execute the
// operation once after marking it unhealthy (which ensures that, when
// the connection is released, it is not reused and instead we
// recreate a connection).  All other errors are reported and raised
// without retrying.
template <class T>
T NativeHbaseClient::invokeRowOperation(const string& table, const string& row,
                                        const char* opname,
                                        typename ThriftOperation<T>::Type op) {
  bool ignore_cache = false;
  for (int attempt = 0; attempt < 2; ++attempt) {
    string host;
    int port = -1;
    if (attempt > 0) {
      stats_counters_->incrementCounter("rowop_region_server_retry");
    }
    try {
      auto now = system_clock::now();
      ScopeGuard g = folly::makeGuard(
        [=] {
          stats_counters_->addHistogramValue(
            "rowop_rpc_time_micros",
            microseconds(system_clock::now() - now).count());
        }
      );
      NativeHbaseConnection* conn = findConnection(table, row, ignore_cache);
      host = conn->host();
      port = conn->port();
      ScopeGuard g2 = folly::makeGuard(
          [=] { this->connection_pool_.release(conn); } );
      logHbaseOperation(opname, host, port, "", timeout_ms_);
      return invokeThriftOperation<T>(this, conn, op);
    }
    catch (const TTransportException& te) {
      stats_counters_->incrementCounter("rowop_region_server_exception");
      // invokeThriftOperation will mark the connection unhealthy so
      // that when we retry, we get a new one; so just continue to the
      // retry on the first attempt.  We need to ignore the cache on
      // the next read in case the region server is down -- we want to
      // re-query the master to find the region.
      const string error_message =
        to<string>("Region server transport exception, table: ", table,
                   ", row: ", humanify(row), ", exception: ", te.what());
      FB_LOG_EVERY_MS(ERROR, 100) << error_message;
      logHbaseError(opname, host, port, row, timeout_ms_, error_message);
      if (attempt == 0) {
        ignore_cache = true;
        continue;
      }
      throw;
    }
    catch (const IOError& e) {
      stats_counters_->incrementCounter("rowop_region_server_exception");
      const string error_message =
        to<string>("Region server io exception, table: ", table,
                   ", row: ", humanify(row), ", exception: ", e.what());
      FB_LOG_EVERY_MS(ERROR, 100) << error_message;
      logHbaseError(opname, host, port, row, timeout_ms_, error_message);
      if (attempt == 0 &&
          boost::starts_with(e.message, "Region is not online:")) {
        LOG(ERROR) << "Retrying operation...";
        ignore_cache = true;
        continue;
      }
      throw;
    }
    catch (const std::exception& e) {
      stats_counters_->incrementCounter("rowop_region_server_exception");
      const string error_message =
        to<string>("Region server unknown exception, table: ", table,
                   ", row: ", humanify(row), ", exception: ", e.what());
      FB_LOG_EVERY_MS(ERROR, 100) << error_message;
      logHbaseError(opname, host, port, row, timeout_ms_, error_message);
      throw;
    }
    catch(...) {
      stats_counters_->incrementCounter("rowop_region_server_exception");
      const string error_message =
        to<string>("Unknown type of exception during RPC, table: ", table,
                   ", row: ", humanify(row));
      FB_LOG_EVERY_MS(ERROR, 100) << error_message;
      logHbaseError(opname, host, port, row, timeout_ms_, error_message);
      throw;
    }
  }
  CHECK(0);  // should never get here
  return T();
}

} }  // namespaces

#endif  // HBASE_SRC_NATIVEHBASECLIENT_INL_H_
