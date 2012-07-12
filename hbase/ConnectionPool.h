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

// A fairly simple, highly templetized connection pool, intended for
// use with the HBase Client.
//
// A connection pool is similar to a hash, and is templetized on two
// types -- the key to the hash, and the entries.  However, in
// addition to this, the pool knows how to create new entries for the
// pool (via callbacks).  This allows the pool to expand as needed.
// See the test case for basic usage.

#ifndef HBASE_SRC_CONNECTIONPOOL_H
#define HBASE_SRC_CONNECTIONPOOL_H

#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "folly/Logging.h"

#include "hbase/Counters.h"

// This needs to be declared before the template below for clang (ADL
// rules, see http://clang.llvm.org/compatibility.html, somewhat
// complicated by 'const std::pair<std::string, int>' being the
// typedef target of HostMapKey).
std::ostream& operator<<(std::ostream& out,
                         const std::pair<std::string, int>& key);

namespace facebook { namespace hbase {
using std::unordered_multimap;

template<class Key, class Conn>
class ConnectionPool : private boost::noncopyable {
public:
  // Types for our callbacks and map.
  typedef unordered_multimap<const Key, Conn*> connection_multimap;
  typedef std::function<Conn*(const Key)> ConnectionFactory;
  typedef std::function<const Key(const Conn*)> ConnectionKeyMaker;

  // The conneciton pool is templetized on the Key and Conn types.  In
  // addition, the pool contains a maximum number of retained
  // connections per key (if more than this limit are requested, they
  // will be created on the fly, and deleted when they are finished
  // being used).
  ConnectionPool(int max_retained_connections_per_key,
                 int max_total_connections_per_key,
                 ConnectionFactory factory,
                 ConnectionKeyMaker key_maker,
                 CounterBase* counters = NULL)
      : max_retained_connections_per_key_(max_retained_connections_per_key),
        max_total_connections_per_key_(max_total_connections_per_key),
        connection_factory_(factory),
        connection_key_maker_(key_maker),
        stats_counters_(counters) {
    if (max_total_connections_per_key_ > 0) {
      CHECK_GE(max_total_connections_per_key_,
               max_retained_connections_per_key_);
    }
  }

  // We should never be destroyed if there are any in-use connections.
  ~ConnectionPool() {
    std::lock_guard<std::mutex> g(connections_mutex_);
    VLOG(1) << "Clearing out " << available_connections_.size()
            << " entries in the connection pool.";
    CHECK_EQ(in_use_connections_.size(), 0)
      << "Cannot destroy a connection pool while there are in-use "
      << "connections!";
  };

  // Given a key, find a connection in our unused connections, or
  // create a new connection.  Note there is a race here; if multiple
  // threads decide to invoke connection_factory_ for this key, then
  // multiple entries will be simultaneously created.  That's okay,
  // the worst result is we missed a chance to re-use an entry
  // returned to the pool.
  Conn* lookup(const Key& key) {
    std::unique_ptr<Conn> ret;
    {
      std::lock_guard<std::mutex> g(connections_mutex_);
      auto p = available_connections_.find(key);
      if (p != available_connections_.end()) {
        if (stats_counters_) {
          stats_counters_->incrementCounter("reused_connections");
        }
        ret = std::move(p->second);
        available_connections_.erase(p);
        VLOG(1) << "Using already created connection for " << key;
      }
      if (!ret.get()) {
        int total_key_connections = available_connections_.count(key) +
          in_use_connections_.count(key);
        VLOG(1) << "Total outstanding connections for " << key << ": "
                << total_key_connections;
        if (max_total_connections_per_key_ > 0 &&
            total_key_connections >= max_total_connections_per_key_) {
          FB_LOG_EVERY_MS(ERROR, 500) << "Hit max total allowed connections "
                                      << "for " << key;
          return NULL;
        }
      }
    }
    if (!ret.get()) {
      ret.reset(connection_factory_(key));
      if (stats_counters_) {
        stats_counters_->incrementCounter("created_connections");
      }
      if (!ret.get()) {
        LOG(ERROR) << "ConnectionPool factory failed to create connection"
                   << " for key:" << key;
        return NULL;
      }
      VLOG(1) << "Creating connection for " << key;
    }
    std::lock_guard<std::mutex> g(connections_mutex_);
    Conn* ret_ptr = ret.get();
    in_use_connections_.insert(make_pair(key, std::move(ret)));
    return ret_ptr;
  }

  // Release a key.  If this would put us under the limit we have per
  // key, retain the connection, otherwise go ahead and free it.
  void release(Conn* conn) {
    CHECK(conn != NULL);
    if (stats_counters_) {
      stats_counters_->incrementCounter("released_connections");
    }

    std::unique_ptr<Conn> conn_holder;
    const Key key(connection_key_maker_(conn));
    std::lock_guard<std::mutex> g(connections_mutex_);
    auto p = in_use_connections_.equal_range(key);
    for (auto it = p.first; it != p.second; ++it) {
      if ((*it).second.get() == conn) {
        conn_holder = std::move((*it).second);
        in_use_connections_.erase(it);
        break;
      }
    }

    if (available_connections_.count(key) + in_use_connections_.count(key) <
        max_retained_connections_per_key_) {
      if (conn->isHealthy()) {
        VLOG(1) << "Returning healthy connection " << conn << " for " << key
                << " to pool";
        // Let's double check the exact same connection we're
        // releasing isn't already in available_connections_.  Just in
        // case.
        auto p = available_connections_.equal_range(key);
        for (auto it = p.first; it != p.second; ++it) {
          if ((*it).second.get() == conn) {
            LOG(ERROR) << "Attempt to double release a connection?!  "
                       << "Shocking you didn't already crash.";
            return;
          }
        }

        available_connections_.insert(make_pair(key, std::move(conn_holder)));
      } else {
        if (stats_counters_) {
          stats_counters_->incrementCounter("discarded_connections");
        }
        LOG(ERROR) << "Discarding unhealthy connection " << conn << " for "
                   << key;
      }
    } else {
      VLOG(1) << "Too many connections for " << key << ", deleting "
              << conn;
    }
  }

 private:
  const int max_retained_connections_per_key_;
  const int max_total_connections_per_key_;
  const ConnectionFactory connection_factory_;
  const ConnectionKeyMaker connection_key_maker_;

  std::mutex connections_mutex_;
  unordered_multimap<Key, std::unique_ptr<Conn>> available_connections_;
  unordered_multimap<Key, std::unique_ptr<Conn>> in_use_connections_;

  CounterBase* stats_counters_;  // unowned
};

} } // facebook::hbase

#endif // HBASE_SRC_CONNECTIONPOOL_H
