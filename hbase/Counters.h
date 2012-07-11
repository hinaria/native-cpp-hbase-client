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

// A basic definition of what a counter class is, and a trivial,
// non-performant implementation using a simple hash map to store
// counters.  More sophisticated implementations might export stats to
// a thrift server or other external source.

#ifndef HBASE_SRC_COUNTERS_H
#define HBASE_SRC_COUNTERS_H

#include <limits>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

#include "boost/noncopyable.hpp"

#include "folly/String.h"
#include "folly/Histogram.h"

namespace facebook { namespace hbase {

using std::map;
using std::string;
using std::unordered_map;

typedef folly::Histogram<int64_t> CounterHistogram;

class CounterBase : private boost::noncopyable {
 public:
  const string fieldName(const string& field) {
    if (prefix_.empty()) {
      return field;
    } else {
      return prefix_ + "_" + field;
    }
  }

  // Counters may or may not have prefixes; if specified, the prefix
  // is prepended to every value when snapshotted and should *not* be
  // part of the "counter" to increment via incrementCounter.
  explicit CounterBase(const string& prefix) : prefix_(prefix) { }
  CounterBase() { }

  virtual ~CounterBase() { }
  virtual void incrementCounter(const string& counter, int64_t incr = 1) { }
  virtual void addHistogramValue(const string& counter, int64_t value) { }

  virtual void snapshot(map<string, int64_t>* output,
                        map<string, string>* output_buckets) { }

private:
  const string prefix_;
};

// A SimpleCounter is a very simple, inefficient counter for tracking
// stats.
class SimpleCounter : public CounterBase {
public:
  explicit SimpleCounter(const string& prefix) : CounterBase(prefix) { init(); }
  SimpleCounter() : CounterBase() { init(); }

  void init() {
    addHistogram("rowop_rpc_time_micros", 20*1000, 0, 1000*1000);
    addHistogram("root_rpc_time_micros", 20*1000, 0, 1000*1000);
  }

  void addHistogram(const string& key, int bucket_width, int lower, int upper) {
    histogram_stats_[key].reset(new CounterHistogram(bucket_width,
                                                     lower, upper));
  }

  virtual void addHistogramValue(const string& counter, int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = histogram_stats_.find(counter);
    if (it != histogram_stats_.end()) {
      it->second->addValue(value);
    }
  }

  virtual ~SimpleCounter() { }
  virtual void incrementCounter(const string& counter, int64_t incr = 1) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_[counter] += incr;
  }

  virtual void snapshot(map<string, int64_t>* output,
                        map<string, string>* output_buckets) {
    std::lock_guard<std::mutex> lock(mutex_);

    output->clear();
    output_buckets->clear();
    for (auto& kv : stats_) {
      output->insert(make_pair(fieldName(kv.first), kv.second));
    }
    for (auto& kv : histogram_stats_) {
      const string field_name = fieldName(kv.first);
      CounterHistogram *h = kv.second.get();

      // Write histogram percentile information to output, not
      // output_buckets, since it is simply numeric data.
      output->insert(make_pair(field_name + ".p50",
                               h->getPercentileEstimate(0.50)));
      output->insert(make_pair(field_name + ".p95",
                               h->getPercentileEstimate(0.95)));
      output->insert(make_pair(field_name + ".p99",
                               h->getPercentileEstimate(0.99)));

      string bucket_string;

      for (int i = 0; i < h->getNumBuckets(); ++i) {
        if (i > 0) bucket_string += ",";
        const auto& bucket = h->getBucketByIndex(i);
        double average = 0.0;
        if (bucket.count > 0)
          average = bucket.sum / bucket.count;

        int64_t bucket_min = std::numeric_limits<int64_t>::min();
        if (i > 0) {
          bucket_min = h->getMin() + (i - 1) * h->getBucketSize();
        }
        folly::stringAppendf(&bucket_string, "%ld:%ld:%ld",
                             bucket_min,
                             bucket.count,
                             (int64_t)average);
      }
      output_buckets->insert(make_pair(field_name, bucket_string));
    }
  }

private:
  mutable std::mutex mutex_;
  unordered_map<string, int64_t> stats_;
  unordered_map<string, std::unique_ptr<CounterHistogram> > histogram_stats_;
};

} } // facebook::hbase

#endif // HBASE_SRC_COUNTERS_H
