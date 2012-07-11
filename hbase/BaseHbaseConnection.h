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

// This class represents a connection to an HBase server.  It is
// templated to be usable with different actual RPC classes (such as
// the default thrift RPCs or the extended logstream APIs).

#ifndef HBASE_SRC_BASEHBASECONNECTION_H
#define HBASE_SRC_BASEHBASECONNECTION_H

#include "hbase/src/nhc-config.h"

#include <string>
#include <memory>

#include "folly/Logging.h"

#if !NHC_OPEN_SOURCE
#include "thrift/lib/cpp/protocol/THeaderProtocol.h"
#include "thrift/lib/cpp/transport/THeaderTransport.h"
#endif

#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/protocol/TCompactProtocol.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/TSocket.h"

namespace facebook { namespace hbase {

#if !NHC_OPEN_SOURCE
using apache::thrift::protocol::THeaderProtocol;
using apache::thrift::transport::THeaderTransport;
#endif

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TProtocol;
using apache::thrift::transport::TFramedTransport;
using apache::thrift::transport::TBufferedTransport;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::TException;
using std::string;

/// Transport to use when connecting.
enum HbaseClientTransport {
  DEFAULT_TRANSPORT,
  BUFFERED_TRANSPORT,
  FRAMED_TRANSPORT,
  COMPACT_FRAMED_TRANSPORT,
  HEADER_TRANSPORT,
  HEADER_TRANSPORT_WITH_ZLIB
};

// Class representing a connection to a thrift server.  Holds the
// various Thrift classes such as the socket, transport, etc.
template<class T>
class BaseHbaseConnection : private boost::noncopyable {
public:
  BaseHbaseConnection(const string& host, const int port);
  ~BaseHbaseConnection();
  bool connect(HbaseClientTransport transport,
              int timeout_ms);
  T* thrift_client() { return client_.get(); }
  const string& host() const { return host_; }
  const int port() const { return port_; }
  void markUnhealthy() { ++errors_; }
  const bool isHealthy() const { return errors_ == 0; }

private:
  const string host_;
  const int port_;
  std::unique_ptr<T> client_;
  int errors_;
};

template<class T>
BaseHbaseConnection<T>::BaseHbaseConnection(
  const string& host, const int port)
    : host_(host), port_(port), errors_(0) {
}

template<class T>
bool BaseHbaseConnection<T>::connect(
  HbaseClientTransport transport_type,
  int timeout_ms) {
  if (host_.empty()) {
    FB_LOG_EVERY_MS(ERROR, 1000) << "Request to connect to invalid host; "
                              << "empty host specified?!";
    return false;
  }
  if (port_ <= 0) {
    FB_LOG_EVERY_MS(ERROR, 1000) << "Invalid port for host " << host_;
    return false;
  }
  VLOG(1) << "Connecting to " << host_ << ":" << port_ << "...";

  try {
    boost::shared_ptr<TSocket> socket(new TSocket(host_, port_));
    if (timeout_ms > 0) {
      socket->setConnTimeout(timeout_ms);
      socket->setRecvTimeout(timeout_ms);
      socket->setSendTimeout(timeout_ms);
    }

    assert(transport_type != DEFAULT_TRANSPORT);

    boost::shared_ptr<TProtocol> protocol;
    boost::shared_ptr<TTransport> transport;
    if (transport_type == BUFFERED_TRANSPORT) {
      transport.reset(new TBufferedTransport(socket));
      protocol.reset(new TBinaryProtocol(transport));
    } else if (transport_type == FRAMED_TRANSPORT) {
      transport.reset(new TFramedTransport(socket));
      protocol.reset(new TBinaryProtocol(transport));
    } else if (transport_type == COMPACT_FRAMED_TRANSPORT) {
      transport.reset(new TFramedTransport(socket));
      protocol.reset(new TCompactProtocol(transport));
    }
#if !NHC_OPEN_SOURCE
    else if (transport_type == HEADER_TRANSPORT) {
      transport.reset(new THeaderTransport(socket));
      protocol.reset(new THeaderProtocol(transport));
    } else if (transport_type == HEADER_TRANSPORT_WITH_ZLIB) {
      auto transport_tmp = new THeaderTransport(socket);
      transport_tmp->setTransform(THeaderTransport::ZLIB_TRANSFORM);
      transport.reset(transport_tmp);
      protocol.reset(new THeaderProtocol(transport));
    }
#endif

    client_.reset(new T(protocol));
    transport->open();
    VLOG(1) << "done";
  }
  catch (const TException& e) {
    LOG(ERROR) << "Unable to connect to " << host_ << ":" << port_ << ": "
               << e.what();
    return false;
  }
  return true;
}

template<class T>
BaseHbaseConnection<T>::~BaseHbaseConnection() {
}

} }  // namespace

#endif // HBASE_SRC_BASEHBASECONNECTION_H
