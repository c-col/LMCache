// SPDX-License-Identifier: Apache-2.0

#include "connector.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

namespace lmcache {
namespace connector {

namespace {

// parse a RESP reply line of the form <prefix><integer>\r\n (e.g. ":3\r\n",
// "*128\r\n", "$-1\r\n"). Surfaces server error replies ("-ERR ...\r\n") as
// exceptions with the server message. Throws on any other malformed line.
int64_t parse_reply_int(const std::string& line, char expected_prefix,
                        const char* op_name) {
  // recv_line guarantees the line ends with \r\n, so size() >= 2
  if (line.size() < 3) {
    throw std::runtime_error(std::string(op_name) + ": reply line too short");
  }
  if (line[0] == '-') {
    // server error reply; strip prefix and trailing \r\n for the message
    throw std::runtime_error(std::string(op_name) + ": server error: " +
                             line.substr(1, line.size() - 3));
  }
  if (line[0] != expected_prefix) {
    throw std::runtime_error(std::string(op_name) +
                             ": unexpected reply type: " + line);
  }
  errno = 0;
  char* end = nullptr;
  long long value = std::strtoll(line.c_str() + 1, &end, 10);
  if (errno != 0 || end != line.c_str() + line.size() - WorkerConn::crlf_len) {
    throw std::runtime_error(std::string(op_name) +
                             ": malformed integer reply: " + line);
  }
  return static_cast<int64_t>(value);
}

}  // namespace

void WorkerConn::connect(const std::string& h, int p) {
  host = h;
  port = p;

  // 1. create socket
  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("failed to create socket");
  }

  // 2. resolve host
  struct addrinfo hints = {}, *result = nullptr;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  std::string port_str = std::to_string(p);
  int err = getaddrinfo(h.c_str(), port_str.c_str(), &hints, &result);

  if (err != 0) {
    ::close(fd);
    throw std::runtime_error(std::string("getaddrinfo failed: ") +
                             gai_strerror(err));
  }

  // 3. connect to host
  if (::connect(fd, result->ai_addr, result->ai_addrlen) < 0) {
    freeaddrinfo(result);
    ::close(fd);
    throw std::runtime_error("connection failed");
  }

  freeaddrinfo(result);
}

// call send multiple times until all the data is sent
void WorkerConn::send_all(const void* data, size_t len) {
  size_t sent_so_far = 0;
  const char* ptr = static_cast<const char*>(data);
  while (sent_so_far < len) {
    ssize_t n = ::send(fd, ptr + sent_so_far, len - sent_so_far, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;  // retry on EINTR
      }
      throw std::runtime_error("socket send failed");
    }
    if (n == 0) {
      throw std::runtime_error("socket send failed: connection closed");
    }
    sent_so_far += n;
  }
}

// scatter gather send
void WorkerConn::send_multipart(
    const std::vector<std::pair<const void*, size_t>>& parts) {
  if (parts.empty()) return;

  std::vector<struct iovec> iov;
  iov.reserve(parts.size());
  for (const auto& part : parts) {
    iov.push_back({const_cast<void*>(part.first), part.second});
  }

  size_t total_to_send = 0;
  for (const auto& part : parts) {
    total_to_send += part.second;
  }

  size_t sent_so_far = 0;
  size_t iov_idx = 0;

  while (sent_so_far < total_to_send) {
    ssize_t n = ::writev(fd, &iov[iov_idx], iov.size() - iov_idx);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("socket writev failed");
    }
    if (n == 0) {
      throw std::runtime_error("socket writev failed: connection closed");
    }

    sent_so_far += n;

    // adjust iovec for partial writes
    size_t remaining = n;
    while (remaining > 0 && iov_idx < iov.size()) {
      if (remaining >= iov[iov_idx].iov_len) {
        remaining -= iov[iov_idx].iov_len;
        iov_idx++;
      } else {
        iov[iov_idx].iov_base =
            static_cast<char*>(iov[iov_idx].iov_base) + remaining;
        iov[iov_idx].iov_len -= remaining;
        remaining = 0;
      }
    }
  }
}

void WorkerConn::recv_exactly(void* buf, size_t len) {
  size_t recv_so_far = 0;
  char* ptr = static_cast<char*>(buf);
  while (recv_so_far < len) {
    ssize_t n = ::recv(fd, ptr + recv_so_far, len - recv_so_far, 0);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("socket recv failed");
    }
    if (n == 0) {
      throw std::runtime_error("socket recv failed: connection closed");
    }
    recv_so_far += n;
  }
}

// read and discard exactly len bytes (used to consume unusable bulk payloads
// so the connection stays in protocol sync)
void WorkerConn::drain_exactly(size_t len) {
  char scratch[8192];
  while (len > 0) {
    size_t chunk = std::min(len, sizeof(scratch));
    recv_exactly(scratch, chunk);
    len -= chunk;
  }
}

std::string WorkerConn::recv_line() {
  std::string line;
  line.reserve(128);
  for (;;) {
    char c;
    recv_exactly(&c, 1);
    line.push_back(c);
    size_t n = line.size();
    if (n >= 2 && line[n - 2] == '\r' && line[n - 1] == '\n') {
      return line;
    }
  }
}

void WorkerConn::authenticate_if_needed() {
  if (authed) return;
  if (password.empty()) return;

  if (!username.empty()) {
    // auth username password
    std::string u =
        "$" + std::to_string(username.size()) + "\r\n" + username + "\r\n";
    std::string p =
        "$" + std::to_string(password.size()) + "\r\n" + password + "\r\n";
    static constexpr std::string_view auth_prefix = "*3\r\n$4\r\nAUTH\r\n";

    send_multipart({
        {auth_prefix.data(), auth_prefix.size()},
        {u.data(), u.size()},
        {p.data(), p.size()},
    });
  } else {
    // auth password
    std::string p =
        "$" + std::to_string(password.size()) + "\r\n" + password + "\r\n";
    static constexpr std::string_view auth_prefix = "*2\r\n$4\r\nAUTH\r\n";

    send_multipart({
        {auth_prefix.data(), auth_prefix.size()},
        {p.data(), p.size()},
    });
  }

  std::string line = recv_line();
  if (line.rfind("+OK\r\n", 0) == 0) {
    authed = true;
    return;
  }
  if (!line.empty() && line[0] == '-') {
    throw std::runtime_error("AUTH failed: " + line);
  }
  throw std::runtime_error("AUTH failed: unexpected reply: " + line);
}

const std::string& WorkerConn::make_key_header(const std::string& key) {
  key_header_buf.clear();

  const size_t needed = key.size() + 16;
  if (needed > key_header_buf.capacity()) {
    key_header_buf.reserve(needed);
  }

  key_header_buf += '$';
  key_header_buf += std::to_string(key.size());
  key_header_buf += crlf;
  key_header_buf += key;
  key_header_buf += crlf;

  return key_header_buf;
}

const std::string& WorkerConn::make_size_header(size_t batch_chunk_num_bytes) {
  size_header_buf.clear();

  size_header_buf += '$';
  size_header_buf += std::to_string(batch_chunk_num_bytes);
  size_header_buf += crlf;

  return size_header_buf;
}

const std::string& WorkerConn::build_multikey_command(
    std::string_view cmd_part, const std::vector<std::string>& keys) {
  cmd_buf.clear();

  // *<argc>\r\n + cmd_part + per key: $<len>\r\n<key>\r\n (~16 bytes overhead)
  size_t needed = 16 + cmd_part.size();
  for (const auto& key : keys) {
    needed += key.size() + 16;
  }
  if (needed > cmd_buf.capacity()) {
    cmd_buf.reserve(needed);
  }

  cmd_buf += '*';
  cmd_buf += std::to_string(keys.size() + 1);
  cmd_buf += crlf;
  cmd_buf += cmd_part;
  for (const auto& key : keys) {
    cmd_buf += '$';
    cmd_buf += std::to_string(key.size());
    cmd_buf += crlf;
    cmd_buf += key;
    cmd_buf += crlf;
  }

  return cmd_buf;
}

/*
the single-key RESP set, get, exists below are fragile since we make hard
assumptions about the RESP responses. a single error (e.g. a GET miss) could
break our assumptions and desync the connection. the batched paths
(do_batch_get / do_batch_exists further down) instead parse reply headers, so
they tolerate misses and unexpected value sizes per key.
*/

RedisConnector::RedisConnector(std::string host, int port, int num_workers,
                               std::string username, std::string password)
    : ConnectorBase(num_workers),
      host_(std::move(host)),
      port_(port),
      username_(std::move(username)),
      password_(std::move(password)) {
  worker_fds_.reserve(num_workers);
  start_workers();  // start after derived class is fully constructed
}

RedisConnector::~RedisConnector() { close(); }

WorkerConn RedisConnector::create_connection() {
  WorkerConn conn;
  conn.connect(host_, port_);

  conn.username = username_;
  conn.password = password_;
  conn.authenticate_if_needed();

  // track socket fd for shutdown
  {
    std::lock_guard<std::mutex> lk(worker_fds_mu_);
    worker_fds_.push_back(conn.fd);
  }

  return conn;
}

void RedisConnector::do_single_get(WorkerConn& conn, const std::string& key,
                                   void* buf, size_t len, size_t chunk_size) {
  if (len != chunk_size) {
    throw std::runtime_error("buffer size mismatch");
  }

  const std::string& size_header = conn.make_size_header(chunk_size);
  const std::string& key_header = conn.make_key_header(key);

  conn.send_multipart({{conn.get_prefix.data(), conn.get_prefix.size()},
                       {key_header.data(), key_header.size()}});

  // parse response in 3 steps

  // 1. recv size header
  std::vector<char> recv_size_header_buf(size_header.size());
  conn.recv_exactly(recv_size_header_buf.data(), size_header.size());
  if (std::memcmp(recv_size_header_buf.data(), size_header.data(),
                  size_header.size()) != 0) {
    throw std::runtime_error("GET: size header mismatch");
  }

  // 2. recv KV Cache (payload) without parsing
  conn.recv_exactly(buf, len);

  // 3. recv trailer
  char trailer[WorkerConn::crlf_len];
  conn.recv_exactly(trailer, WorkerConn::crlf_len);
  if (std::memcmp(trailer, WorkerConn::crlf.data(), WorkerConn::crlf_len) !=
      0) {
    throw std::runtime_error("GET: trailer mismatch");
  }
}

// RESP SET
void RedisConnector::do_single_set(WorkerConn& conn, const std::string& key,
                                   const void* buf, size_t len,
                                   size_t chunk_size) {
  // we only write exactly batch_chunk_num_bytes bytes (save_unfull_chunk must
  // be off)
  if (len != chunk_size) {
    throw std::runtime_error("buffer size mismatch");
  }

  // build headers using reusable buffers
  const std::string& size_header = conn.make_size_header(chunk_size);
  const std::string& key_header = conn.make_key_header(key);

  // send SET cmd
  // iovec let's us combine pre-built parts and dynamic strings
  conn.send_multipart({{conn.set_prefix.data(), conn.set_prefix.size()},
                       {key_header.data(), key_header.size()},
                       {size_header.data(), size_header.size()},
                       {buf, len},
                       {WorkerConn::crlf.data(), WorkerConn::crlf_len}});

  // parse response which should be exactly +OK\r\n
  char response[WorkerConn::ok_response_len];
  conn.recv_exactly(response, WorkerConn::ok_response_len);

  if (std::memcmp(response, WorkerConn::ok_response.data(),
                  WorkerConn::ok_response_len) != 0) {
    throw std::runtime_error("SET: response was not OK");
  }
}

// RESP EXISTS
bool RedisConnector::do_single_exists(WorkerConn& conn,
                                      const std::string& key) {
  // build key header using reusable buffer
  const std::string& key_header = conn.make_key_header(key);

  // send EXISTS cmd
  // iovec let's us combine pre-built parts and dynamic strings
  conn.send_multipart({{conn.exists_prefix.data(), conn.exists_prefix.size()},
                       {key_header.data(), key_header.size()}});

  // parse response (either :0\r\n or :1\r\n)
  char response[WorkerConn::exists_response_len];
  conn.recv_exactly(response, WorkerConn::exists_response_len);

  if (std::memcmp(response, WorkerConn::exists_one.data(),
                  WorkerConn::exists_response_len) == 0) {
    return true;
  } else if (std::memcmp(response, WorkerConn::exists_zero.data(),
                         WorkerConn::exists_response_len) == 0) {
    return false;
  } else {
    throw std::runtime_error(
        "EXISTS returned invalid response that wasn't :0\r\n or :1\r\n");
  }
}

// RESP DEL
bool RedisConnector::do_single_delete(WorkerConn& conn,
                                      const std::string& key) {
  // build key header using reusable buffer
  const std::string& key_header = conn.make_key_header(key);

  // send DEL cmd
  conn.send_multipart({{conn.del_prefix.data(), conn.del_prefix.size()},
                       {key_header.data(), key_header.size()}});

  // parse response (either :0\r\n or :1\r\n, same format as EXISTS)
  char response[WorkerConn::exists_response_len];
  conn.recv_exactly(response, WorkerConn::exists_response_len);

  if (std::memcmp(response, WorkerConn::exists_one.data(),
                  WorkerConn::exists_response_len) == 0) {
    return true;  // key was deleted
  } else if (std::memcmp(response, WorkerConn::exists_zero.data(),
                         WorkerConn::exists_response_len) == 0) {
    return false;  // key did not exist
  } else {
    throw std::runtime_error(
        "DEL returned invalid response that wasn't :0\r\n or :1\r\n");
  }
}

/*
batched GET via MGET.

wire format (request):  *<N+1>\r\n$4\r\nMGET\r\n$<len>\r\n<key>\r\n ...
wire format (reply):    *<N>\r\n then per key either $-1\r\n (miss) or
                        $<len>\r\n<payload>\r\n (hit)

unlike do_single_get, misses and size mismatches are handled per key: the
unusable payload is drained so the connection stays in protocol sync, the
key's result is marked 0, and the rest of the batch proceeds. only protocol
or socket level failures throw (failing the whole tile).
*/
void RedisConnector::do_batch_get(WorkerConn& conn, const Request& req) {
  const size_t num_keys = req.keys.size();

  const std::string& cmd =
      conn.build_multikey_command(WorkerConn::mget_cmd_part, req.keys);
  conn.send_all(cmd.data(), cmd.size());

  // 1. array header: *<N>\r\n
  int64_t num_replies = parse_reply_int(conn.recv_line(), '*', "MGET");
  if (num_replies != static_cast<int64_t>(num_keys)) {
    throw std::runtime_error("MGET: reply count mismatch: expected " +
                             std::to_string(num_keys) + ", got " +
                             std::to_string(num_replies));
  }

  // 2. per-key bulk strings
  for (size_t i = 0; i < num_keys; ++i) {
    int64_t value_len = parse_reply_int(conn.recv_line(), '$', "MGET");

    if (value_len < 0) {
      // $-1\r\n: key does not exist (no payload or trailer follows)
      req.batch->per_key_results[req.start_idx + i] = 0;
      fprintf(stderr, "[LMCache GET] key %s failed: not found\n",
              req.keys[i].c_str());
      continue;
    }

    if (static_cast<size_t>(value_len) == req.buf_lens[i] &&
        req.buf_lens[i] == req.batch_chunk_num_bytes) {
      conn.recv_exactly(req.buf_ptrs[i], static_cast<size_t>(value_len));
      req.batch->per_key_results[req.start_idx + i] = 1;
    } else {
      // stored value does not fit the destination buffer; consume it so
      // later replies still parse, and fail just this key
      conn.drain_exactly(static_cast<size_t>(value_len));
      req.batch->per_key_results[req.start_idx + i] = 0;
      fprintf(stderr,
              "[LMCache GET] key %s failed: size mismatch (value %lld bytes, "
              "buffer %zu bytes, chunk %zu bytes)\n",
              req.keys[i].c_str(), static_cast<long long>(value_len),
              req.buf_lens[i], req.batch_chunk_num_bytes);
    }

    // 3. payload trailer
    char trailer[WorkerConn::crlf_len];
    conn.recv_exactly(trailer, WorkerConn::crlf_len);
    if (std::memcmp(trailer, WorkerConn::crlf.data(), WorkerConn::crlf_len) !=
        0) {
      throw std::runtime_error("MGET: trailer mismatch");
    }
  }
}

/*
batched EXISTS via the multi-key EXISTS command.

wire format (request):  *<N+1>\r\n$6\r\nEXISTS\r\n$<len>\r\n<key>\r\n ...
wire format (reply):    :<count>\r\n

EXISTS counts each argument position independently (a key passed twice that
exists counts twice), so count == N means every key exists and count == 0
means none do — both exact even with duplicate keys. those two cases resolve
the whole tile in one round trip, which covers the common lookup patterns
(fully cached or fully uncached prefix). a partial count carries no per-key
information, so we fall back to one pipelined round of single-key EXISTS.
*/
void RedisConnector::do_batch_exists(WorkerConn& conn, const Request& req) {
  const size_t num_keys = req.keys.size();

  const std::string& cmd =
      conn.build_multikey_command(WorkerConn::exists_cmd_part, req.keys);
  conn.send_all(cmd.data(), cmd.size());

  int64_t count = parse_reply_int(conn.recv_line(), ':', "EXISTS");
  if (count < 0 || count > static_cast<int64_t>(num_keys)) {
    throw std::runtime_error("EXISTS: count out of range: " +
                             std::to_string(count));
  }

  if (count == static_cast<int64_t>(num_keys)) {
    for (size_t i = 0; i < num_keys; ++i) {
      req.batch->per_key_results[req.start_idx + i] = 1;
    }
  } else if (count == 0) {
    for (size_t i = 0; i < num_keys; ++i) {
      req.batch->per_key_results[req.start_idx + i] = 0;
    }
  } else {
    resolve_partial_exists(conn, req);
  }
}

void RedisConnector::resolve_partial_exists(WorkerConn& conn,
                                            const Request& req) {
  const size_t num_keys = req.keys.size();

  // one pipelined write of N single-key EXISTS commands
  std::string pipeline;
  size_t needed = 0;
  for (const auto& key : req.keys) {
    needed += conn.exists_prefix.size() + key.size() + 16;
  }
  pipeline.reserve(needed);
  for (const auto& key : req.keys) {
    pipeline += conn.exists_prefix;
    pipeline += conn.make_key_header(key);
  }
  conn.send_all(pipeline.data(), pipeline.size());

  // N integer replies, each exactly :0\r\n or :1\r\n
  for (size_t i = 0; i < num_keys; ++i) {
    char response[WorkerConn::exists_response_len];
    conn.recv_exactly(response, WorkerConn::exists_response_len);

    if (std::memcmp(response, WorkerConn::exists_one.data(),
                    WorkerConn::exists_response_len) == 0) {
      req.batch->per_key_results[req.start_idx + i] = 1;
    } else if (std::memcmp(response, WorkerConn::exists_zero.data(),
                           WorkerConn::exists_response_len) == 0) {
      req.batch->per_key_results[req.start_idx + i] = 0;
    } else {
      throw std::runtime_error(
          "EXISTS pipeline returned invalid response that wasn't :0\r\n or "
          ":1\r\n");
    }
  }
}

void RedisConnector::shutdown_connections() {
  std::lock_guard<std::mutex> lk(worker_fds_mu_);
  for (int fd : worker_fds_) {
    if (fd >= 0) {
      ::shutdown(fd, SHUT_RDWR);
    }
  }
}

}  // namespace connector
}  // namespace lmcache
