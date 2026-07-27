// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "../connector_base.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <vector>

namespace lmcache {
namespace connector {

// a TCP session (one per thread) implementing RESP2
/*
key optimizations include:
1. preset batch_chunk_num_bytes (allows not parsing for \r\n byte-by-byte)
2. scatter/gather sending of data (with pre-allocated buffers)
3. zero copy (no bounce buffers)
*/
struct WorkerConn {
  int fd = -1;
  std::string host;
  int port;

  // authentication
  std::string username;  // optional; empty => auth password
  std::string password;  // required for auth
  bool authed = false;

  // pre-computed headers
  std::string get_prefix;
  std::string set_prefix;
  std::string exists_prefix;
  std::string del_prefix;

  // command-name elements (without the leading *<argc>\r\n array header) for
  // building variadic multi-key commands like EXISTS k1..kN and MGET k1..kN
  static constexpr std::string_view exists_cmd_part = "$6\r\nEXISTS\r\n";
  static constexpr std::string_view mget_cmd_part = "$4\r\nMGET\r\n";

  // reusable buffers for building headers (avoids repeated dynamic allocations)
  std::string key_header_buf;
  std::string size_header_buf;
  std::string cmd_buf;

  // pre-computed constants (for comparisons)
  static constexpr std::string_view crlf = "\r\n";
  static constexpr size_t crlf_len = crlf.size();

  static constexpr std::string_view ok_response = "+OK\r\n";
  static constexpr size_t ok_response_len = ok_response.size();

  static constexpr std::string_view exists_one = ":1\r\n";
  static constexpr std::string_view exists_zero = ":0\r\n";
  static constexpr size_t exists_response_len = exists_one.size();

  WorkerConn()
      : get_prefix("*2\r\n$3\r\nGET\r\n"),
        set_prefix("*3\r\n$3\r\nSET\r\n"),
        exists_prefix("*2\r\n$6\r\nEXISTS\r\n"),
        del_prefix("*2\r\n$3\r\nDEL\r\n") {
    // pre-allocate key_header_buf to handle typical keys without reallocation
    // typical key format: model_name@world_size@worker_id@chunk_hash_hex@dtype
    // - model_name: 25-50 chars (e.g., "meta-llama/Llama-3-70b-instruct")
    // - world_size: 1-2 chars
    // - worker_id: 1-2 chars
    // - chunk_hash (SHA256): 64 chars hex
    // - dtype: 7-8 chars (e.g., "bfloat16")
    // - separators: 4 chars
    // total typical key: ~100-140 chars
    // resp header overhead: $<len>\r\n<key>\r\n = ~8 bytes
    // reserve 512 bytes to handle typical keys plus margin
    key_header_buf.reserve(512);

    // pre-allocate size_header_buf for chunk size headers
    // typical format: $<batch_chunk_num_bytes>\r\n
    // typical batch_chunk_num_bytes: 1MB-4MB = 7-8 digit number
    // reserve 32 bytes to handle up to 20+ digit numbers with margin
    size_header_buf.reserve(32);
  }

  ~WorkerConn() {
    if (fd >= 0) ::close(fd);
  }

  void connect(const std::string& host, int port);
  void authenticate_if_needed();
  void send_all(const void* data, size_t len);
  void send_multipart(const std::vector<std::pair<const void*, size_t>>& parts);
  void recv_exactly(void* buf, size_t len);
  void drain_exactly(size_t len);
  std::string recv_line();
  const std::string& make_key_header(const std::string& key);
  const std::string& make_size_header(size_t batch_chunk_num_bytes);

  // build a single-buffer variadic command:
  //   *<N+1>\r\n<cmd_part>$<len(k1)>\r\nk1\r\n ... $<len(kN)>\r\nkN\r\n
  // where cmd_part is one of the *_cmd_part constants above. The returned
  // reference points into cmd_buf and is invalidated by the next call.
  const std::string& build_multikey_command(
      std::string_view cmd_part, const std::vector<std::string>& keys);
};

// how do_batch_get executes a tile of keys
enum class GetBatchMode : uint8_t {
  // N single-key GET commands written in one batch, then N replies read
  // back. Same round-trip savings as MGET; each command executes and is
  // routed independently, so cross-slot batches work on Redis Cluster and
  // the server can interleave other clients between keys.
  PIPELINE,
  // one multi-key MGET command per tile. Slightly fewer reply bytes, but
  // executes atomically (holds the event loop for the whole tile) and
  // cross-slot batches fail on Redis Cluster.
  MGET,
};

// how do_batch_exists executes a tile of keys
enum class ExistsBatchMode : uint8_t {
  // N single-key EXISTS commands written in one batch, then N :0/:1
  // replies. Per-key results in one round trip, cluster-safe.
  PIPELINE,
  // one multi-key EXISTS per tile: its count reply resolves fully-cached /
  // fully-uncached tiles in one tiny round trip, with a pipelined per-key
  // fallback (one extra round trip) for partial counts. Cross-slot batches
  // fail on Redis Cluster.
  MULTIKEY,
};

class RedisConnector : public ConnectorBase<WorkerConn> {
 public:
  // get_min_keys_per_tile: minimum keys a batched-GET tile should carry
  // before the batch is split across more worker connections (see
  // choose_num_tiles). Must be >= 1. Applies to both get_batch_mode
  // settings. Higher values favor fewer, larger batched commands; lower
  // values favor connection-level parallelism for payload transfer.
  //
  // get_batch_mode: "pipeline" (default) or "mget"; see GetBatchMode.
  // exists_batch_mode: "pipeline" (default) or "multikey"; see
  // ExistsBatchMode.
  RedisConnector(std::string host, int port, int num_workers,
                 std::string username = "", std::string password = "",
                 size_t get_min_keys_per_tile = 8,
                 std::string get_batch_mode = "pipeline",
                 std::string exists_batch_mode = "pipeline");
  ~RedisConnector() override;

 protected:
  WorkerConn create_connection() override;
  void do_single_get(WorkerConn& conn, const std::string& key, void* buf,
                     size_t len, size_t chunk_size) override;
  void do_single_set(WorkerConn& conn, const std::string& key, const void* buf,
                     size_t len, size_t chunk_size) override;
  bool do_single_exists(WorkerConn& conn, const std::string& key) override;
  bool do_single_delete(WorkerConn& conn, const std::string& key) override;

  // batch overrides executing a whole tile in one round trip instead of one
  // per key: GET via pipeline or MGET (get_batch_mode), SET via pipeline,
  // EXISTS via pipeline or multi-key EXISTS (exists_batch_mode). See
  // connector.cpp for the RESP wire details.
  void do_batch_get(WorkerConn& conn, const Request& req) override;
  void do_batch_set(WorkerConn& conn, const Request& req) override;
  void do_batch_exists(WorkerConn& conn, const Request& req) override;

  // tiling policy tuned for batched commands (see connector.cpp):
  // EXISTS -> 1 tile, GET -> get_min_keys_per_tile floor, SET/DELETE ->
  // default fan-out.
  size_t choose_num_tiles(Op op, size_t num_items) const override;

  void shutdown_connections() override;

 private:
  // the two do_batch_exists strategies (dispatched on exists_batch_mode_).
  // the pipelined variant also serves as the multikey variant's per-key
  // fallback for partially-cached tiles.
  void do_batch_exists_pipelined(WorkerConn& conn, const Request& req);
  void do_batch_exists_multikey(WorkerConn& conn, const Request& req);

  // the two do_batch_get strategies (dispatched on get_batch_mode_)
  void do_batch_get_pipelined(WorkerConn& conn, const Request& req);
  void do_batch_get_mget(WorkerConn& conn, const Request& req);

  // consume one bulk-string value reply for key i of req (the $<len> header
  // line has already been read and parsed into value_len). Receives the
  // payload into the destination buffer on an exact fit, otherwise drains
  // it, and records the per-key result. Throws only on protocol/socket
  // failures.
  void consume_bulk_value(WorkerConn& conn, const Request& req, size_t i,
                          int64_t value_len, const char* op_name);

  std::string host_;
  int port_;
  std::string username_;
  std::string password_;
  size_t get_min_keys_per_tile_;
  GetBatchMode get_batch_mode_;
  ExistsBatchMode exists_batch_mode_;
  std::mutex worker_fds_mu_;
  std::vector<int> worker_fds_;
};

}  // namespace connector
}  // namespace lmcache
