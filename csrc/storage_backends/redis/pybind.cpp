// SPDX-License-Identifier: Apache-2.0
#include <pybind11/pybind11.h>
#include "../connector_pybind_utils.h"
#include "connector.h"

namespace py = pybind11;

PYBIND11_MODULE(lmcache_redis, m) {
  py::class_<lmcache::connector::RedisConnector>(m, "LMCacheRedisClient")
      .def(py::init<std::string, int, int, std::string, std::string, size_t,
                    std::string, std::string>(),
           py::arg("host"), py::arg("port"), py::arg("num_workers"),
           py::arg("username") = "", py::arg("password") = "",
           py::arg("get_min_keys_per_tile") = 8,
           py::arg("get_batch_mode") = "pipeline",
           py::arg("exists_batch_mode") = "pipeline")
          LMCACHE_BIND_CONNECTOR_METHODS(lmcache::connector::RedisConnector);
}
