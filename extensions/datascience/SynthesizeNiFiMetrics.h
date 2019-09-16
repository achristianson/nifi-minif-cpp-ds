/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef NIFI_MINIFI_CPP_SYNTHESIZENIFIMETRICS_H
#define NIFI_MINIFI_CPP_SYNTHESIZENIFIMETRICS_H

#include <atomic>
#include <list>
#include <random>

#include <concurrentqueue.h>
#include <core/Processor.h>
#include <core/Resource.h>

namespace org {
namespace apache {
namespace nifi {
namespace minifi {
namespace processors {

class SynthesizeNiFiMetrics : public core::Processor {
 public:
  explicit SynthesizeNiFiMetrics(const std::string &name,
                                 utils::Identifier uuid = utils::Identifier())
      : Processor(name, uuid),
        logger_(logging::LoggerFactory<SynthesizeNiFiMetrics>::getLogger()) {}

  static core::Relationship Success;
  static core::Relationship Retry;
  static core::Relationship Failure;

  void initialize() override;
  void onSchedule(core::ProcessContext *context,
                  core::ProcessSessionFactory *sessionFactory) override;
  void onTrigger(core::ProcessContext *context,
                 core::ProcessSession *session) override {
    logger_->log_error(
        "onTrigger invocation with raw pointers is not implemented");
  }
  void onTrigger(const std::shared_ptr<core::ProcessContext> &context,
                 const std::shared_ptr<core::ProcessSession> &session) override;

  struct input_params {
    // This distribution determines how likely we are to branch. To branch, we
    // must get a 1 out of the number, so the range determines how likely we are
    // to branch at any given point.
    std::uniform_int_distribution<std::mt19937::result_type> branch_dist;

    // This distribution determines how many processors a branch in the flow graph
    // will have.
    std::normal_distribution<double> branch_proc_count_dist;

    // These distributions determine how fast/slow processors are relative to
    // input bytes and count.
    std::normal_distribution<double> proc_bytes_per_sec_mean_dist;
    std::normal_distribution<double> proc_bytes_per_sec_stddev_dist;
    double proc_bytes_per_sec_min;
    std::normal_distribution<double> proc_count_per_sec_mean_dist;
    std::normal_distribution<double> proc_count_per_sec_stddev_dist;
    double proc_count_per_sec_min;

    // This distribution determines the size of ingested flow files.
    std::normal_distribution<double> ingest_ff_bytes;

    // This distribution determines the random walk of ingest rate
    std::normal_distribution<double> ingest_rwalk_dist;

    input_params() : branch_dist(1, 10),
                     branch_proc_count_dist(10, 2),
                     proc_bytes_per_sec_mean_dist(1000000, 20000),
                     proc_bytes_per_sec_stddev_dist(20000, 5000),
                     proc_bytes_per_sec_min(10),
                     proc_count_per_sec_mean_dist(75, 10),
                     proc_count_per_sec_stddev_dist(5, 2),
                     proc_count_per_sec_min(.1),
                     ingest_ff_bytes(50000, 10000),
                     ingest_rwalk_dist(0, .1) {};
  };

  class ParamsReadCallback : public InputStreamCallback {
   public:
    explicit ParamsReadCallback(input_params *p) : params_(p) {}
    ~ParamsReadCallback() override = default;

    int64_t process(std::shared_ptr<io::BaseStream> stream) override {
      std::vector<uint8_t> buf;
      auto size = stream->getSize();
      buf.resize(size);
      stream->readData(buf, size);
      rapidjson::Document params_doc;
      params_doc.Parse(reinterpret_cast<char *>(&buf[0]), size);

      if (params_doc.HasMember("branch_dist_mean")
          && params_doc.HasMember("branch_dist_stddev")) {
        params_->branch_dist = std::uniform_int_distribution<std::mt19937::result_type>(
            params_doc["branch_dist_mean"].GetInt(),
            params_doc["branch_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("branch_proc_count_dist_mean")
          && params_doc.HasMember("branch_proc_count_dist_stddev")) {
        params_->branch_proc_count_dist = std::normal_distribution<double>(
            params_doc["branch_proc_count_dist_mean"].GetInt(),
            params_doc["branch_proc_count_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("proc_bytes_per_sec_mean_dist_mean")
          && params_doc.HasMember("proc_bytes_per_sec_mean_dist_stddev")) {
        params_->proc_bytes_per_sec_mean_dist = std::normal_distribution<double>(
            params_doc["proc_bytes_per_sec_mean_dist_mean"].GetInt(),
            params_doc["proc_bytes_per_sec_mean_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("proc_bytes_per_sec_stddev_dist_mean")
          && params_doc.HasMember("proc_bytes_per_sec_stddev_dist_stddev")) {
        params_->proc_bytes_per_sec_stddev_dist = std::normal_distribution<double>(
            params_doc["proc_bytes_per_sec_stddev_dist_mean"].GetInt(),
            params_doc["proc_bytes_per_sec_stddev_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("proc_bytes_per_sec_min")) {
        params_->proc_bytes_per_sec_min =
            params_doc["proc_bytes_per_sec_min"].GetDouble();
      }

      if (params_doc.HasMember("proc_count_per_sec_mean_dist_mean")
          && params_doc.HasMember("proc_count_per_sec_mean_dist_stddev")) {
        params_->proc_count_per_sec_mean_dist = std::normal_distribution<double>(
            params_doc["proc_count_per_sec_mean_dist_mean"].GetInt(),
            params_doc["proc_count_per_sec_mean_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("proc_count_per_sec_stddev_dist_mean")
          && params_doc.HasMember("proc_count_per_sec_stddev_dist_stddev")) {
        params_->proc_count_per_sec_stddev_dist = std::normal_distribution<double>(
            params_doc["proc_count_per_sec_stddev_dist_mean"].GetInt(),
            params_doc["proc_count_per_sec_stddev_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("proc_count_per_sec_min")) {
        params_->proc_count_per_sec_min =
            params_doc["proc_count_per_sec_min"].GetDouble();
      }

      if (params_doc.HasMember("ingest_ff_bytes_dist_mean")
          && params_doc.HasMember("ingest_ff_bytes_dist_stddev")) {
        params_->ingest_ff_bytes = std::normal_distribution<double>(
            params_doc["ingest_ff_bytes_dist_mean"].GetInt(),
            params_doc["ingest_ff_bytes_dist_stddev"].GetInt()
        );
      }

      if (params_doc.HasMember("ingest_rwalk_dist_mean")
          && params_doc.HasMember("ingest_rwalk_dist_stddev")) {
        params_->ingest_rwalk_dist = std::normal_distribution<double>(
            params_doc["ingest_rwalk_dist_mean"].GetInt(),
            params_doc["ingest_rwalk_dist_stddev"].GetInt()
        );
      }

      return size;
    }

   private:
    input_params *params_;
  };

  class MetricsWriteCallback : public OutputStreamCallback {
   public:
    explicit MetricsWriteCallback(input_params *params)
        : logger_(logging::LoggerFactory<MetricsWriteCallback>::getLogger()), params_(params) {}
    ~MetricsWriteCallback() override = default;
    static int64_t write_str(const std::string &,
                      const std::shared_ptr<io::BaseStream> &stream);
    int64_t process(std::shared_ptr<io::BaseStream> stream) override;

    struct connection;

    struct ffile {
      connection *cur_connection;

      double size_bytes;
      double time_to_process_ms;
      double time_in_processing_ms;
    };

    struct processor {
      std::string name;
      std::list<connection *> inputs;
      std::map<std::string, std::list<connection *>> outputs;

      std::list<ffile> cur_processing;
      size_t num_waiting;
      unsigned int active_threads;

      std::normal_distribution<double> bytes_per_sec;
      std::normal_distribution<double> count_per_sec;

      size_t bytes_processed;
      size_t count_processed;
    };

    struct connection {
      std::string name;
      processor *source_proc;
      std::string source_rel;
      processor *dest_proc;
      unsigned long max_queued_bytes;
      unsigned long max_queued_count;
      std::list<ffile> queue;
      size_t queued_bytes;

      void enqueue(ffile &&f) {
        queue.emplace_back(f);
        queued_bytes += f.size_bytes;
      }

      ffile dequeue() {
        ffile f = queue.front();
        queue.pop_front();

        if (queue.empty()) {
          queued_bytes = 0;
        } else {
          queued_bytes -= f.size_bytes;
        }

        return f;
      }
    };

    struct branch_point {
      size_t id;
      size_t num_procs;
      size_t proc_idx;
      processor *root_proc;
      processor *last_proc;
    };

    struct flow {
      unsigned long long time_ms;
      double proc_ffiles_per_sec;
      double proc_bytes_per_sec;
      std::list<processor> processors;
      std::list<connection> connections;
      int total_threads;
      int available_threads;
      size_t bytes_ingested;
      size_t count_ingested;
    };

   private:
    std::shared_ptr<logging::Logger> logger_;
    input_params *params_;
    static int64_t record_state(size_t time, bool headers, double ingest_per_sec,
                         flow &state,
                         const std::shared_ptr<io::BaseStream> &stream);
  };

 private:
  std::shared_ptr<logging::Logger> logger_;
};

REGISTER_RESOURCE(SynthesizeNiFiMetrics,
                  "Synthesizes NiFi operating metrics for ML model testing and "
                  "evaulation.");  // NOLINT

} /* namespace processors */
} /* namespace minifi */
} /* namespace nifi */
} /* namespace apache */
} /* namespace org */

#endif  // NIFI_MINIFI_CPP_SYNTHESIZENIFIMETRICS_H
