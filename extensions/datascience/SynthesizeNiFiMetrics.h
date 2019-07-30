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

  static core::Property InputNode;
  static core::Property OutputNode;

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

  class MetricsWriteCallback : public OutputStreamCallback {
   public:
    explicit MetricsWriteCallback()
        : logger_(logging::LoggerFactory<MetricsWriteCallback>::getLogger()) {}
    ~MetricsWriteCallback() override = default;
    int64_t write_str(const std::string &,
                      const std::shared_ptr<io::BaseStream> &stream);
    int64_t process(std::shared_ptr<io::BaseStream> stream) override;

    struct connection;

    struct ffile {
      connection *cur_connection;

      double size_bytes;
      double time_to_process_ms;
      double time_in_processing_ms;
    };

    struct connection;

    struct processor {
      std::string name;
      std::list<connection *> inputs;
      std::map<std::string, std::list<connection *>> outputs;

      std::list<ffile> cur_processing;
      size_t num_waiting;
      unsigned int num_threads;

      std::normal_distribution<double> bytes_per_sec;
      std::normal_distribution<double> count_per_sec;

      double bytes_processed;
      double count_processed;
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
        queue.emplace_back(std::move(f));
        queued_bytes += f.size_bytes;
      }

      ffile dequeue() {
        ffile f = queue.front();
        queue.pop_front();

        if (queue.size() == 0) {
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
    };

   private:
    std::shared_ptr<logging::Logger> logger_;
    int64_t record_state(size_t step, flow &state,
                         const std::shared_ptr<io::BaseStream> &stream);
  };

 private:
  std::shared_ptr<logging::Logger> logger_;
  std::string input_node_;
  std::string output_node_;
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
