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

    struct processor {
      std::string name;
    };

    struct connection {
      std::string name;
      processor *source_proc;
      std::string source_rel;
      processor *dest_proc;
      long queued_bytes;
      long max_queued_bytes;
      long queued_count;
      long max_queued_count;
    };

    struct flow {
      std::vector<processor> processors;
      std::vector<connection> connections;
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
