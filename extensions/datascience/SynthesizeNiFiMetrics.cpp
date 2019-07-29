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

#include "SynthesizeNiFiMetrics.h"
#include <core/ProcessContext.h>
#include <core/ProcessSession.h>

namespace org {
namespace apache {
namespace nifi {
namespace minifi {
namespace processors {

core::Property SynthesizeNiFiMetrics::InputNode(
    core::PropertyBuilder::createProperty("Input Node")
        ->withDescription(
            "The node of the TensorFlow graph to feed tensor inputs to")
        ->withDefaultValue("")
        ->build());

core::Property SynthesizeNiFiMetrics::OutputNode(
    core::PropertyBuilder::createProperty("Output Node")
        ->withDescription(
            "The node of the TensorFlow graph to read tensor outputs from")
        ->withDefaultValue("")
        ->build());

core::Relationship SynthesizeNiFiMetrics::Success(  // NOLINT
    "success", "Successful graph application outputs");
core::Relationship SynthesizeNiFiMetrics::Retry(  // NOLINT
    "retry", "Inputs which fail graph application but may work if sent again");
core::Relationship SynthesizeNiFiMetrics::Failure(  // NOLINT
    "failure", "Failures which will not work if retried");

void SynthesizeNiFiMetrics::initialize() {
  std::set<core::Property> properties;
  properties.insert(InputNode);
  properties.insert(OutputNode);
  setSupportedProperties(std::move(properties));

  std::set<core::Relationship> relationships;
  relationships.insert(Success);
  relationships.insert(Retry);
  relationships.insert(Failure);
  setSupportedRelationships(std::move(relationships));
}

void SynthesizeNiFiMetrics::onSchedule(
    core::ProcessContext *context,
    core::ProcessSessionFactory *sessionFactory) {
  context->getProperty(InputNode.getName(), input_node_);

  if (input_node_.empty()) {
    logger_->log_error("Invalid input node");
  }

  context->getProperty(OutputNode.getName(), output_node_);

  if (output_node_.empty()) {
    logger_->log_error("Invalid output node");
  }
}

void SynthesizeNiFiMetrics::onTrigger(
    const std::shared_ptr<core::ProcessContext> &context,
    const std::shared_ptr<core::ProcessSession> &session) {
  try {
    logger_->log_info("Starting NiFi metrics synthesis");
    auto flow_file = session->create();
    MetricsWriteCallback cb;
    session->write(flow_file, &cb);
    flow_file->setAttribute("filename", "synth-data");
    session->transfer(flow_file, Success);
    //    session->commit();
  } catch (std::exception &exception) {
    logger_->log_error("Caught Exception %s", exception.what());
    //    session->transfer(flow_file, Failure);
    this->yield();
  } catch (...) {
    logger_->log_error("Caught Exception");
    //    session->transfer(flow_file, Failure);
    this->yield();
  }
}

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::write_str(
    const std::string &s, const std::shared_ptr<io::BaseStream> &stream) {
  // This is ugly and will generate a warning --we should change BaseStream to
  // have a way to write const data, as write does not need to make any data
  // modifications.
  return stream->write((uint8_t *)(&s[0]), s.size());
}

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::record_state(
    size_t step, flow &state, const std::shared_ptr<io::BaseStream> &stream) {
  int64_t ret = 0;
  // Output header on step 0.
  if (step == 0) {
    for (size_t i = 0; i < state.connections.size(); i++) {
      auto &c = state.connections[i];
      ret += write_str(c.name.c_str(), stream);
      ret += write_str("_queued_count,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_max_queued_count,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_queued_bytes,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_max_queued_bytes", stream);

      if (i < state.connections.size() - 1) {
        ret += write_str(",", stream);
      } else {
        ret += write_str("\n", stream);
      }
    }
  }

  // Write state values
  for (size_t i = 0; i < state.connections.size(); i++) {
    const auto &c = state.connections[i];
    ret += write_str(std::to_string(c.queued_count), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.max_queued_count), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.queued_bytes), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.max_queued_bytes), stream);

    if (i < state.connections.size() - 1) {
      ret += write_str(",", stream);
    } else {
      ret += write_str("\n", stream);
    }
  }

  return ret;
}

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::process(
    std::shared_ptr<io::BaseStream> stream) {
  int64_t ret = 0;
  flow f;
  processor proc;
  connection conn;
  conn.source_rel = "success";
  conn.queued_bytes = 0;
  conn.max_queued_bytes = 100000;
  conn.queued_count = 0;
  conn.max_queued_count = 1000;
  size_t num_procs = 5;

  for (size_t i = 0; i < num_procs; i++) {
    proc.name = "proc_" + std::to_string(i);
    f.processors.push_back(proc);

    if (i > 0 && i < num_procs - 1) {
      conn.name = f.processors[i - 1].name + "_to_" + f.processors[i].name;
      conn.source_proc = &f.processors[i - 1];
      conn.source_proc = &f.processors[i];
      f.connections.push_back(conn);
    }
  }

  size_t sim_steps = 100;

  for (size_t sim_step = 0; sim_step < sim_steps; sim_step++) {
    ret += record_state(sim_step, f, stream);
  }

  logger_->log_info("Generated %d bytes of synthetic data", ret);

  return ret;
}

} /* namespace processors */
} /* namespace minifi */
} /* namespace nifi */
} /* namespace apache */
} /* namespace org */
