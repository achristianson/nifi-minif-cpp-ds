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

#include <random>

#include <core/ProcessContext.h>
#include <core/ProcessSession.h>
#include "SynthesizeNiFiMetrics.h"

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

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::process(
    std::shared_ptr<io::BaseStream> stream) {
  std::random_device dev;
  std::mt19937 rng(dev());

  // This distribution determines how likely we are to branch. To branch, we
  // must get a 1 out of the number, so the range determines how likely we are
  // to branch at any given point.
  std::uniform_int_distribution<std::mt19937::result_type> branch_dist(1, 10);

  // This distribution determines how many processors a branch in the flow graph
  // will have.
  std::normal_distribution<double> branch_proc_count_dist(10, 2);

  // These distributions determine how fast/slow processors are relative to
  // input bytes and count.
  std::normal_distribution<double> proc_bytes_per_sec_mean_dist(1000000, 20000);
  std::normal_distribution<double> proc_bytes_per_sec_stddev_dist(20000, 5000);
  double proc_bytes_per_sec_min = 10;
  std::normal_distribution<double> proc_count_per_sec_mean_dist(75, 10);
  std::normal_distribution<double> proc_count_per_sec_stddev_dist(5, 2);
  double proc_count_per_sec_min = .1;

  // This distribution determines the size of ingested flow files.
  std::normal_distribution<double> ingest_ff_bytes(50000, 10000);

  // This distribution determines the random walk of ingest rate
  std::normal_distribution<double> ingest_rwalk_dist(0, .1);

  int64_t ret = 0;
  flow flow;
  flow.time_ms = 0;
  unsigned int branch_depth = 0;
  std::list<branch_point> branch_stack;
  size_t next_branch_id = 0;
  branch_stack.emplace_back();
  branch_point *cur_branch = &branch_stack.back();
  cur_branch->id = next_branch_id;
  cur_branch->proc_idx = 0;
  cur_branch->root_proc = nullptr;
  cur_branch->last_proc = nullptr;
  cur_branch->num_procs = static_cast<size_t>(branch_proc_count_dist(rng));
  logger_->log_info("Generating root branch with %d processors",
                    cur_branch->num_procs);
  next_branch_id++;

  // Generate the flow graph.
  for (;;) {
    flow.processors.emplace_back();
    processor &cur_proc = flow.processors.back();
    cur_proc.name = "proc_" + std::to_string(cur_branch->id) + "_" +
                    std::to_string(cur_branch->proc_idx);
    logger_->log_info("Generating processor %s", cur_proc.name);
    cur_proc.num_threads = 1;
    cur_proc.bytes_per_sec = std::normal_distribution<double>(
        proc_bytes_per_sec_mean_dist(rng), proc_bytes_per_sec_stddev_dist(rng));
    cur_proc.count_per_sec = std::normal_distribution<double>(
        proc_count_per_sec_mean_dist(rng), proc_count_per_sec_stddev_dist(rng));

    if (cur_branch->last_proc != nullptr &&
        cur_branch->proc_idx < cur_branch->num_procs) {
      // If we're not at the root of the graph, or the end of a branch, connect
      // the last two processors.
      logger_->log_info("Generating connection from %s to %s",
                        cur_branch->last_proc->name, cur_proc.name);
      flow.connections.emplace_back();
      connection &cur_conn = flow.connections.back();
      cur_conn.source_rel = "success";
      cur_conn.max_queued_bytes = 10000000;
      cur_conn.max_queued_count = 1000;
      cur_conn.name = cur_branch->last_proc->name + "_to_" + cur_proc.name;
      cur_conn.source_proc = cur_branch->last_proc;
      cur_conn.dest_proc = &cur_proc;
      cur_proc.inputs.push_back(&cur_conn);
      cur_branch->last_proc->outputs[cur_conn.source_rel].push_back(&cur_conn);
    }

    cur_branch->last_proc = &cur_proc;

    if (cur_branch->proc_idx == cur_branch->num_procs - 1) {
      // Terminate branch (go up stack or we're done generating the graph, so
      // break out of loop)
      branch_stack.pop_back();
      if (branch_stack.size() == 0) {
        break;
      } else {
        cur_branch = &branch_stack.back();
        branch_depth--;
        logger_->log_info("Continuing at depth %d", branch_depth);
      }
    } else {
      cur_branch->proc_idx++;

      // Add branch in flow.
      if (branch_dist(rng) == 1) {
        branch_stack.emplace_back();
        cur_branch = &branch_stack.back();
        cur_branch->id = next_branch_id;
        cur_branch->num_procs = static_cast<size_t>(std::max(
            1,
            static_cast<int>(branch_proc_count_dist(rng) - branch_depth * 2)));
        logger_->log_info("Generating branch at depth %d with %d processors",
                          branch_depth, cur_branch->num_procs);
        cur_branch->proc_idx = 0;
        cur_branch->root_proc = &cur_proc;
        cur_branch->last_proc = &cur_proc;
        next_branch_id++;
        branch_depth++;
      }
    }
  }

  double ingest_per_sec = 7;
  double ingest_per_sec_max = 50;
  double total_ingested = 0;
  int total_threads = .4 * flow.processors.size();
  int available_threads = total_threads;

  double time_step_ms = 10;
  size_t sim_steps = 12 * 60 * 60 * (1000 / static_cast<size_t>(time_step_ms));

  for (size_t sim_step = 0; sim_step < sim_steps; sim_step++) {
    // random walk the ingest rate
    ingest_per_sec += ingest_rwalk_dist(rng);

    if (ingest_per_sec < 0) {
      ingest_per_sec = 0;
    } else if (ingest_per_sec > ingest_per_sec_max) {
      ingest_per_sec = ingest_per_sec_max;
    }

    // Ingest new flow files into 1st processor.
    unsigned int num_to_ingest = 0;

    if (flow.time_ms > 0) {
      while (((total_ingested + num_to_ingest) / flow.time_ms) <
             (ingest_per_sec / 1000)) {
        num_to_ingest++;
        total_ingested += num_to_ingest;
      }
    }

    for (unsigned int i = 0; i < num_to_ingest; i++) {
      ffile ff;
      ff.time_in_processing_ms = 0;
      ff.size_bytes = ingest_ff_bytes(rng);
      assert(ff.size_bytes > 0);

      for (auto &c : flow.processors.front().outputs["success"]) {
        ffile clone = ff;
        c->enqueue(std::move(clone));
      }
    }

    // Simulate processing of all processors.
    bool any_processing = false;
    bool any_input_exists = false;

    for (auto &p : flow.processors) {
      auto it = p.cur_processing.begin();
      available_threads -= p.num_waiting;
      p.num_waiting = 0;

      while (it != p.cur_processing.end()) {
        any_processing = true;
        (*it).time_in_processing_ms += time_step_ms;

        // When flowfile is done processing, transfer it to outputs.
        if ((*it).time_in_processing_ms >= (*it).time_to_process_ms) {
          bool output_has_backpressure = false;

          for (auto &c : p.outputs["success"]) {
            if (c->queue.size() >= c->max_queued_count) {
              output_has_backpressure = true;
              p.num_waiting++;
              available_threads++;
              break;
            }

            if (c->queued_bytes >= c->max_queued_bytes) {
              output_has_backpressure = true;
              p.num_waiting++;
              available_threads++;
              break;
            }
          }

          // We can only transfer the file out if the queue has space.
          if (!output_has_backpressure) {
            for (auto &c : p.outputs["success"]) {
              ffile clone = *it;
              clone.time_in_processing_ms = 0;
              c->enqueue(std::move(clone));
            }

            p.cur_processing.erase(it++);
            available_threads++;
            assert(available_threads <= total_threads);
          } else {
            it++;
          }
        } else {
          it++;
        }
      }

      // Because threads are available, processor can start working on inputs.
      while (available_threads > 0 && p.cur_processing.size() < p.num_threads &&
             p.num_waiting == 0) {
        bool input_exists = false;
        for (auto &c : p.inputs) {
          if (c->queue.size() > 0) {
            p.cur_processing.push_back(c->dequeue());
            auto &proc_ff = p.cur_processing.back();

            // Determine how long this flow file will take to process.
            auto ms_to_process_bytes =
                (proc_ff.size_bytes /
                 std::max(p.bytes_per_sec(rng), proc_bytes_per_sec_min)) *
                1000;
            auto ms_to_process_count =
                (1 / std::max(p.count_per_sec(rng), proc_count_per_sec_min)) *
                1000;
            proc_ff.time_to_process_ms =
                std::max(ms_to_process_bytes, ms_to_process_count);

            available_threads--;
            input_exists = true;
            any_input_exists = true;
            any_processing = true;
          }

          if (available_threads <= 0 ||
              p.cur_processing.size() > p.num_threads) {
            break;
          }
        }

        if (!input_exists) {
          break;
        }
      }
    }

    // Record the state every second.
    if (flow.time_ms % 1000 == 0) {
      ret += record_state(sim_step, flow, stream);
    }

    // Proceed in time step intervals.
    flow.time_ms += time_step_ms;
  }

  logger_->log_info("Generated %d bytes of synthetic data", ret);

  return ret;
}  // namespace processors

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::write_str(
    const std::string &s, const std::shared_ptr<io::BaseStream> &stream) {
  // This is ugly and will generate a warning --we should change BaseStream to
  // have a way to write const data, aspth 0 wi write does not need to make any
  // data modifications.
  return stream->write((uint8_t *)(&s[0]), s.size());
}

int64_t SynthesizeNiFiMetrics::MetricsWriteCallback::record_state(
    size_t step, flow &state, const std::shared_ptr<io::BaseStream> &stream) {
  int64_t ret = 0;
  // Output header on step 0.
  if (step == 0) {
    size_t conn_idx = 0;
    for (const auto &c : state.connections) {
      ret += write_str(c.name.c_str(), stream);
      ret += write_str("_queued_count,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_max_queued_count,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_queued_bytes,", stream);
      ret += write_str(c.name, stream);
      ret += write_str("_max_queued_bytes", stream);

      if (conn_idx < state.connections.size() - 1) {
        ret += write_str(",", stream);
      } else {
        ret += write_str("\n", stream);
      }

      conn_idx++;
    }
  }

  // Write state values
  size_t conn_idx = 0;

  for (const auto &c : state.connections) {
    ret += write_str(std::to_string(c.queue.size()), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.max_queued_count), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.queued_bytes), stream);
    ret += write_str(",", stream);
    ret += write_str(std::to_string(c.max_queued_bytes), stream);

    if (conn_idx < state.connections.size() - 1) {
      ret += write_str(",", stream);
    } else {
      ret += write_str("\n", stream);
    }

    conn_idx++;
  }

  return ret;
}

}  // namespace processors
}  // namespace minifi
}  // namespace nifi
}  // namespace apache
} /* namespace org */
