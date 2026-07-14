#include "market_pulse/simulation.hpp"

#include "market_pulse/platform.hpp"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace market_pulse {
namespace {

struct QueuedEvent {
    MarketEvent event;
    std::uint64_t enqueue_timestamp = 0;
};

struct ProducerMetrics {
    std::uint64_t generated = 0;
    std::uint64_t halt_events = 0;
    std::uint64_t timestamp_skews = 0;
    std::uint64_t out_of_order_events = 0;
    std::uint64_t burst_storms = 0;
};

class EventCursor {
public:
    EventCursor(const SimulationConfig& config, std::size_t lane) : config_(config) {
        for (std::size_t symbol = lane; symbol < config.symbol_count; symbol += config.producer_count) {
            symbols_.push_back(symbol);
        }
    }

    bool next(MarketEvent& event) {
        while (symbol_index_ < symbols_.size()) {
            const auto symbol = symbols_[symbol_index_];
            const auto event_id = symbol + round_ * config_.symbol_count;
            advance();
            if (event_id < config_.event_count) {
                event = make(event_id, static_cast<std::uint32_t>(symbol));
                return true;
            }
        }
        return false;
    }

private:
    void advance() {
        ++symbol_index_;
        if (symbol_index_ == symbols_.size()) {
            symbol_index_ = 0;
            ++round_;
            if (!symbols_.empty() && symbols_.front() + round_ * config_.symbol_count >= config_.event_count) {
                symbol_index_ = symbols_.size();
            }
        }
    }

    MarketEvent make(std::uint64_t event_id, std::uint32_t symbol_id) const {
        const auto base_price = 100'000'000 + static_cast<std::int64_t>(symbol_id) * 1'000'000;
        auto event_type = event_id % 3 == 0 ? EventType::Trade : EventType::Quote;
        auto exchange_timestamp = config_.seed * 1'000'000 + event_id * 100;
        auto receive_timestamp = exchange_timestamp + 25;

        if (config_.chaos) {
            if (event_id % 17 == 0) {
                event_type = EventType::Halt;
            } else if (event_id % 17 == 4) {
                event_type = EventType::Resume;
            } else if (event_id % 11 == 0) {
                event_type = EventType::Heartbeat;
            }
            if (event_id % 7 == 0) {
                receive_timestamp = exchange_timestamp > 250 ? exchange_timestamp - 250 : 0;
            }
            if (event_id % 13 == 0 && event_id > 0) {
                exchange_timestamp -= 1'000;
            }
        }

        return {
            event_id,
            symbol_id,
            event_type,
            event_id % 2 == 0 ? Side::Bid : Side::Ask,
            exchange_timestamp,
            receive_timestamp,
            base_price + static_cast<std::int64_t>((event_id % 17) * 100),
            static_cast<std::uint32_t>(100 + (event_id % 50)),
        };
    }

    const SimulationConfig& config_;
    std::vector<std::size_t> symbols_;
    std::size_t symbol_index_ = 0;
    std::uint64_t round_ = 0;
};

void record_generated(const SimulationConfig& config, const MarketEvent& event,
                      ProducerMetrics& metrics) noexcept {
    ++metrics.generated;
    if (event.type == EventType::Halt || event.type == EventType::Resume) {
        ++metrics.halt_events;
    }
    if (event.receive_timestamp_ns < event.exchange_timestamp_ns) {
        ++metrics.timestamp_skews;
    }
    if (config.chaos && event.event_id % 13 == 0 && event.event_id > 0) {
        ++metrics.out_of_order_events;
    }
    if (config.chaos && event.event_id % 16 == 0) {
        ++metrics.burst_storms;
    }
}

void apply_event(const MarketEvent& event, TopOfBook& book) noexcept {
    switch (event.type) {
        case EventType::Quote:
            if (event.side == Side::Bid) {
                book.bid_price_micros = event.price_micros;
                book.bid_size = event.size;
            } else if (event.side == Side::Ask) {
                book.ask_price_micros = event.price_micros;
                book.ask_size = event.size;
            }
            break;
        case EventType::Trade:
            book.last_trade_price_micros = event.price_micros;
            book.traded_volume += event.size;
            ++book.trade_count;
            break;
        case EventType::Halt:
            book.halted = true;
            break;
        case EventType::Resume:
            book.halted = false;
            break;
        case EventType::Heartbeat:
            break;
    }
}

std::uint64_t hash_value(std::uint64_t hash, std::uint64_t value) noexcept {
    constexpr std::uint64_t prime = 1099511628211ULL;
    for (int byte = 0; byte < 8; ++byte) {
        hash ^= (value >> (byte * 8)) & 0xffU;
        hash *= prime;
    }
    return hash;
}

std::uint64_t state_checksum(const std::vector<TopOfBook>& books) noexcept {
    std::uint64_t hash = 1469598103934665603ULL;
    for (const auto& book : books) {
        hash = hash_value(hash, static_cast<std::uint64_t>(book.bid_price_micros));
        hash = hash_value(hash, static_cast<std::uint64_t>(book.ask_price_micros));
        hash = hash_value(hash, static_cast<std::uint64_t>(book.last_trade_price_micros));
        hash = hash_value(hash, book.traded_volume);
        hash = hash_value(hash, book.trade_count);
        hash = hash_value(hash, book.bid_size);
        hash = hash_value(hash, book.ask_size);
        hash = hash_value(hash, book.halted ? 1 : 0);
    }
    return hash;
}

std::uint64_t quantile(const std::vector<std::uint64_t>& sorted, double value) noexcept {
    if (sorted.empty()) {
        return 0;
    }
    const auto index = static_cast<std::size_t>((sorted.size() - 1) * value);
    return sorted[index];
}

LaneStats aggregate_lanes(const std::vector<LaneStats>& lanes) noexcept {
    LaneStats total;
    for (const auto& lane : lanes) {
        total.pushed += lane.pushed;
        total.popped += lane.popped;
        total.dropped += lane.dropped;
        total.full_observations += lane.full_observations;
        total.spin_iterations += lane.spin_iterations;
        total.current_depth += lane.current_depth;
        total.max_depth = std::max(total.max_depth, lane.max_depth);
    }
    return total;
}

std::string profile_label(BenchmarkProfile profile) {
    switch (profile) {
        case BenchmarkProfile::Throughput: return "throughput";
        case BenchmarkProfile::Latency: return "latency";
        case BenchmarkProfile::Balanced: return "balanced";
    }
    return "unknown";
}

SimulationConfig configured_profile(const BenchmarkConfig& config) {
    auto simulation = config.simulation;
    switch (config.profile) {
        case BenchmarkProfile::Throughput:
            if (!config.override_capacity) simulation.capacity = 65536;
            if (!config.override_batch) simulation.consumer_batch = 8192;
            if (!config.override_sample_rate) simulation.latency_sample_rate = 0;
            break;
        case BenchmarkProfile::Latency:
            if (!config.override_capacity) simulation.capacity = 64;
            if (!config.override_batch) simulation.consumer_batch = 1;
            if (!config.override_sample_rate) simulation.latency_sample_rate = 64;
            break;
        case BenchmarkProfile::Balanced:
            if (!config.override_capacity) simulation.capacity = 1024;
            if (!config.override_batch) simulation.consumer_batch = 16;
            if (!config.override_sample_rate) simulation.latency_sample_rate = 64;
            break;
    }
    return simulation;
}

std::string bool_label(bool value) { return value ? "on" : "off"; }

}  // namespace

SimulationResult run_simulation(const SimulationConfig& config) {
    if (config.symbol_count == 0 || config.producer_count == 0 || config.event_count == 0) {
        throw std::invalid_argument("symbols, producers, and events must be greater than zero");
    }
    if (config.producer_count > config.symbol_count) {
        throw std::invalid_argument("producer_count cannot exceed symbol_count");
    }
    if (config.consumer_batch == 0) {
        throw std::invalid_argument("consumer_batch must be greater than zero");
    }
    if (!config.cpu_ids.empty() && config.cpu_ids.size() < config.producer_count + 1) {
        throw std::invalid_argument("CPU list must contain the consumer CPU followed by every producer CPU");
    }

    MonotonicClock clock;
    std::vector<std::unique_ptr<SpscRing<QueuedEvent>>> lanes;
    lanes.reserve(config.producer_count);
    for (std::size_t lane = 0; lane < config.producer_count; ++lane) {
        lanes.push_back(std::make_unique<SpscRing<QueuedEvent>>(config.capacity));
    }
    const bool huge_pages_advised = config.huge_pages &&
        std::all_of(lanes.begin(), lanes.end(), [](const auto& lane) { return lane->advise_huge_pages(); });

    auto done = std::make_unique<std::atomic<bool>[]>(config.producer_count);
    std::vector<ProducerMetrics> producer_metrics(config.producer_count);
    std::vector<std::uint8_t> affinity_results(config.producer_count + 1, 0);
    auto cpu_ids = config.cpu_ids.empty() ? available_cpu_ids() : config.cpu_ids;
    std::barrier start_barrier(static_cast<std::ptrdiff_t>(config.producer_count + 2));
    std::vector<std::uint64_t> per_symbol_counts(config.symbol_count, 0);
    std::vector<TopOfBook> books(config.symbol_count);
    std::vector<std::uint64_t> latencies;
    if (config.latency_sample_rate != 0) {
        latencies.reserve(config.event_count / config.latency_sample_rate + config.producer_count);
    }

    std::thread consumer([&] {
        if (config.pin_threads && !cpu_ids.empty()) {
            affinity_results[0] = pin_current_thread(cpu_ids[0]) ? 1 : 0;
        }
        start_barrier.arrive_and_wait();

        for (;;) {
            bool made_progress = false;
            for (std::size_t lane_index = 0; lane_index < lanes.size(); ++lane_index) {
                auto span = lanes[lane_index]->reserve_read(config.consumer_batch);
                if (!span) {
                    continue;
                }
                made_progress = true;
                for (std::size_t index = 0; index < span.size; ++index) {
                    const auto& queued = span.data[index];
                    const auto& event = queued.event;
                    ++per_symbol_counts[event.symbol_id];
                    apply_event(event, books[event.symbol_id]);
                    if (queued.enqueue_timestamp != 0) {
                        const auto received = clock.consumer_timestamp();
                        if (received >= queued.enqueue_timestamp) {
                            latencies.push_back(clock.to_nanoseconds(received - queued.enqueue_timestamp));
                        }
                    }
                }
                lanes[lane_index]->commit_read(span.size);
            }

            if (made_progress) {
                continue;
            }
            bool all_done = true;
            for (std::size_t lane = 0; lane < config.producer_count; ++lane) {
                all_done &= done[lane].load(std::memory_order_acquire);
            }
            if (all_done) {
                bool any_remaining = false;
                for (auto& lane : lanes) {
                    if (lane->reserve_read(1)) {
                        any_remaining = true;
                        break;
                    }
                }
                if (!any_remaining) {
                    break;
                }
                continue;
            }
            cpu_relax();
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(config.producer_count);
    for (std::size_t lane_index = 0; lane_index < config.producer_count; ++lane_index) {
        producers.emplace_back([&, lane_index] {
            if (config.pin_threads && cpu_ids.size() > lane_index + 1) {
                affinity_results[lane_index + 1] = pin_current_thread(cpu_ids[lane_index + 1]) ? 1 : 0;
            }
            EventCursor cursor(config, lane_index);
            ProducerMetrics local_metrics;
            MarketEvent pending;
            bool has_pending = cursor.next(pending);
            start_barrier.arrive_and_wait();

            while (has_pending) {
                auto span = lanes[lane_index]->reserve_write(config.consumer_batch);
                if (!span) {
                    if (config.backpressure == BackpressurePolicy::Drop) {
                        record_generated(config, pending, local_metrics);
                        lanes[lane_index]->note_drop();
                        has_pending = cursor.next(pending);
                    } else {
                        lanes[lane_index]->note_spin();
                        cpu_relax();
                    }
                    continue;
                }

                std::size_t produced = 0;
                while (produced < span.size && has_pending) {
                    record_generated(config, pending, local_metrics);
                    span.data[produced].event = pending;
                    span.data[produced].enqueue_timestamp =
                        config.latency_sample_rate != 0 && pending.event_id % config.latency_sample_rate == 0
                            ? clock.producer_timestamp()
                            : 0;
                    ++produced;
                    has_pending = cursor.next(pending);
                }
                lanes[lane_index]->commit_write(produced);
            }
            producer_metrics[lane_index] = local_metrics;
            done[lane_index].store(true, std::memory_order_release);
        });
    }

    const auto started = std::chrono::steady_clock::now();
    start_barrier.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }
    consumer.join();
    const auto stopped = std::chrono::steady_clock::now();

    SimulationResult result;
    result.elapsed_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(stopped - started).count());
    result.per_symbol_counts = std::move(per_symbol_counts);
    result.books = std::move(books);
    result.state_checksum = state_checksum(result.books);
    result.used_invariant_tsc = clock.uses_invariant_tsc();
    result.affinity_applied = config.pin_threads &&
        std::all_of(affinity_results.begin(), affinity_results.end(), [](std::uint8_t value) { return value != 0; });
    result.huge_pages_advised = huge_pages_advised;

    for (std::size_t lane = 0; lane < lanes.size(); ++lane) {
        result.generated += producer_metrics[lane].generated;
        result.halt_events += producer_metrics[lane].halt_events;
        result.timestamp_skews += producer_metrics[lane].timestamp_skews;
        result.out_of_order_events += producer_metrics[lane].out_of_order_events;
        result.burst_storms += producer_metrics[lane].burst_storms;
        result.lanes.push_back(lanes[lane]->stats());
    }
    result.ring = aggregate_lanes(result.lanes);
    result.accepted = result.ring.pushed;
    result.consumed = result.ring.popped;
    result.events_per_second = result.elapsed_ns == 0 ? 0.0 :
        static_cast<double>(result.consumed) * 1'000'000'000.0 / static_cast<double>(result.elapsed_ns);

    std::sort(latencies.begin(), latencies.end());
    result.p50_latency_ns = quantile(latencies, 0.50);
    result.p95_latency_ns = quantile(latencies, 0.95);
    result.p99_latency_ns = quantile(latencies, 0.99);
    result.p999_latency_ns = quantile(latencies, 0.999);
    result.max_latency_ns = latencies.empty() ? 0 : latencies.back();
    return result;
}

BenchmarkResult run_benchmark(const BenchmarkConfig& config) {
    if (config.repeats == 0) {
        throw std::invalid_argument("benchmark repeats must be greater than zero");
    }
    auto simulation = configured_profile(config);
    if (config.warmup_events != 0) {
        auto warmup = simulation;
        warmup.event_count = config.warmup_events;
        warmup.latency_sample_rate = 0;
        (void)run_simulation(warmup);
    }

    BenchmarkResult result;
    result.profile = config.profile;
    result.runs.reserve(config.repeats);
    for (std::size_t repeat = 0; repeat < config.repeats; ++repeat) {
        result.runs.push_back(run_simulation(simulation));
    }
    std::vector<std::size_t> order(result.runs.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](std::size_t left, std::size_t right) {
        return result.runs[left].events_per_second < result.runs[right].events_per_second;
    });
    result.median = result.runs[order[order.size() / 2]];
    return result;
}

std::string format_simulation_summary(const SimulationConfig& config, const SimulationResult& result) {
    std::ostringstream output;
    output << std::fixed << std::setprecision(2)
           << "market-pulse simulation\n"
           << "symbols=" << config.symbol_count << " events=" << config.event_count
           << " producers=" << config.producer_count << " lane_capacity=" << config.capacity
           << " batch=" << config.consumer_batch << " chaos=" << bool_label(config.chaos) << '\n'
           << "generated=" << result.generated << " accepted=" << result.accepted
           << " consumed=" << result.consumed << " drops=" << result.ring.dropped
           << " full_observations=" << result.ring.full_observations
           << " spins=" << result.ring.spin_iterations << '\n'
           << "depth_current=" << result.ring.current_depth << " depth_max=" << result.ring.max_depth << '\n'
           << "elapsed_ns=" << result.elapsed_ns << " events_per_second=" << result.events_per_second << '\n'
           << "p50_handoff_ns=" << result.p50_latency_ns << " p95_handoff_ns=" << result.p95_latency_ns
           << " p99_handoff_ns=" << result.p99_latency_ns << " p999_handoff_ns=" << result.p999_latency_ns
           << " max_handoff_ns=" << result.max_latency_ns << '\n'
           << "state_checksum=" << result.state_checksum << " invariant_tsc=" << bool_label(result.used_invariant_tsc)
           << " affinity=" << bool_label(result.affinity_applied)
           << " huge_pages=" << bool_label(result.huge_pages_advised) << '\n'
           << "halt_events=" << result.halt_events << " timestamp_skews=" << result.timestamp_skews
           << " out_of_order_events=" << result.out_of_order_events << " burst_storms=" << result.burst_storms << '\n';
    return output.str();
}

std::string format_benchmark_summary(const BenchmarkConfig& config, const BenchmarkResult& result) {
    auto simulation = configured_profile(config);
    std::ostringstream output;
    output << "market-pulse benchmark\nprofile=" << profile_label(config.profile)
           << " repeats=" << config.repeats << " warmup_events=" << config.warmup_events << '\n'
           << format_simulation_summary(simulation, result.median);
    return output.str();
}

std::string format_benchmark_json(const BenchmarkConfig& config, const BenchmarkResult& result) {
    std::ostringstream output;
    output << std::fixed << std::setprecision(2)
           << "{\n  \"profile\": \"" << profile_label(config.profile) << "\",\n"
           << "  \"repeats\": " << config.repeats << ",\n"
           << "  \"events_per_second\": " << result.median.events_per_second << ",\n"
           << "  \"elapsed_ns\": " << result.median.elapsed_ns << ",\n"
           << "  \"p50_handoff_ns\": " << result.median.p50_latency_ns << ",\n"
           << "  \"p99_handoff_ns\": " << result.median.p99_latency_ns << ",\n"
           << "  \"p999_handoff_ns\": " << result.median.p999_latency_ns << ",\n"
           << "  \"max_handoff_ns\": " << result.median.max_latency_ns << ",\n"
           << "  \"drops\": " << result.median.ring.dropped << ",\n"
           << "  \"state_checksum\": \"" << result.median.state_checksum << "\"\n}\n";
    return output.str();
}

std::string format_html_report(const SimulationConfig& config, const SimulationResult& result) {
    std::ostringstream output;
    output << std::fixed << std::setprecision(2)
           << "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
           << "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
           << "<title>market-pulse report</title><style>"
           << "body{font:15px system-ui;margin:0;color:#17202a}main{max-width:960px;margin:auto;padding:32px 20px}"
           << "h1{font-size:32px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}"
           << ".metric{border:1px solid #d5dbe3;border-radius:6px;padding:14px}.metric strong{display:block;font-size:22px}"
           << "table{border-collapse:collapse;width:100%}th,td{padding:9px;border-bottom:1px solid #ddd;text-align:left}"
           << "</style></head><body><main><h1>market-pulse report</h1>"
           << "<p>Cache-isolated SPSC market replay with measured producer-to-consumer handoff latency.</p>"
           << "<h2>Run Summary</h2><div class=\"grid\">"
           << "<div class=\"metric\"><strong>" << result.consumed << "</strong>consumed</div>"
           << "<div class=\"metric\"><strong>" << result.events_per_second << "</strong>events/sec</div>"
           << "<div class=\"metric\"><strong>" << result.ring.dropped << "</strong>actual drops</div>"
           << "<div class=\"metric\"><strong>" << result.state_checksum << "</strong>state checksum</div></div>"
           << "<h2>Measured Handoff Latency</h2><div class=\"grid\">"
           << "<div class=\"metric\"><strong>" << result.p50_latency_ns << " ns</strong>p50</div>"
           << "<div class=\"metric\"><strong>" << result.p99_latency_ns << " ns</strong>p99</div>"
           << "<div class=\"metric\"><strong>" << result.p999_latency_ns << " ns</strong>p99.9</div></div>"
           << "<h2>Configuration</h2><table>"
           << "<tr><th>symbols</th><td>" << config.symbol_count << "</td></tr>"
           << "<tr><th>events</th><td>" << config.event_count << "</td></tr>"
           << "<tr><th>producer lanes</th><td>" << config.producer_count << "</td></tr>"
           << "<tr><th>capacity per lane</th><td>" << config.capacity << "</td></tr>"
           << "<tr><th>batch</th><td>" << config.consumer_batch << "</td></tr></table>"
           << "<h2>Backpressure</h2><p>Full observations: " << result.ring.full_observations
           << "; spin iterations: " << result.ring.spin_iterations << "; max lane depth: " << result.ring.max_depth << ".</p>"
           << "<h2>Test Suite Coverage</h2><p>SPSC wraparound, batch reservations, concurrent integrity, deterministic state, metrics, and reports.</p>"
           << "<p><code>market-pulse simulate --symbols " << config.symbol_count << " --events " << config.event_count
           << " --producers " << config.producer_count << " --capacity " << config.capacity << "</code></p>"
           << "</main></body></html>\n";
    return output.str();
}

}  // namespace market_pulse
