#pragma once

#include "market_pulse/spsc_ring.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace market_pulse {

enum class EventType : std::uint8_t { Quote, Trade, Halt, Resume, Heartbeat };
enum class Side : std::uint8_t { Bid, Ask, None };
enum class BackpressurePolicy : std::uint8_t { Spin, Drop };
enum class BenchmarkProfile : std::uint8_t { Throughput, Latency, Balanced };

struct MarketEvent {
    std::uint64_t event_id = 0;
    std::uint32_t symbol_id = 0;
    EventType type = EventType::Quote;
    Side side = Side::None;
    std::uint64_t exchange_timestamp_ns = 0;
    std::uint64_t receive_timestamp_ns = 0;
    std::int64_t price_micros = 0;
    std::uint32_t size = 0;
};

struct TopOfBook {
    std::int64_t bid_price_micros = 0;
    std::int64_t ask_price_micros = 0;
    std::int64_t last_trade_price_micros = 0;
    std::uint64_t traded_volume = 0;
    std::uint64_t trade_count = 0;
    std::uint32_t bid_size = 0;
    std::uint32_t ask_size = 0;
    bool halted = false;

    bool operator==(const TopOfBook&) const = default;
};

struct SimulationConfig {
    std::size_t symbol_count = 8;
    std::size_t event_count = 10000;
    std::size_t producer_count = 1;
    std::size_t capacity = 1024;  // Per producer lane.
    std::size_t consumer_batch = 16;
    std::size_t latency_sample_rate = 64;
    std::uint64_t seed = 1;
    BackpressurePolicy backpressure = BackpressurePolicy::Spin;
    bool chaos = false;
    bool pin_threads = false;
    bool huge_pages = false;
    std::vector<int> cpu_ids;
};

struct SimulationResult {
    std::uint64_t generated = 0;
    std::uint64_t accepted = 0;
    std::uint64_t consumed = 0;
    std::uint64_t halt_events = 0;
    std::uint64_t timestamp_skews = 0;
    std::uint64_t out_of_order_events = 0;
    std::uint64_t burst_storms = 0;
    std::uint64_t p50_latency_ns = 0;
    std::uint64_t p95_latency_ns = 0;
    std::uint64_t p99_latency_ns = 0;
    std::uint64_t p999_latency_ns = 0;
    std::uint64_t max_latency_ns = 0;
    std::uint64_t elapsed_ns = 0;
    double events_per_second = 0.0;
    std::uint64_t state_checksum = 0;
    bool used_invariant_tsc = false;
    bool affinity_applied = false;
    bool huge_pages_advised = false;
    std::vector<std::uint64_t> per_symbol_counts;
    std::vector<TopOfBook> books;
    std::vector<LaneStats> lanes;
    LaneStats ring;
};

struct BenchmarkConfig {
    SimulationConfig simulation;
    BenchmarkProfile profile = BenchmarkProfile::Balanced;
    std::size_t warmup_events = 100000;
    std::size_t repeats = 3;
    bool override_capacity = false;
    bool override_batch = false;
    bool override_sample_rate = false;
};

struct BenchmarkResult {
    BenchmarkProfile profile = BenchmarkProfile::Balanced;
    SimulationResult median;
    std::vector<SimulationResult> runs;
};

SimulationResult run_simulation(const SimulationConfig& config);
BenchmarkResult run_benchmark(const BenchmarkConfig& config);
std::string format_simulation_summary(const SimulationConfig& config, const SimulationResult& result);
std::string format_benchmark_summary(const BenchmarkConfig& config, const BenchmarkResult& result);
std::string format_benchmark_json(const BenchmarkConfig& config, const BenchmarkResult& result);
std::string format_html_report(const SimulationConfig& config, const SimulationResult& result);

}  // namespace market_pulse
