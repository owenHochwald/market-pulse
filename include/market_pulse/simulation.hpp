#pragma once

#include "market_pulse/ring_buffer.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace market_pulse {

enum class EventType : std::uint8_t {
    Quote,
    Trade,
    Halt,
    Resume,
    Heartbeat,
};

enum class Side : std::uint8_t {
    Bid,
    Ask,
    None,
};

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

struct SimulationConfig {
    std::size_t symbol_count = 8;
    std::size_t event_count = 10000;
    std::size_t producer_count = 1;
    std::size_t capacity = 1024;
    std::uint64_t seed = 1;
    bool chaos = false;
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
    std::vector<std::uint64_t> per_symbol_counts;
    RingStats ring;
};

int smoke_value();
SimulationResult run_simulation(const SimulationConfig& config);
std::string format_simulation_summary(const SimulationConfig& config, const SimulationResult& result);

}  // namespace market_pulse
