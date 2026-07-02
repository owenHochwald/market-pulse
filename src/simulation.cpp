#include "market_pulse/simulation.hpp"

#include <atomic>
#include <stdexcept>
#include <thread>
#include <vector>

namespace market_pulse {

int smoke_value() {
    return 42;
}

namespace {

MarketEvent make_event(const SimulationConfig& config, std::uint64_t event_id) {
    const auto symbol_count = static_cast<std::uint64_t>(config.symbol_count);
    const auto symbol_id = static_cast<std::uint32_t>(event_id % symbol_count);
    const auto base_price = 100'000'000 + static_cast<std::int64_t>(symbol_id) * 1'000'000;
    auto event_type = event_id % 3 == 0 ? EventType::Trade : EventType::Quote;
    auto exchange_timestamp = config.seed * 1'000'000 + event_id * 100;
    auto receive_timestamp = exchange_timestamp + 25;

    if (config.chaos) {
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

    return MarketEvent{
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

}  // namespace

SimulationResult run_simulation(const SimulationConfig& config) {
    if (config.symbol_count == 0) {
        throw std::invalid_argument("symbol_count must be greater than zero");
    }
    if (config.producer_count == 0) {
        throw std::invalid_argument("producer_count must be greater than zero");
    }

    RingBuffer<MarketEvent> ring(config.capacity);
    std::atomic<std::uint64_t> generated{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<std::uint64_t> halt_events{0};
    std::atomic<std::uint64_t> timestamp_skews{0};
    std::atomic<std::uint64_t> out_of_order_events{0};
    std::atomic<std::uint64_t> burst_storms{0};
    std::atomic<bool> producers_done{false};
    std::vector<std::uint64_t> per_symbol_counts(config.symbol_count, 0);

    std::thread consumer([&] {
        MarketEvent event;
        while (!producers_done.load(std::memory_order_acquire) ||
               consumed.load(std::memory_order_relaxed) < accepted.load(std::memory_order_relaxed)) {
            if (!ring.try_pop(event)) {
                std::this_thread::yield();
                continue;
            }

            if (event.symbol_id < per_symbol_counts.size()) {
                ++per_symbol_counts[event.symbol_id];
            }
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(config.producer_count);
    for (std::size_t producer = 0; producer < config.producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::uint64_t event_id = producer; event_id < config.event_count;
                 event_id += config.producer_count) {
                auto event = make_event(config, event_id);

                generated.fetch_add(1, std::memory_order_relaxed);
                if (event.type == EventType::Halt || event.type == EventType::Resume) {
                    halt_events.fetch_add(1, std::memory_order_relaxed);
                }
                if (event.receive_timestamp_ns < event.exchange_timestamp_ns) {
                    timestamp_skews.fetch_add(1, std::memory_order_relaxed);
                }
                if (config.chaos && event_id % 13 == 0 && event_id > 0) {
                    out_of_order_events.fetch_add(1, std::memory_order_relaxed);
                }
                if (config.chaos && event_id % 16 == 0) {
                    burst_storms.fetch_add(1, std::memory_order_relaxed);
                }

                if (config.chaos) {
                    if (ring.try_push(event)) {
                        accepted.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    while (!ring.try_push(event)) {
                        std::this_thread::yield();
                    }
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    producers_done.store(true, std::memory_order_release);
    consumer.join();

    return SimulationResult{
        generated.load(std::memory_order_relaxed),
        accepted.load(std::memory_order_relaxed),
        consumed.load(std::memory_order_relaxed),
        halt_events.load(std::memory_order_relaxed),
        timestamp_skews.load(std::memory_order_relaxed),
        out_of_order_events.load(std::memory_order_relaxed),
        burst_storms.load(std::memory_order_relaxed),
        per_symbol_counts,
        ring.stats(),
    };
}

}  // namespace market_pulse
