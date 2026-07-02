#include "market_pulse/simulation.hpp"
#include "market_pulse/ring_buffer.hpp"

#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

namespace {

int failures = 0;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <typename Func>
void expect_invalid_argument(Func&& func, std::string_view message) {
    try {
        func();
    } catch (const std::invalid_argument&) {
        return;
    } catch (...) {
        ++failures;
        std::cerr << "FAIL: " << message << " threw the wrong exception\n";
        return;
    }

    ++failures;
    std::cerr << "FAIL: " << message << '\n';
}

void test_smoke() {
    expect(market_pulse::smoke_value() == 42, "smoke test returns expected value");
}

void test_ring_buffer_empty_and_capacity() {
    market_pulse::RingBuffer<int> buffer(2);

    int value = 99;
    expect(!buffer.try_pop(value), "new buffer is empty");
    expect(value == 99, "failed pop does not overwrite output");

    expect(buffer.try_push(10), "first push succeeds");
    expect(buffer.try_push(20), "second push succeeds");
    expect(!buffer.try_push(30), "push fails when buffer is full");

    expect(buffer.try_pop(value), "first pop succeeds");
    expect(value == 10, "first pop returns first value");
    expect(buffer.try_pop(value), "second pop succeeds");
    expect(value == 20, "second pop returns second value");
    expect(!buffer.try_pop(value), "buffer is empty after popping all values");
}

void test_ring_buffer_wraparound_ordering() {
    expect_invalid_argument(
        [] {
            market_pulse::RingBuffer<int> invalid(3);
        },
        "capacity must be an explicit power of two");

    market_pulse::RingBuffer<int> buffer(4);
    int value = 0;

    expect(buffer.try_push(1), "push 1 succeeds");
    expect(buffer.try_push(2), "push 2 succeeds");
    expect(buffer.try_push(3), "push 3 succeeds");
    expect(buffer.try_push(4), "push 4 succeeds");
    expect(buffer.try_pop(value) && value == 1, "pop 1 before wraparound");
    expect(buffer.try_pop(value) && value == 2, "pop 2 before wraparound");
    expect(buffer.try_push(5), "push 5 reuses first freed slot");
    expect(buffer.try_push(6), "push 6 reuses second freed slot");

    for (int expected = 3; expected <= 6; ++expected) {
        expect(buffer.try_pop(value), "pop succeeds after wraparound");
        expect(value == expected, "wraparound preserves FIFO order");
    }
    expect(!buffer.try_pop(value), "wrapped buffer is empty after draining");
}

void test_ring_buffer_stats_accounting() {
    market_pulse::RingBuffer<int> buffer(2);
    int value = 0;

    expect(buffer.try_push(7), "stats test first push succeeds");
    expect(buffer.try_push(8), "stats test second push succeeds");
    expect(!buffer.try_push(9), "stats test drop recorded when full");
    expect(buffer.try_pop(value), "stats test pop succeeds");

    const auto stats = buffer.stats();
    expect(stats.pushed == 2, "stats count accepted pushes");
    expect(stats.popped == 1, "stats count successful pops");
    expect(stats.dropped == 1, "stats count rejected pushes");
    expect(stats.current_depth == 1, "stats report current backlog");
    expect(stats.max_depth == 2, "stats report peak backlog");
}

struct ProducerEvent {
    int producer = 0;
    int sequence = 0;
};

void test_ring_buffer_multi_producer_integrity() {
    constexpr int producer_count = 4;
    constexpr int events_per_producer = 512;
    constexpr int total_events = producer_count * events_per_producer;

    market_pulse::RingBuffer<ProducerEvent> buffer(4096);
    std::vector<std::thread> producers;
    std::vector<int> seen(total_events, 0);
    std::atomic<int> consumed{0};

    std::thread consumer([&] {
        ProducerEvent event;
        while (consumed.load(std::memory_order_relaxed) < total_events) {
            if (!buffer.try_pop(event)) {
                std::this_thread::yield();
                continue;
            }

            const int index = event.producer * events_per_producer + event.sequence;
            if (index >= 0 && index < total_events) {
                ++seen[static_cast<std::size_t>(index)];
            }
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    for (int producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (int sequence = 0; sequence < events_per_producer; ++sequence) {
                ProducerEvent event{producer, sequence};
                while (!buffer.try_push(event)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    consumer.join();

    for (int count : seen) {
        expect(count == 1, "multi-producer event delivered exactly once");
    }

    const auto stats = buffer.stats();
    expect(stats.pushed == total_events, "multi-producer stats count all pushes");
    expect(stats.popped == total_events, "multi-producer stats count all pops");
    expect(stats.dropped == 0, "multi-producer retry loop avoids drops");
    expect(stats.producer_retries >= 0, "multi-producer stats expose producer CAS retries");
}

void test_market_simulation_determinism() {
    market_pulse::SimulationConfig config;
    config.symbol_count = 3;
    config.event_count = 9;
    config.producer_count = 1;
    config.capacity = 16;
    config.seed = 1234;

    const auto result = market_pulse::run_simulation(config);

    expect(result.generated == 9, "simulation reports generated events");
    expect(result.accepted == 9, "simulation accepts all deterministic events");
    expect(result.consumed == 9, "simulation consumes all accepted deterministic events");
    expect(result.ring.dropped == 0, "simulation has no drops with sufficient capacity");
    expect(result.per_symbol_counts.size() == 3, "simulation reports each symbol");
    if (result.per_symbol_counts.size() == 3) {
        expect(result.per_symbol_counts[0] == 3, "symbol 0 count is deterministic");
        expect(result.per_symbol_counts[1] == 3, "symbol 1 count is deterministic");
        expect(result.per_symbol_counts[2] == 3, "symbol 2 count is deterministic");
    }
}

void test_chaos_mode_metrics() {
    market_pulse::SimulationConfig config;
    config.symbol_count = 2;
    config.event_count = 48;
    config.producer_count = 1;
    config.capacity = 8;
    config.seed = 99;
    config.chaos = true;

    const auto result = market_pulse::run_simulation(config);

    expect(result.generated == 48, "chaos mode reports attempted events");
    expect(result.accepted == result.consumed, "chaos mode consumes every accepted event");
    expect(result.halt_events > 0, "chaos mode injects halt events");
    expect(result.timestamp_skews > 0, "chaos mode injects timestamp skew");
    expect(result.out_of_order_events > 0, "chaos mode injects out-of-order events");
    expect(result.burst_storms > 0, "chaos mode reports burst storms");
}

}  // namespace

int main() {
    test_smoke();
    test_ring_buffer_empty_and_capacity();
    test_ring_buffer_wraparound_ordering();
    test_ring_buffer_stats_accounting();
    test_ring_buffer_multi_producer_integrity();
    test_market_simulation_determinism();
    test_chaos_mode_metrics();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return EXIT_FAILURE;
    }

    std::cout << "all tests passed\n";
    return EXIT_SUCCESS;
}
