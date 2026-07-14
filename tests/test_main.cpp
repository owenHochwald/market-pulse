#include "market_pulse/simulation.hpp"
#include "market_pulse/spsc_ring.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
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

template <typename Function>
void expect_invalid_argument(Function&& function, std::string_view message) {
    try {
        function();
    } catch (const std::invalid_argument&) {
        return;
    } catch (...) {
        expect(false, message);
        return;
    }
    expect(false, message);
}

void test_spsc_capacity_and_fifo() {
    expect_invalid_argument([] { market_pulse::SpscRing<int> invalid(3); },
                            "capacity must be a power of two");
    market_pulse::SpscRing<int> ring(4);
    int value = 99;
    expect(!ring.try_pop(value) && value == 99, "empty pop leaves output untouched");
    for (int item = 1; item <= 4; ++item) {
        expect(ring.try_push(item), "push within capacity succeeds");
    }
    expect(!ring.try_push(5), "full ring rejects a push");
    for (int item = 1; item <= 4; ++item) {
        expect(ring.try_pop(value) && value == item, "FIFO order is preserved");
    }
    const auto stats = ring.stats();
    expect(stats.pushed == 4 && stats.popped == 4, "push and pop stats are exact");
    expect(stats.full_observations == 1 && stats.dropped == 0,
           "full observation is not mislabeled as a drop");
    expect(stats.current_depth == 0 && stats.max_depth == 4, "depth stays bounded");
}

void test_spsc_bulk_wraparound() {
    market_pulse::SpscRing<int> ring(8);
    auto write = ring.reserve_write(6);
    expect(write.size == 6, "initial bulk reservation is contiguous");
    for (std::size_t index = 0; index < write.size; ++index) write.data[index] = static_cast<int>(index);
    ring.commit_write(write.size);

    auto read = ring.reserve_read(5);
    expect(read.size == 5, "bulk read returns requested items");
    ring.commit_read(read.size);

    write = ring.reserve_write(6);
    expect(write.size == 2, "write reservation stops at wrap boundary");
    write.data[0] = 6;
    write.data[1] = 7;
    ring.commit_write(2);
    write = ring.reserve_write(4);
    expect(write.size == 4, "next reservation continues after wrap");
    for (std::size_t index = 0; index < write.size; ++index) write.data[index] = static_cast<int>(8 + index);
    ring.commit_write(write.size);

    int value = 0;
    for (int expected = 5; expected <= 11; ++expected) {
        expect(ring.try_pop(value) && value == expected, "wrapped bulk data preserves FIFO order");
    }
    expect(ring.stats().max_depth <= ring.capacity(), "bulk depth cannot underflow or exceed capacity");
}

void test_spsc_concurrent_integrity() {
    constexpr std::uint64_t count = 250000;
    market_pulse::SpscRing<std::uint64_t> ring(1024);
    std::atomic<bool> producer_done{false};
    std::uint64_t consumed = 0;
    bool ordered = true;

    std::thread producer([&] {
        for (std::uint64_t value = 0; value < count; ++value) {
            while (!ring.try_push(value)) {
                ring.note_spin();
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });
    std::thread consumer([&] {
        std::uint64_t value = 0;
        while (!producer_done.load(std::memory_order_acquire) || consumed < count) {
            if (!ring.try_pop(value)) {
                std::this_thread::yield();
                continue;
            }
            ordered &= value == consumed;
            ++consumed;
        }
    });
    producer.join();
    consumer.join();

    expect(ordered && consumed == count, "concurrent lane delivers every item exactly once in order");
    const auto stats = ring.stats();
    expect(stats.pushed == count && stats.popped == count, "concurrent stats are exact");
    expect(stats.current_depth == 0 && stats.max_depth <= ring.capacity(), "concurrent depth remains valid");
}

market_pulse::SimulationConfig deterministic_config(std::size_t producers) {
    market_pulse::SimulationConfig config;
    config.symbol_count = 8;
    config.event_count = 100000;
    config.producer_count = producers;
    config.capacity = 1024;
    config.consumer_batch = 16;
    config.latency_sample_rate = 64;
    config.seed = 1234;
    return config;
}

void test_simulation_state_and_scaling_determinism() {
    const auto one = market_pulse::run_simulation(deterministic_config(1));
    const auto four = market_pulse::run_simulation(deterministic_config(4));
    expect(one.generated == 100000 && one.accepted == 100000 && one.consumed == 100000,
           "lossless replay processes every event");
    expect(four.generated == 100000 && four.accepted == 100000 && four.consumed == 100000,
           "sharded replay processes every event");
    expect(one.state_checksum == four.state_checksum, "state checksum is independent of lane interleaving");
    expect(one.books == four.books, "top-of-book state is deterministic across producer counts");
    expect(four.ring.dropped == 0, "lossless retry never records a drop");
    expect(four.ring.max_depth <= 1024, "aggregate maximum is a bounded lane high-water mark");
    expect(four.p99_latency_ns > 0, "sampled runtime handoff latency is measured");
}

void test_chaos_and_drop_metrics() {
    auto config = deterministic_config(4);
    config.event_count = 250000;
    config.capacity = 2;
    config.consumer_batch = 1;
    config.chaos = true;
    config.backpressure = market_pulse::BackpressurePolicy::Drop;
    const auto result = market_pulse::run_simulation(config);
    expect(result.generated == config.event_count, "drop mode attempts every event");
    expect(result.accepted == result.consumed, "consumer drains every accepted event");
    expect(result.ring.dropped == result.generated - result.accepted, "drop count represents abandoned events");
    expect(result.ring.dropped > 0, "saturated drop mode produces actual loss");
    expect(result.halt_events > 0 && result.timestamp_skews > 0 && result.out_of_order_events > 0,
           "chaos anomalies are counted");
    expect(result.ring.max_depth <= config.capacity, "saturated depth remains bounded");
}

void test_benchmark_and_formatters() {
    market_pulse::BenchmarkConfig config;
    config.simulation = deterministic_config(2);
    config.simulation.event_count = 20000;
    config.warmup_events = 1000;
    config.repeats = 2;
    config.profile = market_pulse::BenchmarkProfile::Balanced;
    const auto result = market_pulse::run_benchmark(config);
    expect(result.runs.size() == 2 && result.median.events_per_second > 0,
           "benchmark records repeats and selects a median");

    const auto summary = market_pulse::format_benchmark_summary(config, result);
    const auto json = market_pulse::format_benchmark_json(config, result);
    const auto html = market_pulse::format_html_report(config.simulation, result.median);
    expect(summary.find("profile=balanced") != std::string::npos, "summary includes benchmark profile");
    expect(summary.find("p999_handoff_ns=") != std::string::npos, "summary includes measured tail latency");
    expect(json.find("\"events_per_second\"") != std::string::npos, "JSON includes throughput");
    expect(json.find("\"state_checksum\"") != std::string::npos, "JSON includes deterministic checksum");
    expect(html.find("Measured Handoff Latency") != std::string::npos, "HTML labels runtime latency honestly");
    expect(html.find("Test Suite Coverage") != std::string::npos, "HTML describes verification");
}

}  // namespace

int main() {
    test_spsc_capacity_and_fifo();
    test_spsc_bulk_wraparound();
    test_spsc_concurrent_integrity();
    test_simulation_state_and_scaling_determinism();
    test_chaos_and_drop_metrics();
    test_benchmark_and_formatters();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "all tests passed\n";
    return EXIT_SUCCESS;
}
