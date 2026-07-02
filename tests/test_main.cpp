#include "market_pulse/simulation.hpp"
#include "market_pulse/ring_buffer.hpp"

#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>

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

}  // namespace

int main() {
    test_smoke();
    test_ring_buffer_empty_and_capacity();
    test_ring_buffer_wraparound_ordering();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return EXIT_FAILURE;
    }

    std::cout << "all tests passed\n";
    return EXIT_SUCCESS;
}
