#include "market_pulse/simulation.hpp"
#include "market_pulse/ring_buffer.hpp"

#include <cstdlib>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

int failures = 0;

void expect(bool condition, std::string_view message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
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

}  // namespace

int main() {
    test_smoke();
    test_ring_buffer_empty_and_capacity();
    if (failures != 0) {
        std::cerr << failures << " test(s) failed\n";
        return EXIT_FAILURE;
    }

    std::cout << "all tests passed\n";
    return EXIT_SUCCESS;
}
