#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

namespace market_pulse {

struct RingStats {
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
    std::uint64_t dropped = 0;
    std::uint64_t max_depth = 0;
};

template <typename T>
class RingBuffer {
public:
    explicit RingBuffer(std::size_t capacity)
        : capacity_(round_up_power_of_two(capacity)),
          mask_(capacity_ - 1),
          slots_(std::make_unique<Slot[]>(capacity_)) {
        if (capacity < 2) {
            throw std::invalid_argument("ring buffer capacity must be at least 2");
        }

        for (std::size_t i = 0; i < capacity_; ++i) {
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    bool try_push(const T& value) {
        std::size_t position = write_position_.load(std::memory_order_relaxed);

        for (;;) {
            Slot& slot = slots_[position & mask_];
            const std::size_t sequence = slot.sequence.load(std::memory_order_acquire);
            const auto difference = static_cast<std::intptr_t>(sequence) -
                                    static_cast<std::intptr_t>(position);

            if (difference == 0) {
                if (write_position_.compare_exchange_weak(
                        position, position + 1, std::memory_order_relaxed)) {
                    slot.value = value;
                    slot.sequence.store(position + 1, std::memory_order_release);
                    pushed_.fetch_add(1, std::memory_order_relaxed);
                    update_max_depth(position + 1);
                    return true;
                }
            } else if (difference < 0) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            } else {
                position = write_position_.load(std::memory_order_relaxed);
            }
        }
    }

    bool try_pop(T& out) {
        const std::size_t position = read_position_.load(std::memory_order_relaxed);
        Slot& slot = slots_[position & mask_];
        const std::size_t sequence = slot.sequence.load(std::memory_order_acquire);
        const auto difference = static_cast<std::intptr_t>(sequence) -
                                static_cast<std::intptr_t>(position + 1);

        if (difference == 0) {
            out = slot.value;
            slot.sequence.store(position + capacity_, std::memory_order_release);
            read_position_.store(position + 1, std::memory_order_relaxed);
            popped_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        return false;
    }

    [[nodiscard]] std::size_t capacity() const {
        return capacity_;
    }

    [[nodiscard]] RingStats stats() const {
        return RingStats{
            pushed_.load(std::memory_order_relaxed),
            popped_.load(std::memory_order_relaxed),
            dropped_.load(std::memory_order_relaxed),
            max_depth_.load(std::memory_order_relaxed),
        };
    }

private:
    struct Slot {
        std::atomic<std::size_t> sequence{0};
        T value{};
    };

    static std::size_t round_up_power_of_two(std::size_t value) {
        std::size_t capacity = 1;
        while (capacity < value) {
            capacity <<= 1;
        }
        return capacity;
    }

    void update_max_depth(std::size_t observed_write_position) {
        const std::size_t read_position = read_position_.load(std::memory_order_relaxed);
        const auto depth = static_cast<std::uint64_t>(observed_write_position - read_position);
        auto current = max_depth_.load(std::memory_order_relaxed);
        while (depth > current &&
               !max_depth_.compare_exchange_weak(current, depth, std::memory_order_relaxed)) {
        }
    }

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<Slot[]> slots_;

    alignas(64) std::atomic<std::size_t> write_position_{0};
    alignas(64) std::atomic<std::size_t> read_position_{0};
    alignas(64) std::atomic<std::uint64_t> pushed_{0};
    std::atomic<std::uint64_t> popped_{0};
    std::atomic<std::uint64_t> dropped_{0};
    std::atomic<std::uint64_t> max_depth_{0};
};

}  // namespace market_pulse
