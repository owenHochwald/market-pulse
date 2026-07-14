#pragma once

#include "market_pulse/platform.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace market_pulse {

inline constexpr std::size_t cache_line_size = 64;

struct LaneStats {
    std::uint64_t pushed = 0;
    std::uint64_t popped = 0;
    std::uint64_t dropped = 0;
    std::uint64_t full_observations = 0;
    std::uint64_t spin_iterations = 0;
    std::uint64_t current_depth = 0;
    std::uint64_t max_depth = 0;
};

template <typename T>
class SpscRing {
    static_assert(std::is_nothrow_destructible_v<T>);
    static_assert(std::is_trivially_copyable_v<T>, "SPSC storage is optimized for trivial wire values");
    static_assert(std::is_trivially_destructible_v<T>);

public:
    struct Span {
        T* data = nullptr;
        std::size_t size = 0;

        explicit operator bool() const noexcept { return size != 0; }
    };

    explicit SpscRing(std::size_t capacity)
        : capacity_(checked_capacity(capacity)), mask_(capacity_ - 1),
          storage_(allocate_storage(capacity_)) {}

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    [[nodiscard]] Span reserve_write(std::size_t requested) noexcept {
        const auto used = producer_.position - producer_.cached_read;
        if (used == capacity_) {
            producer_.cached_read = consumer_.published.load(std::memory_order_acquire);
            if (producer_.position - producer_.cached_read == capacity_) {
                ++producer_.full_observations;
                producer_.observed_full = true;
                return {};
            }
        }

        const auto available = static_cast<std::size_t>(capacity_ - (producer_.position - producer_.cached_read));
        const auto index = static_cast<std::size_t>(producer_.position & mask_);
        const auto contiguous = static_cast<std::size_t>(capacity_) - index;
        return {storage_.get() + index, std::min({requested, available, contiguous})};
    }

    void commit_write(std::size_t count) noexcept {
        producer_.position += count;
        producer_.pushed += count;
        producer_.published.store(producer_.position, std::memory_order_release);
    }

    [[nodiscard]] Span reserve_read(std::size_t requested) noexcept {
        if (consumer_.position == consumer_.cached_write) {
            consumer_.cached_write = producer_.published.load(std::memory_order_acquire);
            if (consumer_.position == consumer_.cached_write) {
                return {};
            }
            consumer_.max_depth = std::max(consumer_.max_depth,
                                           consumer_.cached_write - consumer_.position);
        }

        const auto available = static_cast<std::size_t>(consumer_.cached_write - consumer_.position);
        const auto index = static_cast<std::size_t>(consumer_.position & mask_);
        const auto contiguous = static_cast<std::size_t>(capacity_) - index;
        return {storage_.get() + index, std::min({requested, available, contiguous})};
    }

    void commit_read(std::size_t count) noexcept {
        consumer_.position += count;
        consumer_.popped += count;
        consumer_.published.store(consumer_.position, std::memory_order_release);
    }

    bool try_push(const T& value) noexcept(std::is_nothrow_copy_assignable_v<T>) {
        auto span = reserve_write(1);
        if (!span) {
            return false;
        }
        span.data[0] = value;
        commit_write(1);
        return true;
    }

    bool try_pop(T& value) noexcept(std::is_nothrow_copy_assignable_v<T>) {
        auto span = reserve_read(1);
        if (!span) {
            return false;
        }
        value = span.data[0];
        commit_read(1);
        return true;
    }

    void note_spin() noexcept { ++producer_.spin_iterations; }
    void note_drop() noexcept { ++producer_.dropped; }

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

    [[nodiscard]] bool advise_huge_pages() noexcept {
        return market_pulse::advise_huge_pages(storage_.get(), sizeof(T) * capacity_);
    }

    [[nodiscard]] LaneStats stats() const noexcept {
        const auto write = producer_.published.load(std::memory_order_relaxed);
        const auto read = consumer_.published.load(std::memory_order_relaxed);
        return {
            producer_.pushed,
            consumer_.popped,
            producer_.dropped,
            producer_.full_observations,
            producer_.spin_iterations,
            write - read,
            producer_.observed_full ? capacity_ : consumer_.max_depth,
        };
    }

private:
    static constexpr bool is_power_of_two(std::size_t value) noexcept {
        return value != 0 && (value & (value - 1)) == 0;
    }

    static std::size_t checked_capacity(std::size_t capacity) {
        if (capacity < 2 || !is_power_of_two(capacity) ||
            capacity > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
            throw std::invalid_argument("SPSC capacity must be a representable power of two and at least 2");
        }
        return capacity;
    }

    struct AlignedDelete {
        void operator()(T* pointer) const noexcept {
            ::operator delete[](pointer, std::align_val_t{cache_line_size});
        }
    };

    static T* allocate_storage(std::size_t capacity) {
        return static_cast<T*>(::operator new[](sizeof(T) * capacity,
                                                std::align_val_t{cache_line_size}));
    }

    struct alignas(cache_line_size) ProducerState {
        std::atomic<std::uint64_t> published{0};
        std::uint64_t position = 0;
        std::uint64_t cached_read = 0;
        std::uint64_t pushed = 0;
        std::uint64_t dropped = 0;
        std::uint64_t full_observations = 0;
        std::uint64_t spin_iterations = 0;
        bool observed_full = false;
    };

    struct alignas(cache_line_size) ConsumerState {
        std::atomic<std::uint64_t> published{0};
        std::uint64_t position = 0;
        std::uint64_t cached_write = 0;
        std::uint64_t popped = 0;
        std::uint64_t max_depth = 0;
    };

    const std::uint64_t capacity_;
    const std::uint64_t mask_;
    std::unique_ptr<T[], AlignedDelete> storage_;
    ProducerState producer_;
    ConsumerState consumer_;
};

}  // namespace market_pulse
