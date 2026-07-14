#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace market_pulse {

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield");
#else
    std::this_thread::yield();
#endif
}

class MonotonicClock {
public:
    MonotonicClock();

    [[nodiscard]] std::uint64_t producer_timestamp() const noexcept;
    [[nodiscard]] std::uint64_t consumer_timestamp() const noexcept;
    [[nodiscard]] std::uint64_t to_nanoseconds(std::uint64_t delta) const noexcept;
    [[nodiscard]] bool uses_invariant_tsc() const noexcept { return use_tsc_; }

private:
    bool use_tsc_ = false;
    double nanoseconds_per_tick_ = 1.0;
};

[[nodiscard]] std::vector<int> available_cpu_ids();
[[nodiscard]] bool pin_current_thread(int cpu_id) noexcept;
[[nodiscard]] bool advise_huge_pages(void* address, std::size_t bytes) noexcept;

}  // namespace market_pulse
