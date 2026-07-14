#include "market_pulse/platform.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <set>
#include <string>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <cpuid.h>
#include <x86intrin.h>
#endif

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace market_pulse {
namespace {

std::uint64_t steady_nanoseconds() noexcept {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

#if defined(__x86_64__) || defined(_M_X64)
bool has_invariant_tsc() noexcept {
    unsigned int maximum = __get_cpuid_max(0x80000000, nullptr);
    if (maximum < 0x80000007) {
        return false;
    }
    unsigned int eax = 0;
    unsigned int ebx = 0;
    unsigned int ecx = 0;
    unsigned int edx = 0;
    __get_cpuid(0x80000001, &eax, &ebx, &ecx, &edx);
    const bool has_rdtscp = (edx & (1U << 27U)) != 0;
    __get_cpuid(0x80000007, &eax, &ebx, &ecx, &edx);
    return has_rdtscp && (edx & (1U << 8U)) != 0;
}

std::uint64_t tsc_start() noexcept {
    _mm_lfence();
    const auto value = __rdtsc();
    _mm_lfence();
    return value;
}

std::uint64_t tsc_end() noexcept {
    unsigned int auxiliary = 0;
    const auto value = __rdtscp(&auxiliary);
    _mm_lfence();
    return value;
}
#endif

}  // namespace

MonotonicClock::MonotonicClock() {
#if defined(__x86_64__) || defined(_M_X64)
    if (has_invariant_tsc()) {
        const auto start_ns = steady_nanoseconds();
        const auto start_ticks = tsc_start();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        const auto end_ticks = tsc_end();
        const auto end_ns = steady_nanoseconds();
        if (end_ticks > start_ticks && end_ns > start_ns) {
            nanoseconds_per_tick_ = static_cast<double>(end_ns - start_ns) /
                                    static_cast<double>(end_ticks - start_ticks);
            use_tsc_ = true;
        }
    }
#endif
}

std::uint64_t MonotonicClock::producer_timestamp() const noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    if (use_tsc_) {
        return tsc_start();
    }
#endif
    return steady_nanoseconds();
}

std::uint64_t MonotonicClock::consumer_timestamp() const noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    if (use_tsc_) {
        return tsc_end();
    }
#endif
    return steady_nanoseconds();
}

std::uint64_t MonotonicClock::to_nanoseconds(std::uint64_t delta) const noexcept {
    return static_cast<std::uint64_t>(static_cast<double>(delta) * nanoseconds_per_tick_);
}

std::vector<int> available_cpu_ids() {
    std::vector<int> result;
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (CPU_ISSET(cpu, &set)) {
                result.push_back(cpu);
            }
        }
    }
    std::vector<int> physical_first;
    std::vector<int> siblings;
    std::set<std::pair<int, int>> seen_cores;
    for (int cpu : result) {
        const auto topology = "/sys/devices/system/cpu/cpu" + std::to_string(cpu) + "/topology/";
        std::ifstream package_file(topology + "physical_package_id");
        std::ifstream core_file(topology + "core_id");
        int package = 0;
        int core = cpu;
        if (package_file >> package && core_file >> core && seen_cores.emplace(package, core).second) {
            physical_first.push_back(cpu);
        } else {
            siblings.push_back(cpu);
        }
    }
    if (!physical_first.empty()) {
        physical_first.insert(physical_first.end(), siblings.begin(), siblings.end());
        result = std::move(physical_first);
    }
#endif
    if (result.empty()) {
        const auto count = std::max(1U, std::thread::hardware_concurrency());
        for (unsigned int cpu = 0; cpu < count; ++cpu) {
            result.push_back(static_cast<int>(cpu));
        }
    }
    return result;
}

bool advise_huge_pages(void* address, std::size_t bytes) noexcept {
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    const auto system_page_size = sysconf(_SC_PAGESIZE);
    if (address == nullptr || system_page_size <= 0) {
        return false;
    }
    const auto page_size = static_cast<std::uintptr_t>(system_page_size);
    if (bytes < page_size) return false;
    const auto begin = reinterpret_cast<std::uintptr_t>(address);
    const auto aligned_begin = (begin + page_size - 1) & ~(page_size - 1);
    const auto end = (begin + bytes) & ~(page_size - 1);
    return end > aligned_begin && madvise(reinterpret_cast<void*>(aligned_begin), end - aligned_begin,
                                         MADV_HUGEPAGE) == 0;
#else
    (void)address;
    (void)bytes;
    return false;
#endif
}

bool pin_current_thread(int cpu_id) noexcept {
#if defined(__linux__)
    if (cpu_id < 0 || cpu_id >= CPU_SETSIZE) {
        return false;
    }
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_id, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)cpu_id;
    return false;
#endif
}

}  // namespace market_pulse
