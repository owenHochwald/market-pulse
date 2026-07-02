#include "market_pulse/simulation.hpp"

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>

namespace {

void print_usage() {
    std::cerr << "usage: market-pulse simulate [--symbols N] [--events N] [--producers N]"
              << " [--capacity N] [--seed N] [--chaos]\n";
}

bool read_size_arg(int argc, char** argv, int& index, std::size_t& out) {
    if (index + 1 >= argc) {
        return false;
    }
    out = static_cast<std::size_t>(std::stoull(argv[++index]));
    return true;
}

bool read_u64_arg(int argc, char** argv, int& index, std::uint64_t& out) {
    if (index + 1 >= argc) {
        return false;
    }
    out = static_cast<std::uint64_t>(std::stoull(argv[++index]));
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2 || std::string_view(argv[1]) != "simulate") {
        print_usage();
        return EXIT_FAILURE;
    }

    market_pulse::SimulationConfig config;
    try {
        for (int i = 2; i < argc; ++i) {
            const std::string_view arg(argv[i]);
            if (arg == "--symbols" && read_size_arg(argc, argv, i, config.symbol_count)) {
                continue;
            }
            if (arg == "--events" && read_size_arg(argc, argv, i, config.event_count)) {
                continue;
            }
            if (arg == "--producers" && read_size_arg(argc, argv, i, config.producer_count)) {
                continue;
            }
            if (arg == "--capacity" && read_size_arg(argc, argv, i, config.capacity)) {
                continue;
            }
            if (arg == "--seed" && read_u64_arg(argc, argv, i, config.seed)) {
                continue;
            }
            if (arg == "--chaos") {
                config.chaos = true;
                continue;
            }

            print_usage();
            return EXIT_FAILURE;
        }

        const auto result = market_pulse::run_simulation(config);
        std::cout << market_pulse::format_simulation_summary(config, result);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
