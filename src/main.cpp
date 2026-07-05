#include "market_pulse/simulation.hpp"

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

void print_help(std::ostream& output) {
    output << "market-pulse\n\n"
           << "usage:\n"
           << "  market-pulse --help\n"
           << "  market-pulse simulate [options]\n\n"
           << "simulate options:\n"
           << "  --symbols N      number of symbols to replay (default: 8)\n"
           << "  --events N       number of market events to generate (default: 10000)\n"
           << "  --producers N    number of producer threads (default: 1)\n"
           << "  --capacity N     ring-buffer capacity; power of two, at least 2 (default: 1024)\n"
           << "  --seed N         deterministic replay seed (default: 1)\n"
           << "  --chaos          inject halts, timestamp skew, burst storms, and out-of-order events\n"
           << "  --report PATH    write HTML report to PATH (default: market-pulse-report.html)\n"
           << "  --no-report      skip HTML report generation\n"
           << "  --help           show this help text\n";
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

std::string path_to_file_url(const std::filesystem::path& path) {
    std::string input = path.string();
    std::string output = "file://";
    for (const char character : input) {
        if (character == ' ') {
            output += "%20";
        } else {
            output += character;
        }
    }
    return output;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_help(std::cerr);
        return EXIT_FAILURE;
    }

    const std::string_view command(argv[1]);
    if (command == "--help" || command == "-h") {
        print_help(std::cout);
        return EXIT_SUCCESS;
    }
    if (command != "simulate") {
        print_help(std::cerr);
        return EXIT_FAILURE;
    }

    market_pulse::SimulationConfig config;
    bool write_report = true;
    std::filesystem::path report_path = "market-pulse-report.html";

    try {
        for (int i = 2; i < argc; ++i) {
            const std::string_view arg(argv[i]);
            if (arg == "--help" || arg == "-h") {
                print_help(std::cout);
                return EXIT_SUCCESS;
            }
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
            if (arg == "--report") {
                if (i + 1 >= argc) {
                    print_help(std::cerr);
                    return EXIT_FAILURE;
                }
                report_path = argv[++i];
                write_report = true;
                continue;
            }
            if (arg == "--no-report") {
                write_report = false;
                continue;
            }

            print_help(std::cerr);
            return EXIT_FAILURE;
        }

        const auto result = market_pulse::run_simulation(config);
        std::cout << market_pulse::format_simulation_summary(config, result);
        if (write_report) {
            const auto absolute_report_path = std::filesystem::absolute(report_path);
            if (absolute_report_path.has_parent_path()) {
                std::filesystem::create_directories(absolute_report_path.parent_path());
            }

            std::ofstream report(absolute_report_path);
            if (!report) {
                throw std::runtime_error("could not open HTML report for writing");
            }
            report << market_pulse::format_html_report(config, result);
            report.close();
            if (!report) {
                throw std::runtime_error("could not write HTML report");
            }

            std::cout << "html_report=" << absolute_report_path << '\n'
                      << "html_report_url=" << path_to_file_url(absolute_report_path) << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
