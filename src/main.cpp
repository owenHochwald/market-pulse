#include "market_pulse/simulation.hpp"

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

void print_help(std::ostream& output) {
    output << "market-pulse\n\n"
           << "usage:\n"
           << "  market-pulse simulate [options]\n"
           << "  market-pulse benchmark [options]\n\n"
           << "common options:\n"
           << "  --symbols N          symbols (simulate: 8, benchmark: 128)\n"
           << "  --events N           generated events (simulate: 10000, benchmark: 5000000)\n"
           << "  --producers N        SPSC producer lanes\n"
           << "  --capacity N         capacity of each lane; power of two\n"
           << "  --batch N            maximum consumer batch\n"
           << "  --sample-rate N      sample every Nth handoff; 0 disables\n"
           << "  --seed N             deterministic replay seed\n"
           << "  --chaos              inject exchange anomalies\n"
           << "  --backpressure MODE  spin or drop\n"
           << "  --pin                 pin workers on supported Linux systems\n"
           << "  --cpus LIST          consumer CPU followed by producer CPUs\n\n"
           << "  --huge-pages         request transparent huge pages on Linux\n\n"
           << "benchmark options:\n"
           << "  --profile MODE       throughput, latency, or balanced\n"
           << "  --warmup N           warmup event count\n"
           << "  --repeats N          measured repetitions\n"
           << "  --json PATH          write median metrics as JSON\n\n"
           << "simulate report options:\n"
           << "  --report PATH        write HTML report (default: market-pulse-report.html)\n"
           << "  --no-report          skip HTML report generation\n";
}

template <typename T>
bool read_integer(int argc, char** argv, int& index, T& output) {
    if (index + 1 >= argc) {
        return false;
    }
    output = static_cast<T>(std::stoull(argv[++index]));
    return true;
}

std::vector<int> parse_cpu_list(std::string_view input) {
    std::vector<int> cpus;
    std::stringstream stream{std::string(input)};
    std::string item;
    while (std::getline(stream, item, ',')) {
        if (item.empty()) {
            throw std::invalid_argument("CPU list contains an empty entry");
        }
        const int cpu = std::stoi(item);
        if (cpu < 0) {
            throw std::invalid_argument("CPU identifiers must be non-negative");
        }
        cpus.push_back(cpu);
    }
    return cpus;
}

void write_text_file(const std::filesystem::path& path, std::string_view contents) {
    const auto absolute = std::filesystem::absolute(path);
    if (absolute.has_parent_path()) {
        std::filesystem::create_directories(absolute.parent_path());
    }
    std::ofstream output(absolute);
    if (!output) {
        throw std::runtime_error("could not open output file");
    }
    output << contents;
    if (!output) {
        throw std::runtime_error("could not write output file");
    }
}

std::string path_to_file_url(const std::filesystem::path& path) {
    std::string result = "file://";
    for (const char character : std::filesystem::absolute(path).string()) {
        result += character == ' ' ? "%20" : std::string(1, character);
    }
    return result;
}

bool parse_common_option(std::string_view argument, int argc, char** argv, int& index,
                         market_pulse::SimulationConfig& config) {
    if (argument == "--symbols") return read_integer(argc, argv, index, config.symbol_count);
    if (argument == "--events") return read_integer(argc, argv, index, config.event_count);
    if (argument == "--producers") return read_integer(argc, argv, index, config.producer_count);
    if (argument == "--capacity") return read_integer(argc, argv, index, config.capacity);
    if (argument == "--batch") return read_integer(argc, argv, index, config.consumer_batch);
    if (argument == "--sample-rate") return read_integer(argc, argv, index, config.latency_sample_rate);
    if (argument == "--seed") return read_integer(argc, argv, index, config.seed);
    if (argument == "--chaos") {
        config.chaos = true;
        return true;
    }
    if (argument == "--pin") {
        config.pin_threads = true;
        return true;
    }
    if (argument == "--huge-pages") {
        config.huge_pages = true;
        return true;
    }
    if (argument == "--cpus" && index + 1 < argc) {
        config.cpu_ids = parse_cpu_list(argv[++index]);
        config.pin_threads = true;
        return true;
    }
    if (argument == "--backpressure" && index + 1 < argc) {
        const std::string_view policy(argv[++index]);
        if (policy == "spin") config.backpressure = market_pulse::BackpressurePolicy::Spin;
        else if (policy == "drop") config.backpressure = market_pulse::BackpressurePolicy::Drop;
        else throw std::invalid_argument("backpressure must be spin or drop");
        return true;
    }
    return false;
}

int run_simulate(int argc, char** argv) {
    market_pulse::SimulationConfig config;
    bool write_report = true;
    std::filesystem::path report_path = "market-pulse-report.html";

    for (int index = 2; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            print_help(std::cout);
            return EXIT_SUCCESS;
        }
        if (argument == "--report" && index + 1 < argc) {
            report_path = argv[++index];
            write_report = true;
            continue;
        }
        if (argument == "--no-report") {
            write_report = false;
            continue;
        }
        if (!parse_common_option(argument, argc, argv, index, config)) {
            throw std::invalid_argument("unknown or incomplete simulate option: " + std::string(argument));
        }
    }

    const auto result = market_pulse::run_simulation(config);
    std::cout << market_pulse::format_simulation_summary(config, result);
    if (write_report) {
        write_text_file(report_path, market_pulse::format_html_report(config, result));
        std::cout << "html_report=" << std::filesystem::absolute(report_path) << '\n'
                  << "html_report_url=" << path_to_file_url(report_path) << '\n';
    }
    return EXIT_SUCCESS;
}

int run_benchmark(int argc, char** argv) {
    market_pulse::BenchmarkConfig config;
    config.simulation.symbol_count = 128;
    config.simulation.event_count = 5'000'000;
    config.simulation.producer_count = 4;
    config.simulation.pin_threads = true;
    std::filesystem::path json_path;

    for (int index = 2; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--help" || argument == "-h") {
            print_help(std::cout);
            return EXIT_SUCCESS;
        }
        if (argument == "--warmup" && read_integer(argc, argv, index, config.warmup_events)) continue;
        if (argument == "--repeats" && read_integer(argc, argv, index, config.repeats)) continue;
        if (argument == "--json" && index + 1 < argc) {
            json_path = argv[++index];
            continue;
        }
        if (argument == "--profile" && index + 1 < argc) {
            const std::string_view profile(argv[++index]);
            if (profile == "throughput") config.profile = market_pulse::BenchmarkProfile::Throughput;
            else if (profile == "latency") config.profile = market_pulse::BenchmarkProfile::Latency;
            else if (profile == "balanced") config.profile = market_pulse::BenchmarkProfile::Balanced;
            else throw std::invalid_argument("profile must be throughput, latency, or balanced");
            continue;
        }
        if (argument == "--capacity") config.override_capacity = true;
        if (argument == "--batch") config.override_batch = true;
        if (argument == "--sample-rate") config.override_sample_rate = true;
        if (!parse_common_option(argument, argc, argv, index, config.simulation)) {
            throw std::invalid_argument("unknown or incomplete benchmark option: " + std::string(argument));
        }
    }

    const auto result = market_pulse::run_benchmark(config);
    std::cout << market_pulse::format_benchmark_summary(config, result);
    if (!json_path.empty()) {
        write_text_file(json_path, market_pulse::format_benchmark_json(config, result));
        std::cout << "json_report=" << std::filesystem::absolute(json_path) << '\n';
    }
    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_help(std::cerr);
        return EXIT_FAILURE;
    }
    try {
        const std::string_view command(argv[1]);
        if (command == "--help" || command == "-h") {
            print_help(std::cout);
            return EXIT_SUCCESS;
        }
        if (command == "simulate") return run_simulate(argc, argv);
        if (command == "benchmark") return run_benchmark(argc, argv);
        throw std::invalid_argument("unknown command: " + std::string(command));
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
