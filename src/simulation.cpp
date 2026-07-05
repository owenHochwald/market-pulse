#include "market_pulse/simulation.hpp"

#include <atomic>
#include <algorithm>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace market_pulse {

int smoke_value() {
    return 42;
}

namespace {

MarketEvent make_event(const SimulationConfig& config, std::uint64_t event_id) {
    const auto symbol_count = static_cast<std::uint64_t>(config.symbol_count);
    const auto symbol_id = static_cast<std::uint32_t>(event_id % symbol_count);
    const auto base_price = 100'000'000 + static_cast<std::int64_t>(symbol_id) * 1'000'000;
    auto event_type = event_id % 3 == 0 ? EventType::Trade : EventType::Quote;
    auto exchange_timestamp = config.seed * 1'000'000 + event_id * 100;
    auto receive_timestamp = exchange_timestamp + 25;

    if (config.chaos) {
        if (event_id % 17 == 0) {
            event_type = EventType::Halt;
        } else if (event_id % 17 == 4) {
            event_type = EventType::Resume;
        } else if (event_id % 11 == 0) {
            event_type = EventType::Heartbeat;
        }

        if (event_id % 7 == 0) {
            receive_timestamp = exchange_timestamp > 250 ? exchange_timestamp - 250 : 0;
        }

        if (event_id % 13 == 0 && event_id > 0) {
            exchange_timestamp -= 1'000;
        }
    }

    return MarketEvent{
        event_id,
        symbol_id,
        event_type,
        event_id % 2 == 0 ? Side::Bid : Side::Ask,
        exchange_timestamp,
        receive_timestamp,
        base_price + static_cast<std::int64_t>((event_id % 17) * 100),
        static_cast<std::uint32_t>(100 + (event_id % 50)),
    };
}

std::uint64_t percentile(std::vector<std::uint64_t>& values, double quantile) {
    if (values.empty()) {
        return 0;
    }

    std::sort(values.begin(), values.end());
    const auto index = static_cast<std::size_t>((values.size() - 1) * quantile);
    return values[index];
}

std::string bool_label(bool value) {
    return value ? "on" : "off";
}

}  // namespace

SimulationResult run_simulation(const SimulationConfig& config) {
    if (config.symbol_count == 0) {
        throw std::invalid_argument("symbol_count must be greater than zero");
    }
    if (config.producer_count == 0) {
        throw std::invalid_argument("producer_count must be greater than zero");
    }

    RingBuffer<MarketEvent> ring(config.capacity);
    std::atomic<std::uint64_t> generated{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> consumed{0};
    std::atomic<std::uint64_t> halt_events{0};
    std::atomic<std::uint64_t> timestamp_skews{0};
    std::atomic<std::uint64_t> out_of_order_events{0};
    std::atomic<std::uint64_t> burst_storms{0};
    std::atomic<bool> producers_done{false};
    std::vector<std::uint64_t> per_symbol_counts(config.symbol_count, 0);
    std::vector<std::uint64_t> latencies;
    latencies.reserve(config.event_count);

    std::thread consumer([&] {
        MarketEvent event;
        while (!producers_done.load(std::memory_order_acquire) ||
               consumed.load(std::memory_order_relaxed) < accepted.load(std::memory_order_relaxed)) {
            if (!ring.try_pop(event)) {
                std::this_thread::yield();
                continue;
            }

            if (event.symbol_id < per_symbol_counts.size()) {
                ++per_symbol_counts[event.symbol_id];
            }
            const auto latency = event.receive_timestamp_ns >= event.exchange_timestamp_ns
                                     ? event.receive_timestamp_ns - event.exchange_timestamp_ns
                                     : 0;
            latencies.push_back(latency);
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(config.producer_count);
    for (std::size_t producer = 0; producer < config.producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::uint64_t event_id = producer; event_id < config.event_count;
                 event_id += config.producer_count) {
                auto event = make_event(config, event_id);

                generated.fetch_add(1, std::memory_order_relaxed);
                if (event.type == EventType::Halt || event.type == EventType::Resume) {
                    halt_events.fetch_add(1, std::memory_order_relaxed);
                }
                if (event.receive_timestamp_ns < event.exchange_timestamp_ns) {
                    timestamp_skews.fetch_add(1, std::memory_order_relaxed);
                }
                if (config.chaos && event_id % 13 == 0 && event_id > 0) {
                    out_of_order_events.fetch_add(1, std::memory_order_relaxed);
                }
                if (config.chaos && event_id % 16 == 0) {
                    burst_storms.fetch_add(1, std::memory_order_relaxed);
                }

                if (config.chaos) {
                    if (ring.try_push(event)) {
                        accepted.fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    while (!ring.try_push(event)) {
                        std::this_thread::yield();
                    }
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& producer : producers) {
        producer.join();
    }
    producers_done.store(true, std::memory_order_release);
    consumer.join();

    auto p50_values = latencies;
    auto p95_values = latencies;
    auto p99_values = latencies;

    return SimulationResult{
        generated.load(std::memory_order_relaxed),
        accepted.load(std::memory_order_relaxed),
        consumed.load(std::memory_order_relaxed),
        halt_events.load(std::memory_order_relaxed),
        timestamp_skews.load(std::memory_order_relaxed),
        out_of_order_events.load(std::memory_order_relaxed),
        burst_storms.load(std::memory_order_relaxed),
        percentile(p50_values, 0.50),
        percentile(p95_values, 0.95),
        percentile(p99_values, 0.99),
        per_symbol_counts,
        ring.stats(),
    };
}

std::string format_simulation_summary(const SimulationConfig& config, const SimulationResult& result) {
    std::ostringstream output;
    output << "market-pulse simulation\n"
           << "symbols=" << config.symbol_count << " events=" << config.event_count
           << " producers=" << config.producer_count << " capacity=" << config.capacity
           << " chaos=" << bool_label(config.chaos) << '\n'
           << "generated=" << result.generated << " accepted=" << result.accepted
           << " consumed=" << result.consumed << " drops=" << result.ring.dropped
           << " producer_retries=" << result.ring.producer_retries << '\n'
           << "depth_current=" << result.ring.current_depth
           << " depth_max=" << result.ring.max_depth << '\n'
           << "p50_latency_ns=" << result.p50_latency_ns
           << " p95_latency_ns=" << result.p95_latency_ns
           << " p99_latency_ns=" << result.p99_latency_ns << '\n'
           << "halt_events=" << result.halt_events
           << " timestamp_skews=" << result.timestamp_skews
           << " out_of_order_events=" << result.out_of_order_events
           << " burst_storms=" << result.burst_storms << '\n';
    return output.str();
}

std::string format_html_report(const SimulationConfig& config, const SimulationResult& result) {
    const auto drop_rate = result.generated == 0
                               ? 0.0
                               : static_cast<double>(result.generated - result.accepted) * 100.0 /
                                     static_cast<double>(result.generated);
    const auto consume_rate = result.generated == 0
                                  ? 0.0
                                  : static_cast<double>(result.consumed) * 100.0 /
                                        static_cast<double>(result.generated);

    std::ostringstream output;
    output << std::fixed << std::setprecision(2)
           << "<!doctype html>\n"
           << "<html lang=\"en\">\n"
           << "<head>\n"
           << "  <meta charset=\"utf-8\">\n"
           << "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
           << "  <title>market-pulse report</title>\n"
           << "  <style>\n"
           << "    :root { color-scheme: light; --ink:#18202a; --muted:#667085; --line:#d8dee8; --fill:#f5f7fa; --accent:#0f766e; --warn:#b45309; }\n"
           << "    * { box-sizing: border-box; }\n"
           << "    body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; color:var(--ink); background:#ffffff; }\n"
           << "    main { max-width: 1040px; margin: 0 auto; padding: 40px 24px 56px; }\n"
           << "    header { border-bottom: 1px solid var(--line); padding-bottom: 22px; margin-bottom: 24px; }\n"
           << "    h1 { margin: 0 0 8px; font-size: clamp(2rem, 4vw, 3.4rem); line-height: 1; letter-spacing: 0; }\n"
           << "    h2 { margin: 0 0 14px; font-size: 1.05rem; letter-spacing: 0; }\n"
           << "    p { margin: 0; color: var(--muted); line-height: 1.55; }\n"
           << "    section { margin-top: 28px; }\n"
           << "    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }\n"
           << "    .metric { border:1px solid var(--line); border-radius:8px; padding:16px; background:var(--fill); min-height: 94px; }\n"
           << "    .metric strong { display:block; font-size:1.7rem; line-height:1.15; overflow-wrap:anywhere; }\n"
           << "    .metric span { display:block; margin-top:6px; color:var(--muted); font-size:.9rem; }\n"
           << "    .ok strong { color:var(--accent); }\n"
           << "    .warn strong { color:var(--warn); }\n"
           << "    table { width:100%; border-collapse: collapse; border:1px solid var(--line); border-radius:8px; overflow:hidden; display:table; }\n"
           << "    th, td { padding:12px 14px; border-bottom:1px solid var(--line); text-align:left; font-size:.95rem; }\n"
           << "    th { width:34%; background:var(--fill); color:var(--muted); font-weight:600; }\n"
           << "    tr:last-child th, tr:last-child td { border-bottom:0; }\n"
           << "    ul { margin:0; padding-left:20px; color:var(--ink); line-height:1.7; }\n"
           << "    code { background:var(--fill); border:1px solid var(--line); border-radius:6px; padding:2px 6px; }\n"
           << "    @media (max-width: 640px) { main { padding: 28px 16px 40px; } th, td { display:block; width:100%; } th { border-bottom:0; padding-bottom:4px; } td { padding-top:4px; } }\n"
           << "  </style>\n"
           << "</head>\n"
           << "<body>\n"
           << "  <main>\n"
           << "    <header>\n"
           << "      <h1>market-pulse report</h1>\n"
           << "      <p>C++20 multi-producer market replay through a lock-free MPSC ring buffer.</p>\n"
           << "    </header>\n"
           << "    <section>\n"
           << "      <h2>Run Summary</h2>\n"
           << "      <div class=\"grid\">\n"
           << "        <div class=\"metric ok\"><strong>" << result.generated << "</strong><span>events generated</span></div>\n"
           << "        <div class=\"metric ok\"><strong>" << result.accepted << "</strong><span>events accepted</span></div>\n"
           << "        <div class=\"metric ok\"><strong>" << result.consumed << "</strong><span>events consumed</span></div>\n"
           << "        <div class=\"metric\"><strong>" << consume_rate << "%</strong><span>generated events consumed</span></div>\n"
           << "      </div>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Latency</h2>\n"
           << "      <div class=\"grid\">\n"
           << "        <div class=\"metric\"><strong>" << result.p50_latency_ns << " ns</strong><span>p50 replay latency</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.p95_latency_ns << " ns</strong><span>p95 replay latency</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.p99_latency_ns << " ns</strong><span>p99 replay latency</span></div>\n"
           << "      </div>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Backpressure</h2>\n"
           << "      <div class=\"grid\">\n"
           << "        <div class=\"metric" << (result.ring.dropped > 0 ? " warn" : "") << "\"><strong>" << result.ring.dropped << "</strong><span>failed push attempts / chaos drops</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.ring.producer_retries << "</strong><span>producer CAS retries</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.ring.max_depth << "</strong><span>max queue depth</span></div>\n"
           << "        <div class=\"metric\"><strong>" << drop_rate << "%</strong><span>generated events not accepted</span></div>\n"
           << "      </div>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Configuration</h2>\n"
           << "      <table>\n"
           << "        <tr><th>symbols</th><td>" << config.symbol_count << "</td></tr>\n"
           << "        <tr><th>events</th><td>" << config.event_count << "</td></tr>\n"
           << "        <tr><th>producers</th><td>" << config.producer_count << "</td></tr>\n"
           << "        <tr><th>capacity</th><td>" << config.capacity << "</td></tr>\n"
           << "        <tr><th>seed</th><td>" << config.seed << "</td></tr>\n"
           << "        <tr><th>chaos mode</th><td>" << bool_label(config.chaos) << "</td></tr>\n"
           << "      </table>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Chaos Signals</h2>\n"
           << "      <div class=\"grid\">\n"
           << "        <div class=\"metric\"><strong>" << result.halt_events << "</strong><span>halt/resume events</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.timestamp_skews << "</strong><span>timestamp skews</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.out_of_order_events << "</strong><span>out-of-order events</span></div>\n"
           << "        <div class=\"metric\"><strong>" << result.burst_storms << "</strong><span>burst storms</span></div>\n"
           << "      </div>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Test Suite Coverage</h2>\n"
           << "      <ul>\n"
           << "        <li>Ring buffer empty/full behavior, FIFO ordering, wraparound, and capacity validation.</li>\n"
           << "        <li>Drop, current-depth, max-depth, and producer retry accounting.</li>\n"
           << "        <li>Multi-producer event integrity with exactly-once delivery in the retry path.</li>\n"
           << "        <li>Deterministic market replay counts and chaos-mode signal metrics.</li>\n"
           << "        <li>CLI summary formatting for generated, accepted, drop, and latency metrics.</li>\n"
           << "      </ul>\n"
           << "    </section>\n"
           << "    <section>\n"
           << "      <h2>Re-run</h2>\n"
           << "      <p><code>market-pulse simulate --symbols " << config.symbol_count
           << " --events " << config.event_count
           << " --producers " << config.producer_count
           << " --capacity " << config.capacity
           << " --seed " << config.seed
           << (config.chaos ? " --chaos" : "")
           << "</code></p>\n"
           << "    </section>\n"
           << "  </main>\n"
           << "</body>\n"
           << "</html>\n";
    return output.str();
}

}  // namespace market_pulse
