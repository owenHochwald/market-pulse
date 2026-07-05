# market-pulse

`market-pulse` is a C++20 market replay engine built around a lock-free,
multi-producer/single-consumer ring buffer. It generates exchange-style market
events, pushes them through concurrent producers, drains them through one market
state consumer, and reports the latency and backpressure metrics that show how
the system behaves under load.

The project is meant to demonstrate systems work that is easy to run and inspect:
lock-free data structures, deterministic replay, chaos testing, and a small CLI
that can push a 5,000,000-event multi-producer replay in a release build.

## Highlights

- Lock-free bounded MPSC ring buffer with sequence-numbered slots.
- Multi-producer market event replay into a single consumer.
- Throughput-oriented release scenario for 5M+ events/sec on local hardware.
- Chaos mode for halt/resume events, burst storms, timestamp skew, and
  out-of-order feed behavior.
- CLI metrics for accepted/consumed events, queue depth, retries, drops, and
  p50/p95/p99 replay latency.
- Generated HTML report with run metrics and a concise test-suite coverage view.
- Self-contained CTest suite covering ring-buffer behavior, concurrency,
  deterministic replay, chaos metrics, and CLI/report formatting.

## Quick Start

Download:

```bash
git clone https://github.com/owenHochwald/market-pulse.git && cd market-pulse
```

Build:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build
```

See every CLI option:

```bash
./build/market-pulse --help
```

Run a 5M-event multi-producer replay:

```bash
./build/market-pulse simulate --symbols 128 --events 5000000 --producers 2 --capacity 8388608 --seed 7
```

Each simulation prints a terminal summary and writes a local HTML report:

```text
html_report="/path/to/market-pulse-report.html"
html_report_url=file:///path/to/market-pulse-report.html
```

Open the `file://` URL in a browser to view a lightweight dashboard with run
metrics, latency, backpressure, chaos signals, and test-suite coverage. Use
`--report reports/run.html` to choose a path, or `--no-report` to skip HTML
generation.

Run with exchange-style chaos enabled:

```bash
./build/market-pulse simulate --symbols 8 --events 10000 --producers 4 --capacity 4096 --seed 7 --chaos
```

## Test

One command from the repo root:

```bash
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

The suite validates:

- Empty/full behavior, FIFO ordering, wraparound, and capacity validation.
- Drop, depth, retry, and max-depth accounting.
- Multi-producer event integrity.
- Deterministic market replay counts.
- Chaos-mode halt, skew, out-of-order, and burst-storm metrics.
- CLI summary and HTML report output.

## Architecture

```text
synthetic feed producers
  -> lock-free RingBuffer<MarketEvent>
  -> market-state consumer
  -> latency, depth, retry, drop, and chaos metrics
```

The ring buffer reserves producer slots with compare-and-swap, publishes events
with per-slot sequence numbers, and uses power-of-two capacity so hot-path slot
lookup is a mask operation. Normal replay retries on backpressure; chaos replay
can intentionally drop events to model overloaded feeds.

## Requirements

- C++20 compiler
- CMake 3.20+
- pthreads-compatible threading support
