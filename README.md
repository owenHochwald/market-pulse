# market-pulse

`market-pulse` is a C++20 lock-free market replay engine built around a bounded
MPSC ring buffer. It simulates exchange-style event bursts, pushes them through a
non-blocking queue, drains them through a single market-state consumer, and
reports the latency/backpressure metrics that decide whether the system actually
survives load.

The twist: chaos mode injects halt/resume events, burst storms, timestamp skew,
and out-of-order feed behavior so the project is more than a generic ring buffer.

## Build and Test

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

If CMake is unavailable, the core test binary can be compiled directly:

```bash
clang++ -std=c++20 -Wall -Wextra -Wpedantic -Iinclude \
  src/simulation.cpp tests/test_main.cpp -o /tmp/market_pulse_tests
/tmp/market_pulse_tests
```

## Run

```bash
cmake --build build
./build/market-pulse simulate --symbols 4 --events 1000 --producers 2 --capacity 1024 --seed 7 --chaos
```

Direct compiler fallback:

```bash
clang++ -std=c++20 -Wall -Wextra -Wpedantic -Iinclude \
  src/simulation.cpp src/main.cpp -o /tmp/market-pulse
/tmp/market-pulse simulate --symbols 4 --events 1000 --producers 2 --capacity 1024 --seed 7 --chaos
```

Example output:

```text
market-pulse simulation
symbols=4 events=1000 producers=2 capacity=1024 chaos=on
generated=1000 accepted=1000 consumed=1000 drops=0 producer_retries=86
depth_current=0 depth_max=327
p50_latency_ns=25 p95_latency_ns=1025 p99_latency_ns=1025
halt_events=118 timestamp_skews=133 out_of_order_events=76 burst_storms=63
```

## Architecture

```text
synthetic feed producers
  -> lock-free MPSC RingBuffer<MarketEvent>
  -> single market-state consumer
  -> latency, depth, drop, retry, and chaos metrics
```

The ring buffer uses sequence-numbered slots and a CAS-reserved producer cursor.
Consumers release slots by advancing their sequence numbers after reading. The
capacity must be a power of two so slot lookup stays a mask operation instead of
a modulo in the hot path.

## What Is Tested

- Empty/full behavior, FIFO ordering, wraparound, and power-of-two capacity validation.
- Drop, current-depth, max-depth, and producer CAS retry accounting.
- Multi-producer integrity with unique event ranges.
- Deterministic market replay counts.
- Chaos-mode metrics for halts, timestamp skew, out-of-order events, and burst storms.
- CLI summary formatting for generated, accepted, drop, and latency metrics.

## Resume Framing

- Built a C++20 lock-free MPSC market replay engine with sequence-numbered ring
  slots, cache-line-separated atomics, and non-blocking producer semantics.
- Added deterministic and chaos-mode exchange simulations with halt/resume
  events, burst storms, timestamp skew, and out-of-order feed behavior.
- Exposed latency percentiles, queue depth, drop counts, and producer CAS retry
  metrics through a CLI service and red/green tested each behavioral slice.
