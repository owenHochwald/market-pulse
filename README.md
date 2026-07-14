# market-pulse

`market-pulse` is a C++20 market replay and benchmarking engine built around
cache-isolated, single-producer/single-consumer ingress lanes. Producers generate
deterministic exchange-style events directly into preallocated queue storage. A
single consumer batch-drains the lanes into per-symbol top-of-book state and
reports real handoff latency, throughput, backpressure, and a deterministic state
checksum.

## Build and test

```bash
cmake --preset release
cmake --build --preset release
ctest --preset release
```

For a machine-specific performance build with `-march=native` and IPO:

```bash
cmake --preset release-native
cmake --build --preset release-native
ctest --preset release-native
```

## Replay

```bash
./build-release/market-pulse simulate \
  --symbols 128 --events 5000000 --producers 4 \
  --capacity 4096 --batch 16 --sample-rate 64
```

`simulate` is lossless by default. `--chaos --backpressure drop` injects halt,
resume, skew, and out-of-order signals while deliberately dropping events when a
lane is full. Full-queue observations and spin retries are never reported as
dropped events.

The command prints stable key/value metrics and writes an HTML report unless
`--no-report` is supplied.

## Benchmarks

```bash
# Maximum sustained throughput: deep lanes, large batches, timing disabled.
./build-native/market-pulse benchmark --profile throughput \
  --symbols 128 --events 50000000 --producers 4 --repeats 5

# Queue residence time: shallow lanes, one-at-a-time draining, 1/64 sampling.
./build-native/market-pulse benchmark --profile latency \
  --symbols 128 --events 5000000 --producers 2 --repeats 5

# Mixed workload and machine-readable output.
./build-native/market-pulse benchmark --profile balanced \
  --events 10000000 --json reports/balanced.json
```

Profile defaults are intentionally different:

| Profile | Lane capacity | Batch | Latency sampling |
|---|---:|---:|---:|
| `throughput` | 65,536 | 8,192 | off |
| `latency` | 64 | 1 | every 64th event |
| `balanced` | 1,024 | 16 | every 64th event |

Explicit `--capacity`, `--batch`, and `--sample-rate` values override these
defaults. Linux runs can add `--pin`, `--cpus 2,4,6,8,10`, and `--huge-pages`.
CPU lists place the consumer first and producers after it.

## Results

Observed on an Apple-silicon laptop using the native Release build; affinity,
invariant TSC timing, and huge-page advice are unavailable on this platform:

| Scenario | Result |
|---|---:|
| Original shared MPSC, 5M events, 4 producers | 1.28 s median / 3.9M events/sec |
| Sharded SPSC, 50M events, 4 producers | 0.07 s median / 714M events/sec |
| Throughput benchmark, 100M events, 4 producers | 698M events/sec median |
| Latency profile, 5M events, 2 producers | p50 1.8 us / p99 2.4 us / p99.9 8.5 us |

The first two rows are whole-process measurements from the same compiler and
non-native Release configuration. The new throughput run disables handoff timing;
the old implementation had no way to disable its per-event telemetry and final
latency-vector copies/sorts, so the table measures the intended throughput modes,
not identical instrumentation. These are local observations, not
hardware-independent promises. Use longer runs,
pin workers, fix CPU frequency policy, and compare JSON from the same host when
evaluating a change. Queue latency under deliberate saturation measures backlog;
it is not an intrinsic operation-latency number.

Compare saved results with explicit regression budgets:

```bash
python3 scripts/compare_benchmarks.py baseline.json candidate.json \
  --min-throughput-ratio 0.98 --max-p999-ratio 1.10
```

## Architecture

```text
symbol-partitioned producers
  -> one preallocated SPSC lane per producer
  -> round-robin batch consumer
  -> deterministic top-of-book state
  -> off-hot-path aggregation and reports
```

Each symbol belongs to exactly one producer, so events for that symbol retain
order without a shared reservation counter. Events for independent symbols may
interleave differently without changing the final checksum.

See [docs/performance.md](docs/performance.md) for the complete data flow,
memory-ordering contract, benchmark methodology, Linux tuning, PGO workflow, and
an explanation of every optimization.

## Requirements

- C++20 compiler
- CMake 3.20+
- pthread-compatible threads
- Linux x86-64 for affinity, transparent huge-page advice, and invariant-TSC timing
