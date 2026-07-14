# Engineering guide

This guide explains how an event moves through `market-pulse`, why the original
design stopped scaling, and which performance claims the benchmark can support.

## End-to-end data flow

1. The replay configuration partitions symbols by `symbol_id % producer_count`.
2. Each producer generates only its assigned symbols in per-symbol sequence order.
3. The producer reserves a contiguous span in its private SPSC lane and constructs
   events directly in queue storage.
4. A release store publishes the entire span. No other producer can touch that
   lane, so publishing needs no compare-and-swap operation.
5. The consumer performs an acquire load only when its cached write position is
   exhausted, processes a contiguous batch in place, and releases the read cursor.
6. Quote, trade, halt, and resume events update the symbol's `TopOfBook` object.
7. After all threads join, the main thread combines lane-local metrics, sorts the
   sampled latency values once, and computes the state checksum and reports.

The final checksum covers explicit state fields rather than raw struct bytes, so
padding and lane interleaving cannot make a valid replay look nondeterministic.

## Components

### `SpscRing<T>`

The queue owns fixed-size, power-of-two storage and monotonic 64-bit cursors.
Masking replaces division during wraparound. Producer and consumer control blocks
are aligned to 64-byte cache lines, and each side caches the cursor owned by the
other side. An acquire load is therefore needed only on empty/full boundaries.

`reserve_write`/`commit_write` and `reserve_read`/`commit_read` form a two-phase
API. Reservations never cross the physical end of the array; callers naturally
handle wraparound on the next reservation. Publishing after a batch amortizes the
atomic store, and in-place processing removes an event copy.

Only the producer writes producer counters and only the consumer writes consumer
counters. Stats are read after `join`, which provides synchronization without
contended per-event telemetry atomics.

### Event generation and partitioning

The original queue allowed every producer to reserve from one global write index.
More producers meant more cache-line ownership transfers and failed CAS loops.
The new design assigns each symbol to one lane. This matches common feed-handler
partitioning and makes the independence assumption explicit: global ordering is
not promised, but order within a symbol is.

### Top-of-book consumer

The consumer maintains bid/ask price and size, last trade, traded volume, trade
count, and halt state. This prevents the benchmark from being only a transport
loop. One consumer owns all books, so no locks are needed in market-state updates.

The consumer visits lanes round-robin and processes at most `consumer_batch`
contiguous events before moving on. Smaller batches reduce queue residence time;
larger batches improve cache locality and amortize cursor publication.

### Timing

Throughput uses `steady_clock` around the synchronized worker phase. Allocation,
thread creation, warmup, percentile sorting, JSON, and HTML formatting are outside
the measured interval.

Latency is sampled before producer publication and immediately after consumer
observation. On supported x86-64 machines, `MonotonicClock` verifies invariant TSC,
calibrates cycles against `steady_clock`, and uses fenced `RDTSC`/`RDTSCP` reads.
Other platforms use `steady_clock`. Sampling avoids turning a clock read into the
dominant per-event cost.

The latency profile uses 64-slot lanes because queue depth is a latency budget.
A 65,536-slot saturated lane can honestly report milliseconds of residence time;
calling that an operation latency would be misleading.

### Backpressure

`spin` is lossless: a producer executes a CPU-relax instruction and retries. A
full observation and a spin iteration are telemetry, not data loss. `drop`
abandons exactly one event when the lane is full, and only that increments the
drop count. Maximum depth is calculated from valid lane-local cursors and cannot
underflow beyond `UINT64_MAX` as the old shared statistic did.

## Why it is faster

The original hot path performed a global producer CAS, per-slot sequence loads and
stores, shared pushed/dropped/retry atomics, a shared accepted counter, and depth
tracking for every event. Producers fought over the same cache lines. It also
copied and sorted the full latency vector three times even though those values
were synthetic feed timestamps.

The refactor changes the cost model:

- No producer-to-producer sharing or CAS on the ingress path.
- One release publication per produced batch and one release consume per batch.
- Cached opposing cursors avoid unnecessary acquire loads.
- Contiguous in-place generation and consumption improve prefetching.
- Fixed allocations keep allocation and destruction outside the hot loop.
- Thread-local metrics eliminate observer-induced contention.
- Symbol ownership removes locks from deterministic market state.
- Sampled timing and one percentile sort measure runtime without dominating it.
- CPU-relax instructions reduce pipeline and coherence pressure during short waits.

Native builds add `-march=native` and interprocedural optimization. Linux can pin
workers to one logical CPU per physical core before using siblings and can request
transparent huge pages for sufficiently large lane interiors.

The Linux CI matrix runs Release, AddressSanitizer, UndefinedBehaviorSanitizer,
and ThreadSanitizer builds. Local sanitizer presets are available as `ubsan` and
`tsan`; the Apple AddressSanitizer runtime may require an unrestricted host process.

## Benchmark discipline

Use throughput and latency profiles separately. Throughput deliberately allows
deep batching and disables timestamp reads. Latency uses shallow queues and batch
size one. Balanced is useful for change detection but is not a substitute for
either extreme.

For credible comparisons:

1. Use the same host, compiler, preset, profile, producer count, and event count.
2. Run at least five repetitions and use the reported median.
3. Use enough events to run well beyond scheduler startup noise.
4. On Linux, isolate or pin CPUs and avoid placing workers on sibling threads when
   physical cores are available.
5. Confirm `drops=0`, a stable `state_checksum`, and the expected affinity/TSC flags.
6. Save JSON and compare throughput and tail latency, not only one headline value.

`scripts/compare_benchmarks.py` enforces explicit throughput and p99.9 ratios for
two reports. Compare like-for-like profiles; a throughput report intentionally has
zero latency samples.

The benchmark is closed-loop: producers run as fast as possible. Under saturation,
handoff latency includes intentional queueing. A production service-level test
would additionally require an open-loop arrival-rate generator and coordinated
omission correction.

## PGO workflow

The PGO presets use Clang instrumentation:

```bash
cmake --preset pgo-generate
cmake --build --preset pgo-generate
./build-pgo/market-pulse benchmark --profile throughput --events 50000000 --repeats 3
llvm-profdata merge -output=build-pgo/market-pulse.profdata build-pgo/market-pulse-*.profraw
cmake --preset pgo-use
cmake --build --preset pgo-use
```

On macOS, invoke the merge command as `xcrun llvm-profdata merge ...`.

Train with workloads representative of deployment. PGO is an optional final step,
not a replacement for algorithmic improvements or repeatable measurement.

## Tradeoffs and next boundary

SPSC sharding requires stable symbol ownership. Arbitrary producers cannot submit
the same symbol concurrently without an upstream partitioner. The engine models
top-of-book state, not a full price-level order book or network parser. Those are
separate workloads and should be added with their own correctness fixtures and
benchmarks rather than hidden inside the queue measurement.
