# market-pulse

Lock-free market replay engine built around a bounded ring buffer.

## Build

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```
