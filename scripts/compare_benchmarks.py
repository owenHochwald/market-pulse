#!/usr/bin/env python3
"""Compare two market-pulse JSON reports using explicit regression budgets."""

import argparse
import json
import sys
from pathlib import Path


def read_report(path: Path) -> dict:
    with path.open(encoding="utf-8") as report_file:
        return json.load(report_file)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--min-throughput-ratio", type=float, default=0.98)
    parser.add_argument("--max-p999-ratio", type=float, default=1.10)
    args = parser.parse_args()

    baseline = read_report(args.baseline)
    candidate = read_report(args.candidate)
    baseline_throughput = float(baseline["events_per_second"])
    candidate_throughput = float(candidate["events_per_second"])
    throughput_ratio = candidate_throughput / baseline_throughput

    failures = []
    if throughput_ratio < args.min_throughput_ratio:
        failures.append(
            f"throughput ratio {throughput_ratio:.4f} is below {args.min_throughput_ratio:.4f}"
        )

    baseline_p999 = int(baseline["p999_handoff_ns"])
    candidate_p999 = int(candidate["p999_handoff_ns"])
    if baseline_p999 > 0:
        latency_ratio = candidate_p999 / baseline_p999
        if latency_ratio > args.max_p999_ratio:
            failures.append(
                f"p99.9 ratio {latency_ratio:.4f} exceeds {args.max_p999_ratio:.4f}"
            )
    else:
        latency_ratio = 0.0

    print(f"throughput_ratio={throughput_ratio:.4f}")
    print(f"p999_ratio={latency_ratio:.4f}")
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
