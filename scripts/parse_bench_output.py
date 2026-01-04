#!/usr/bin/env python3
"""
Parse Criterion benchmark output and generate structured reports.

This script extracts benchmark results from Criterion's text output and produces:
1. Markdown summary for GitHub Actions
2. JSON output for programmatic processing
3. Clear regression/improvement identification

Usage:
    python3 scripts/parse_bench_output.py bench_output.txt
"""

import re
import sys
import json
from dataclasses import dataclass, asdict
from typing import List, Optional
from enum import Enum


class ChangeType(Enum):
    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    NO_CHANGE = "no_change"
    NEW_BENCHMARK = "new"


@dataclass
class BenchmarkResult:
    name: str
    time_estimate: str  # e.g., "1.234 µs"
    change_percent: Optional[str]  # e.g., "+5.23%"
    change_type: ChangeType
    confidence: Optional[str]  # e.g., "p = 0.00 < 0.01"
    throughput: Optional[str] = None


def parse_criterion_output(output: str) -> List[BenchmarkResult]:
    """Parse Criterion benchmark output into structured results."""
    results = []
    lines = output.split('\n')

    i = 0
    while i < len(lines):
        line = lines[i]

        # Criterion outputs benchmark name on one line, then indented "time:" on next line
        # Example:
        # allocation_ci/packet_allocation
        #                         time:   [142.35 ns 143.12 ns 143.94 ns]
        #
        # OR sometimes on same line:
        # level0/crypto/encrypt  time:   [773.86 ns 775.44 ns 777.14 ns]

        # Try to match time line (could have name on same line or be indented)
        time_match = re.search(r'time:\s+\[(.+?)\]', line)
        if time_match:
            time_values = time_match.group(1).strip().split()
            time_estimate = time_values[1]  # Take middle value

            # Try to extract benchmark name from this line first
            bench_name = line.split('time:')[0].strip()

            # If name is empty (indented line), look at previous line
            if not bench_name and i > 0:
                prev_line = lines[i - 1].strip()
                # Skip empty lines and metadata lines
                if prev_line and not prev_line.startswith('Gnuplot') and not prev_line.startswith('Running') and not prev_line.startswith('Found'):
                    bench_name = prev_line

            # Skip if we still don't have a benchmark name
            if not bench_name:
                i += 1
                continue

            # Look ahead for change information
            change_percent = None
            change_type = ChangeType.NO_CHANGE
            confidence = None
            throughput = None

            # Check next few lines for change, throughput, and status
            for j in range(i + 1, min(i + 6, len(lines))):
                next_line = lines[j]

                # Throughput line: "thrpt:  [1.2345 GiB/s 1.3456 GiB/s 1.4567 GiB/s]"
                thrpt_match = re.search(r'thrpt:\s+\[(.+?)\]', next_line)
                if thrpt_match:
                    thrpt_values = thrpt_match.group(1).strip().split()
                    throughput = thrpt_values[1] if len(thrpt_values) > 1 else thrpt_values[0]

                # Change line: "change: [-1.23% +2.34% +5.67%] (p = 0.02 < 0.05)"
                change_match = re.search(r'change:\s+\[(.+?)\]\s*(\(p\s*=\s*.+?\))?', next_line)
                if change_match:
                    change_values = change_match.group(1).strip().split()
                    change_percent = change_values[1] if len(change_values) > 1 else change_values[0]
                    if change_match.group(2):
                        confidence = change_match.group(2).strip('()')

                # Status lines
                if 'Performance has regressed' in next_line:
                    change_type = ChangeType.REGRESSION
                elif 'Performance has improved' in next_line:
                    change_type = ChangeType.IMPROVEMENT
                elif 'No change in performance detected' in next_line:
                    change_type = ChangeType.NO_CHANGE

                # Stop if we hit the next benchmark (new time: line)
                if 'time:' in next_line and next_line != line:
                    break

            results.append(BenchmarkResult(
                name=bench_name,
                time_estimate=time_estimate,
                change_percent=change_percent,
                change_type=change_type,
                confidence=confidence,
                throughput=throughput
            ))

        i += 1

    return results


def format_markdown_summary(results: List[BenchmarkResult]) -> str:
    """Generate a markdown summary suitable for GitHub Actions."""
    regressions = [r for r in results if r.change_type == ChangeType.REGRESSION]
    improvements = [r for r in results if r.change_type == ChangeType.IMPROVEMENT]
    no_change = [r for r in results if r.change_type == ChangeType.NO_CHANGE]

    md = []

    # Summary stats
    md.append("## Benchmark Results Summary\n")
    md.append(f"- **Total benchmarks**: {len(results)}")
    md.append(f"- **Regressions**: {len(regressions)} ⚠️" if regressions else "- **Regressions**: 0 ✅")
    md.append(f"- **Improvements**: {len(improvements)} 🚀" if improvements else "- **Improvements**: 0")
    md.append(f"- **No significant change**: {len(no_change)}")
    md.append("")

    # Detailed regressions
    if regressions:
        md.append("### ⚠️ Performance Regressions")
        md.append("")
        md.append("| Benchmark | Time | Change | Confidence |")
        md.append("|-----------|------|--------|------------|")
        for r in sorted(regressions, key=lambda x: float(x.change_percent.strip('%+')) if x.change_percent else 0, reverse=True):
            conf = r.confidence or "N/A"
            change = r.change_percent or "N/A"
            md.append(f"| `{r.name}` | {r.time_estimate} | **{change}** | {conf} |")
        md.append("")

    # Detailed improvements
    if improvements:
        md.append("### 🚀 Performance Improvements")
        md.append("")
        md.append("| Benchmark | Time | Change | Confidence |")
        md.append("|-----------|------|--------|------------|")
        for r in sorted(improvements, key=lambda x: float(x.change_percent.strip('%+-')) if x.change_percent else 0):
            conf = r.confidence or "N/A"
            change = r.change_percent or "N/A"
            md.append(f"| `{r.name}` | {r.time_estimate} | **{change}** | {conf} |")
        md.append("")

    # Note about interpretation
    if regressions or improvements:
        md.append("---")
        md.append("**Note**: These results compare against the most recent baseline from the `main` branch.")
        md.append("Regressions may be false positives if:")
        md.append("- Recent PRs improved performance on main but this PR doesn't include those changes yet")
        md.append("- GitHub-hosted runner had different CPU contention")
        md.append("- The baseline is from an older commit (check the cache restore log)")

    return '\n'.join(md)


def format_pr_comment(results: List[BenchmarkResult]) -> str:
    """Generate a concise PR comment."""
    regressions = [r for r in results if r.change_type == ChangeType.REGRESSION]

    if not regressions:
        return None

    lines = [
        "## ⚠️ Performance Benchmark Regressions Detected",
        "",
        f"Found {len(regressions)} benchmark(s) with performance regressions:",
        ""
    ]

    # Show top 10 worst regressions
    sorted_regressions = sorted(
        regressions,
        key=lambda x: float(x.change_percent.strip('%+')) if x.change_percent else 0,
        reverse=True
    )[:10]

    for r in sorted_regressions:
        change = r.change_percent or "unknown"
        lines.append(f"- **`{r.name}`**: {change}")

    if len(regressions) > 10:
        lines.append(f"- ... and {len(regressions) - 10} more (see workflow summary)")

    lines.extend([
        "",
        "### ⚠️ Important: This may be a false positive!",
        "",
        "**Common causes of false positives:**",
        "1. **Stale baseline**: If recent PRs improved performance on `main`, this PR (which doesn't include those changes) will show as \"regressed\" when compared to the new baseline",
        "2. **GitHub runner variance**: Benchmarks run on shared `ubuntu-latest` runners with variable CPU contention",
        "3. **Old baseline**: The baseline might be from an older `main` commit if the cache restore used `restore-keys` fallback",
        "",
        "**To verify if this is a real regression:**",
        "1. Check if recent commits on `main` touched transport or benchmark code",
        "2. Merge `main` into your branch and re-run benchmarks",
        "3. Review the baseline age in the \"Download main branch baseline\" step",
        "",
        "> This is informational only and does not block the PR."
    ])

    return '\n'.join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/parse_bench_output.py <bench_output.txt>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]

    try:
        with open(input_file, 'r') as f:
            output = f.read()
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found", file=sys.stderr)
        sys.exit(1)

    results = parse_criterion_output(output)

    if not results:
        print("Warning: No benchmark results found in input", file=sys.stderr)
        sys.exit(0)

    # Write JSON output
    json_output = {
        'results': [asdict(r) for r in results],
        'summary': {
            'total': len(results),
            'regressions': len([r for r in results if r.change_type == ChangeType.REGRESSION]),
            'improvements': len([r for r in results if r.change_type == ChangeType.IMPROVEMENT]),
            'no_change': len([r for r in results if r.change_type == ChangeType.NO_CHANGE])
        }
    }

    with open('bench_results.json', 'w') as f:
        json.dump(json_output, f, indent=2, default=str)

    # Write markdown summary
    with open('bench_summary.md', 'w') as f:
        f.write(format_markdown_summary(results))

    # Write PR comment if there are regressions
    pr_comment = format_pr_comment(results)
    if pr_comment:
        with open('bench_pr_comment.md', 'w') as f:
            f.write(pr_comment)

    # Print summary to stdout
    print(format_markdown_summary(results))

    # Exit with code 1 if regressions found (for CI)
    has_regressions = any(r.change_type == ChangeType.REGRESSION for r in results)
    sys.exit(1 if has_regressions else 0)


if __name__ == '__main__':
    main()
