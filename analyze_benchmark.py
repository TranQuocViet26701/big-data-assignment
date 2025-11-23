#!/usr/bin/env python3
"""
Benchmark Results Analysis Tool

Analyzes benchmark_results.csv and generates:
- Performance comparison charts
- Optimal configuration recommendations
- Scaling efficiency analysis
- Summary statistics

Usage:
    python3 analyze_benchmark.py benchmark_results.csv
"""

import sys
import csv
import os
from collections import defaultdict
from datetime import datetime

def load_results(csv_file):
    """Load benchmark results from CSV file."""
    results = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['Status'] == 'SUCCESS':
                    results.append({
                        'books': int(row['Books']),
                        'reducers': int(row['Reducers']),
                        'stage1_time': int(row['Stage1Time(s)']),
                        'stage2_time': int(row['Stage2Time(s)']),
                        'total_time': int(row['TotalTime(s)']),
                        'input_size': int(row['InputSize(bytes)']),
                        'index_size': int(row['IndexSize(bytes)']),
                        'jpii_size': int(row['JPIISize(bytes)']),
                        'unique_words': int(row['UniqueWords']),
                        'similar_docs': int(row['SimilarDocs']),
                        'throughput': float(row['Throughput(books/s)'])
                    })
        return results
    except FileNotFoundError:
        print(f"Error: File '{csv_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

def find_optimal_reducers(results):
    """Find optimal reducer count for each dataset size."""
    # Group by dataset size
    by_size = defaultdict(list)
    for r in results:
        by_size[r['books']].append(r)

    optimal = {}
    for books, tests in by_size.items():
        # Find test with minimum total time
        best = min(tests, key=lambda x: x['total_time'])
        optimal[books] = {
            'reducers': best['reducers'],
            'time': best['total_time'],
            'throughput': best['throughput']
        }

    return optimal

def calculate_statistics(results):
    """Calculate summary statistics."""
    if not results:
        return None

    total_times = [r['total_time'] for r in results]
    throughputs = [r['throughput'] for r in results]

    return {
        'total_tests': len(results),
        'avg_time': sum(total_times) / len(total_times),
        'min_time': min(total_times),
        'max_time': max(total_times),
        'avg_throughput': sum(throughputs) / len(throughputs),
        'max_throughput': max(throughputs)
    }

def analyze_scaling(results):
    """Analyze how performance scales with dataset size."""
    # Group by reducer count
    by_reducers = defaultdict(list)
    for r in results:
        by_reducers[r['reducers']].append(r)

    scaling = {}
    for reducers, tests in by_reducers.items():
        # Sort by dataset size
        tests_sorted = sorted(tests, key=lambda x: x['books'])
        if len(tests_sorted) >= 2:
            # Compare first and last
            first = tests_sorted[0]
            last = tests_sorted[-1]

            data_ratio = last['books'] / first['books']
            time_ratio = last['total_time'] / first['total_time']

            # Scaling efficiency (1.0 = perfect linear)
            efficiency = data_ratio / time_ratio if time_ratio > 0 else 0

            scaling[reducers] = {
                'efficiency': efficiency,
                'type': 'linear' if 0.9 <= efficiency <= 1.1 else
                        'sub-linear' if efficiency > 1.1 else 'super-linear'
            }

    return scaling

def analyze_bottleneck(results):
    """Identify which stage is the bottleneck."""
    stage1_pct = []
    stage2_pct = []

    for r in results:
        total = r['stage1_time'] + r['stage2_time']
        if total > 0:
            stage1_pct.append(100 * r['stage1_time'] / total)
            stage2_pct.append(100 * r['stage2_time'] / total)

    avg_stage1 = sum(stage1_pct) / len(stage1_pct) if stage1_pct else 0
    avg_stage2 = sum(stage2_pct) / len(stage2_pct) if stage2_pct else 0

    return {
        'stage1_pct': avg_stage1,
        'stage2_pct': avg_stage2,
        'bottleneck': 'Stage 1 (Inverted Index)' if avg_stage1 > avg_stage2 else 'Stage 2 (JPII)'
    }

def print_header(title):
    """Print formatted header."""
    print()
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)

def print_table(headers, rows, widths=None):
    """Print formatted table."""
    if not widths:
        widths = [len(h) for h in headers]

    # Print header
    header_line = "  ".join(f"{h:<{w}}" for h, w in zip(headers, widths))
    print(header_line)
    print("-" * len(header_line))

    # Print rows
    for row in rows:
        row_line = "  ".join(f"{str(v):<{w}}" for v, w in zip(row, widths))
        print(row_line)

def generate_ascii_chart(data, max_width=50):
    """Generate simple ASCII bar chart."""
    if not data:
        return

    max_val = max(v for _, v in data)
    for label, value in data:
        bar_width = int((value / max_val) * max_width) if max_val > 0 else 0
        bar = "█" * bar_width
        print(f"  {label:20} {bar} {value:.2f}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_benchmark.py benchmark_results.csv")
        sys.exit(1)

    csv_file = sys.argv[1]

    print_header("Benchmark Results Analysis")
    print(f"Reading: {csv_file}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load results
    results = load_results(csv_file)
    if not results:
        print("\nNo successful test results found!")
        sys.exit(1)

    print(f"Loaded {len(results)} successful test results")

    # Overall statistics
    print_header("Overall Statistics")
    stats = calculate_statistics(results)
    print(f"  Total Tests:       {stats['total_tests']}")
    print(f"  Average Time:      {stats['avg_time']:.1f}s")
    print(f"  Min Time:          {stats['min_time']}s")
    print(f"  Max Time:          {stats['max_time']}s")
    print(f"  Avg Throughput:    {stats['avg_throughput']:.3f} books/s")
    print(f"  Max Throughput:    {stats['max_throughput']:.3f} books/s")

    # Optimal reducers for each dataset size
    print_header("Optimal Reducer Configuration")
    optimal = find_optimal_reducers(results)

    headers = ["Books", "Optimal Reducers", "Time (s)", "Throughput (books/s)"]
    rows = []
    for books in sorted(optimal.keys()):
        opt = optimal[books]
        rows.append([
            books,
            opt['reducers'],
            opt['time'],
            f"{opt['throughput']:.3f}"
        ])

    print_table(headers, rows, [10, 20, 12, 25])

    print("\n  Recommendation:")
    reducer_ratio = sum(books / optimal[books]['reducers'] for books in optimal.keys()) / len(optimal)
    print(f"  → Use approximately 1 reducer per {reducer_ratio:.1f} books")

    # Performance by dataset size
    print_header("Performance by Dataset Size")
    by_books = defaultdict(list)
    for r in results:
        by_books[r['books']].append(r)

    for books in sorted(by_books.keys()):
        tests = by_books[books]
        print(f"\n  {books} books:")
        chart_data = [(f"{r['reducers']} reducers", r['total_time']) for r in sorted(tests, key=lambda x: x['reducers'])]
        generate_ascii_chart(chart_data, max_width=40)

    # Scaling efficiency
    print_header("Scaling Efficiency Analysis")
    scaling = analyze_scaling(results)

    if scaling:
        headers = ["Reducers", "Efficiency", "Scaling Type"]
        rows = []
        for reducers in sorted(scaling.keys()):
            s = scaling[reducers]
            rows.append([
                reducers,
                f"{s['efficiency']:.2f}",
                s['type']
            ])

        print_table(headers, rows, [12, 15, 20])

        print("\n  Notes:")
        print("  • Efficiency = 1.0  → Perfect linear scaling (2x data = 2x time)")
        print("  • Efficiency > 1.0  → Sub-linear (better than expected)")
        print("  • Efficiency < 1.0  → Super-linear (worse than expected)")
    else:
        print("  Not enough data for scaling analysis")

    # Bottleneck analysis
    print_header("Bottleneck Analysis")
    bottleneck = analyze_bottleneck(results)

    print(f"  Stage 1 (Inverted Index): {bottleneck['stage1_pct']:.1f}% of total time")
    print(f"  Stage 2 (JPII):           {bottleneck['stage2_pct']:.1f}% of total time")
    print()
    print(f"  Bottleneck: {bottleneck['bottleneck']}")

    if bottleneck['stage1_pct'] > 70:
        print("\n  Optimization Tips:")
        print("  → Focus on optimizing inverted index building")
        print("  → Consider increasing reducers for Stage 1")
        print("  → Enable compression to reduce shuffle size")
    elif bottleneck['stage2_pct'] > 70:
        print("\n  Optimization Tips:")
        print("  → Focus on optimizing similarity computation")
        print("  → Reduce query complexity")
        print("  → Consider filtering candidate pairs more aggressively")

    # Throughput comparison
    print_header("Throughput Comparison")
    throughput_data = []
    for books in sorted(by_books.keys()):
        tests = by_books[books]
        best = max(tests, key=lambda x: x['throughput'])
        throughput_data.append((f"{books} books", best['throughput']))

    generate_ascii_chart(throughput_data, max_width=50)

    # Data size analysis
    print_header("Data Size Analysis")

    headers = ["Books", "Input (MB)", "Index (MB)", "JPII (KB)", "Compression"]
    rows = []
    for books in sorted(by_books.keys()):
        tests = by_books[books]
        # Average across all reducer counts
        avg_input = sum(r['input_size'] for r in tests) / len(tests) / 1024 / 1024
        avg_index = sum(r['index_size'] for r in tests) / len(tests) / 1024 / 1024
        avg_jpii = sum(r['jpii_size'] for r in tests) / len(tests) / 1024
        compression = avg_index / avg_input if avg_input > 0 else 0

        rows.append([
            books,
            f"{avg_input:.1f}",
            f"{avg_index:.1f}",
            f"{avg_jpii:.1f}",
            f"{compression:.2f}x"
        ])

    print_table(headers, rows, [10, 15, 15, 15, 15])

    # Summary recommendations
    print_header("Summary & Recommendations")

    print("\n  Key Findings:")
    print(f"  1. Optimal reducer configuration found for each dataset size")
    print(f"  2. {bottleneck['bottleneck']} is the primary bottleneck")
    print(f"  3. Best throughput: {stats['max_throughput']:.3f} books/s")

    print("\n  Recommended Configuration:")
    for books in sorted(optimal.keys()):
        opt = optimal[books]
        print(f"  • {books:3d} books → {opt['reducers']:2d} reducers ({opt['time']:3d}s, {opt['throughput']:.3f} books/s)")

    print("\n  Next Steps:")
    print("  1. Use the optimal reducer counts for production workloads")
    print("  2. Monitor YARN ResourceManager for detailed job metrics")
    print("  3. Consider enabling compression if index size is large")
    print("  4. Re-run benchmark on larger datasets to verify scaling patterns")

    print_header("Analysis Complete")
    print()

if __name__ == "__main__":
    main()
