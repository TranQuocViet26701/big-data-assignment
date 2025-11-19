#!/usr/bin/env python3
"""
Query Similarity Results from HBase

This script queries the similarity_scores table in HBase without requiring Spark.
It uses HappyBase to directly query HBase via Thrift server.

Usage:
    # Query JPII results for a specific document
    python3 query_similarity.py --mode jpii --document my_query --top 20

    # Query all pairwise results above threshold
    python3 query_similarity.py --mode pairwise --threshold 0.1 --top 100

    # Export to CSV
    python3 query_similarity.py --mode jpii --document my_query --output results.csv

    # Export to JSON
    python3 query_similarity.py --mode pairwise --threshold 0.2 --output results.json
"""

import argparse
import sys
import os
import json
import csv
from typing import List, Dict

# Add script directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from hbase_connector import HBaseConnector


def format_table(results: List[Dict], mode: str) -> str:
    """Format results as a text table"""
    if not results:
        return "No results found."

    # Header
    lines = []
    lines.append("=" * 100)
    lines.append(f"Similarity Results ({mode.upper()} mode)")
    lines.append("=" * 100)
    lines.append(f"{'Rank':<6} {'Document Pair':<50} {'Similarity':<12} {'Match Count':<12}")
    lines.append("-" * 100)

    # Rows
    for i, result in enumerate(results, 1):
        lines.append(
            f"{i:<6} {result['doc_pair']:<50} "
            f"{result['similarity']:<12.6f} {result['match_count']:<12}"
        )

    lines.append("=" * 100)
    lines.append(f"Total results: {len(results)}")
    lines.append("")

    return "\n".join(lines)


def export_to_csv(results: List[Dict], output_file: str):
    """Export results to CSV file"""
    if not results:
        print("[WARNING] No results to export")
        return

    fieldnames = ['rank', 'doc_pair', 'similarity', 'match_count', 'w1', 'w2', 'mode', 'timestamp']

    try:
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, result in enumerate(results, 1):
                row = result.copy()
                row['rank'] = i
                writer.writerow(row)

        print(f"[SUCCESS] Results exported to {output_file}")

    except Exception as e:
        print(f"[ERROR] Failed to export to CSV: {e}")
        raise


def export_to_json(results: List[Dict], output_file: str):
    """Export results to JSON file"""
    if not results:
        print("[WARNING] No results to export")
        return

    # Add rank to results
    ranked_results = []
    for i, result in enumerate(results, 1):
        result_with_rank = result.copy()
        result_with_rank['rank'] = i
        ranked_results.append(result_with_rank)

    try:
        with open(output_file, 'w') as f:
            json.dump(ranked_results, f, indent=2)

        print(f"[SUCCESS] Results exported to {output_file}")

    except Exception as e:
        print(f"[ERROR] Failed to export to JSON: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Query similarity results from HBase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query JPII results for a specific document (top 20)
  python3 query_similarity.py --mode jpii --document my_query --top 20

  # Query all pairwise results above 0.1 similarity
  python3 query_similarity.py --mode pairwise --threshold 0.1

  # Export JPII results to CSV
  python3 query_similarity.py --mode jpii --document my_query --output results.csv

  # Export pairwise results to JSON with threshold
  python3 query_similarity.py --mode pairwise --threshold 0.2 --output results.json --top 100

  # Query with custom HBase server
  python3 query_similarity.py --mode jpii --document my_query \\
      --thrift-host hbase-server --thrift-port 9090
        """
    )

    # Required arguments
    parser.add_argument('--mode', type=str, choices=['jpii', 'pairwise'], required=True,
                        help='Query mode: jpii or pairwise')

    # Optional filters
    parser.add_argument('--document', type=str,
                        help='Document name to filter (for JPII mode, without .txt extension)')
    parser.add_argument('--threshold', type=float, default=0.0,
                        help='Minimum similarity threshold (default: 0.0)')
    parser.add_argument('--top', type=int,
                        help='Limit results to top N (default: no limit)')

    # Output options
    parser.add_argument('--output', type=str,
                        help='Output file path (CSV or JSON based on extension)')
    parser.add_argument('--format', type=str, choices=['table', 'csv', 'json'],
                        help='Output format (default: auto-detect from --output extension, or table)')

    # HBase connection
    parser.add_argument('--thrift-host', type=str, default='localhost',
                        help='HBase Thrift server hostname (default: localhost)')
    parser.add_argument('--thrift-port', type=int, default=9090,
                        help='HBase Thrift server port (default: 9090)')

    args = parser.parse_args()

    # Connect to HBase
    print(f"[INFO] Connecting to HBase at {args.thrift_host}:{args.thrift_port}...")

    try:
        connector = HBaseConnector(host=args.thrift_host, port=args.thrift_port)
    except Exception as e:
        print(f"[ERROR] Failed to connect to HBase: {e}")
        sys.exit(1)

    # Query similarity results
    print(f"[INFO] Querying {args.mode.upper()} similarity results...")

    if args.mode == 'jpii' and args.document:
        print(f"[INFO] Filtering by document: {args.document}")

    if args.threshold > 0:
        print(f"[INFO] Filtering by threshold: >= {args.threshold}")

    try:
        results = connector.query_similarity(
            mode=args.mode,
            document=args.document,
            threshold=args.threshold,
            limit=args.top
        )

        print(f"[SUCCESS] Found {len(results)} results")

    except Exception as e:
        print(f"[ERROR] Query failed: {e}")
        connector.close()
        sys.exit(1)

    finally:
        connector.close()

    # Determine output format
    output_format = args.format

    if not output_format and args.output:
        # Auto-detect from extension
        if args.output.endswith('.csv'):
            output_format = 'csv'
        elif args.output.endswith('.json'):
            output_format = 'json'
        else:
            output_format = 'table'
    elif not output_format:
        output_format = 'table'

    # Output results
    if output_format == 'csv':
        if args.output:
            export_to_csv(results, args.output)
        else:
            print("[ERROR] --output required for CSV format")
            sys.exit(1)

    elif output_format == 'json':
        if args.output:
            export_to_json(results, args.output)
        else:
            print("[ERROR] --output required for JSON format")
            sys.exit(1)

    else:  # table format
        table = format_table(results, args.mode)
        print("\n" + table)

        # Also save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                f.write(table)
            print(f"[INFO] Table also saved to {args.output}")


if __name__ == '__main__':
    main()
