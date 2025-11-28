#!/usr/bin/env python3
"""
Query Similarity Results from HBase with Multi-Layer Ranking

This script queries the similarity_scores table in HBase, calculates F1/Overlap,
applies the custom three-layer ranking strategy, and exports the top results.
"""

import argparse
import sys
import os
import json
import csv
from typing import List, Dict
from collections import namedtuple

# --- RANKING LOGIC AND METRICS (START) ---

# Define structure for ranking/sorting purposes (used internally after HBase retrieval)
# Note: We assume the HBase results are converted to a similar dict/namedtuple structure.
# This structure is primarily used for sorting in rank_candidates.
Result = namedtuple('Result', ['doc_pair', 'similarity', 'match_count', 'w1', 'w2', 'f1_score', 'overlap_score'])

TOP_K = 5 # Default limit for ranking

def calculate_overlap_score(intersection, count1, count2):
    """Calculates the Overlap Coefficient: |A ∩ Q| / min(|A|, |Q|)"""
    min_count = min(count1, count2)
    if min_count == 0:
        return 0.0
    return intersection / min_count

def calculate_f1_score(intersection, count1, count2):
    """Calculates the F1-Score (assuming count1=Ebook Count, count2=Query Count)"""
    
    # We need to know which count belongs to the Ebook (A) and Query (Q).
    # Assuming count1 = Ebook Count (A), count2 = Query Count (Q)
    
    query_count = count2
    ebook_count = count1
    
    recall = intersection / query_count if query_count > 0 else 0.0
    precision = intersection / ebook_count if ebook_count > 0 else 0.0
    
    if precision + recall == 0:
        return 0.0
    
    return 2 * (precision * recall) / (precision + recall)


def rank_candidates(results: List[Dict], mode):
    """
    Applies the multi-layer ranking strategy (Jaccard -> F1 -> Overlap -> Raw Count)
    to the list of results fetched from HBase.
    """
    
    # 1. Enrich data with calculated metrics (F1, Overlap)
    enriched_candidates = []
    for item in results:
        # Assuming the HBase results dict already contains:
        # 'similarity' (Jaccard Score), 'match_count' (Intersection), 'w1', 'w2' (Counts)
        
        # We need to assume w1 and w2 represent Ebook Count and Query Count, respectively, or vice versa.
        # Assuming w1 = Ebook Count, w2 = Query Count based on standard IR practices.
        
        ebook_count  = item.get('w1', 0)
        query_count  = item.get('w2', 0)
        intersection = item.get('match_count', 0)
        pair         = item.get('doc_pair',0)
        if mode == 'jpii':
            if pair.startswith("000"):   # ebook on the left
                ebook_name = pair.split('-')[0]
            else:                        # ebook on the right
                ebook_name = pair.split('-')[1]
        else:
            ebook_name = pair
        item['doc_pair'] = ebook_name
        
        if query_count == 0 or ebook_count == 0: continue
        
        f1 = calculate_f1_score(intersection, ebook_count, query_count)
        overlap = calculate_overlap_score(intersection, ebook_count, query_count)
        
        # Add calculated fields for sorting
        item['f1_score'] = f1
        item['overlap_score'] = overlap
        
        enriched_candidates.append(item)


    # 2. Apply Multi-Criteria Sorting (Layer 2 & 3)
    # The sort keys are derived directly from the dictionary keys.
    
    final_ranking = sorted(
        enriched_candidates,
        key=lambda x: (
            # 1. Primary Sort Key: Jaccard Score (ROUNDED TO 2 DECIMAL PLACES)
            round(x['similarity'], 2), 
            # 2. Tie-breaker 1: F1-Score (ROUNDED TO 2 DECIMAL PLACES)
            round(x['f1_score'], 3), 
            # 3. Tie-breaker 2: Overlap Score
            x['overlap_score'], 
            # 4. Tie-breaker 3: Raw Intersection Count
            x['match_count'] 
        ),
        reverse=True # All criteria are sorted in descending order
    )
    
    return final_ranking[:TOP_K]

# --- RANKING LOGIC AND METRICS (END) ---

# Add script directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# NOTE: The hbase_connector module is assumed to be available.
from hbase_connector import HBaseConnector


def format_table(results: List[Dict], mode: str) -> str:
    """Format results as a text table, now showing F1 and Overlap."""
    if not results:
        return "No results found."

    # Header
    lines = []
    lines.append("=" * 140)
    lines.append("## 🏆 Top Ebooks Ranking 🏆")
    lines.append("=" * 140)
    # UPDATED HEADER
    lines.append(f"{'Rank':<6} | {'Ebook Name':<30} | {'Jaccard':<10} | {'F1-Score':<10} | {'Overlap':<10} | {'Shared Terms':<12}")
    lines.append("-" * 140)

    # Rows
    for i, result in enumerate(results, 1):
        lines.append(
            f"{i:<7} {result['doc_pair']:<32} "
            f"{result['similarity']:<13.2f} {result.get('f1_score', 0.0):<13.3f} "
            f"{result.get('overlap_score', 0.0):<13.3f} {result['match_count']:<12}"
        )

    lines.append("=" * 140)
    lines.append(f"Total results shown: {len(results)}")
    lines.append("")

    return "\n".join(lines)


def export_to_csv(results: List[Dict], output_file: str):
    """Export results to CSV file, now including F1 and Overlap."""
    if not results:
        print("[WARNING] No results to export")
        return

    # UPDATED FIELDNAMES
    fieldnames = ['rank', 'doc_pair', 'similarity', 'f1_score', 'overlap_score', 'match_count', 'w1', 'w2', 'mode', 'timestamp']

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
    """Export results to JSON file, now including F1 and Overlap."""
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
        """
    )

    # ... (parser arguments remain the same) ...
    parser.add_argument('--mode', type=str, choices=['jpii', 'pairwise'], required=True,
                        help='Query mode: jpii or pairwise')
    parser.add_argument('--document', type=str,
                        help='Document name to filter (for JPII mode, without .txt extension)')
    parser.add_argument('--threshold', type=float, default=0.0,
                        help='Minimum similarity threshold (default: 0.0)')
    parser.add_argument('--top', type=int,
                        help='Limit results to top N (default: no limit)')
    parser.add_argument('--output', type=str,
                        help='Output file path (CSV or JSON based on extension)')
    parser.add_argument('--format', type=str, choices=['table', 'csv', 'json'],
                        help='Output format (default: auto-detect from --output extension, or table)')
    parser.add_argument('--thrift-host', type=str, default='localhost',
                        help='HBase Thrift server hostname (default: localhost)')
    parser.add_argument('--thrift-port', type=int, default=9090,
                        help='HBase Thrift server port (default: 9090)')
    parser.add_argument('--query_hash', type=str, 
                        help='Query Hash Content Input')

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

    # ... (filtering INFO messages remain the same) ...
    if args.mode == 'jpii' and args.document:
        print(f"[INFO] Filtering by document: {args.document}")

    if args.threshold > 0:
        print(f"[INFO] Filtering by threshold: >= {args.threshold}")

    try:
        # 1. FETCH DATA FROM HBASE
        results = connector.query_similarity(
            mode=args.mode,
            document=args.document,
            threshold=args.threshold,
            limit=None # Fetch all needed results to apply global ranking
        )
        
        # 2. APPLY MULTI-LAYER RANKING LOGIC
        # We perform global sorting and enrich the data with F1/Overlap here.
        # This function also filters by TOP_K limit.
        ranked_results = rank_candidates(results, args.mode)
        connector.write_query_cache(args.query_hash, ranked_results)
        
        print(f"[SUCCESS] Found {len(ranked_results)} ranked results")

    except Exception as e:
        print(f"[ERROR] Query failed: {e}")
        connector.close()
        sys.exit(1)

    finally:
        connector.close()

    # Determine output format
    output_format = args.format

    # ... (output format detection remains the same) ...
    if not output_format and args.output:
        if args.output.endswith('.csv'):
            output_format = 'csv'
        elif args.output.endswith('.json'):
            output_format = 'json'
        else:
            output_format = 'table'
    elif not output_format:
        output_format = 'table'

    # Output results (using the globally ranked_results)
    if output_format == 'csv':
        if args.output:
            export_to_csv(ranked_results, args.output)
        else:
            print("[ERROR] --output required for CSV format")
            sys.exit(1)

    elif output_format == 'json':
        if args.output:
            export_to_json(ranked_results, args.output)
        else:
            print("[ERROR] --output required for JSON format")
            sys.exit(1)

    else:  # table format
        table = format_table(ranked_results, args.mode)
        print("\n" + table)

        # Also save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                f.write(table)
            print(f"[INFO] Table also saved to {args.output}")


if __name__ == '__main__':
    main()
