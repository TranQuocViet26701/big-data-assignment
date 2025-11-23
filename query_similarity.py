#!/usr/bin/env python3
"""
Real-time Similarity Query Tool

Queries HBase 'similarity_scores' table to find similar documents
for a given search query.

Features:
- Fast real-time lookups (no MapReduce needed)
- Supports filtering by minimum similarity score
- Returns top-K most similar documents
- Shows query metadata (when computed, shared terms)

Usage:
    python3 query_similarity.py "animal wildlife nature"
    python3 query_similarity.py "animal wildlife" --top 5 --min-score 0.3
    python3 query_similarity.py --query-file my_query.txt --top 20

Requirements:
    pip install happybase
"""

import argparse
import hashlib
import sys
from datetime import datetime

try:
    import happybase
except ImportError:
    print("Error: happybase library not found.")
    print("Install with: pip install happybase")
    sys.exit(1)

# Color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def get_query_hash(query_text):
    """Generate hash for query text (same as reducer)."""
    return hashlib.md5(query_text.encode()).hexdigest()[:8]

def connect_hbase(host='localhost', port=9090):
    """Connect to HBase Thrift server."""
    try:
        connection = happybase.Connection(host=host, port=port)
        connection.tables()  # Test connection
        return connection
    except Exception as e:
        print(f"{Colors.FAIL}Error: Cannot connect to HBase at {host}:{port}{Colors.ENDC}")
        print(f"Details: {e}")
        print("\nMake sure HBase Thrift server is running:")
        print("  hbase thrift start -p 9090")
        sys.exit(1)

def query_similarity(connection, query_text, top_k=10, min_score=0.0):
    """
    Query HBase for documents similar to the given query.

    Args:
        connection: HBase connection
        query_text: Search query string
        top_k: Number of top results to return
        min_score: Minimum Jaccard similarity score (0.0-1.0)

    Returns:
        List of tuples: (document_name, jaccard_score, shared_terms, timestamp)
    """
    query_hash = get_query_hash(query_text)

    # Access similarity_scores table
    table = connection.table('similarity_scores')

    # Scan for all rows starting with query_hash
    # Row key format: <query_hash>:<document_name>
    row_prefix = f"{query_hash}:".encode()

    results = []

    try:
        for key, data in table.scan(row_prefix=row_prefix):
            key_str = key.decode('utf-8')
            doc_name = key_str.split(':', 1)[1]

            # Extract metrics
            jaccard = float(data.get(b'score:jaccard', b'0').decode('utf-8'))
            shared_terms = int(data.get(b'score:shared_terms', b'0').decode('utf-8'))
            timestamp = data.get(b'meta:timestamp', b'unknown').decode('utf-8')

            # Filter by minimum score
            if jaccard >= min_score:
                results.append((doc_name, jaccard, shared_terms, timestamp))

    except Exception as e:
        print(f"{Colors.FAIL}Error querying HBase: {e}{Colors.ENDC}")
        return []

    # Sort by Jaccard score (descending)
    results.sort(key=lambda x: x[1], reverse=True)

    # Return top K
    return results[:top_k]

def display_results(query_text, results, top_k):
    """Display query results in a formatted table."""
    print()
    print(f"{Colors.HEADER}{'='*80}{Colors.ENDC}")
    print(f"{Colors.HEADER}  Similarity Search Results{Colors.ENDC}")
    print(f"{Colors.HEADER}{'='*80}{Colors.ENDC}")
    print()

    print(f"{Colors.OKBLUE}Query:{Colors.ENDC} \"{query_text}\"")
    print(f"{Colors.OKBLUE}Query Hash:{Colors.ENDC} {get_query_hash(query_text)}")
    print(f"{Colors.OKBLUE}Top Results:{Colors.ENDC} {min(len(results), top_k)}")
    print()

    if not results:
        print(f"{Colors.WARNING}No similar documents found.{Colors.ENDC}")
        print()
        print("Possible reasons:")
        print("  1. Query hasn't been processed yet (run MapReduce pipeline first)")
        print("  2. No documents match the query terms")
        print("  3. All matches filtered by minimum score threshold")
        print()
        return

    # Table header
    print(f"{Colors.BOLD}{'Rank':<6} {'Document':<40} {'Score':<10} {'Terms':<10} {'Computed':<20}{Colors.ENDC}")
    print("-" * 86)

    # Table rows
    for rank, (doc_name, jaccard, shared_terms, timestamp) in enumerate(results, 1):
        # Color-code by score
        if jaccard >= 0.7:
            score_color = Colors.OKGREEN
        elif jaccard >= 0.4:
            score_color = Colors.OKCYAN
        else:
            score_color = Colors.WARNING

        print(f"{rank:<6} {doc_name:<40} {score_color}{jaccard:<10.4f}{Colors.ENDC} {shared_terms:<10} {timestamp:<20}")

    print()
    print(f"{Colors.OKGREEN}Found {len(results)} similar documents{Colors.ENDC}")
    print()

def print_statistics(connection):
    """Print statistics about the HBase tables."""
    try:
        table = connection.table('similarity_scores')
        count = 0
        for _ in table.scan(limit=1000):
            count += 1

        print(f"{Colors.OKCYAN}HBase Statistics:{Colors.ENDC}")
        print(f"  Similarity scores stored: {count}+ entries")
        print()
    except Exception as e:
        print(f"{Colors.WARNING}Cannot retrieve statistics: {e}{Colors.ENDC}")

def main():
    parser = argparse.ArgumentParser(
        description='Query document similarity from HBase in real-time',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Search for similar documents
  %(prog)s "animal wildlife nature"

  # Get top 5 results with minimum 30%% similarity
  %(prog)s "animal wildlife" --top 5 --min-score 0.3

  # Read query from file
  %(prog)s --query-file my_query.txt --top 20

  # Query specific HBase server
  %(prog)s "conservation" --host my-hbase-server --port 9090

  # Show statistics
  %(prog)s --stats
        """
    )

    parser.add_argument(
        'query',
        nargs='?',
        help='Search query text (e.g., "animal wildlife")'
    )
    parser.add_argument(
        '--query-file',
        help='Read query from file instead of command line'
    )
    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='Number of top results to return (default: 10)'
    )
    parser.add_argument(
        '--min-score',
        type=float,
        default=0.0,
        help='Minimum Jaccard similarity score 0.0-1.0 (default: 0.0)'
    )
    parser.add_argument(
        '--host',
        default='localhost',
        help='HBase Thrift server host (default: localhost)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=9090,
        help='HBase Thrift server port (default: 9090)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show HBase table statistics'
    )

    args = parser.parse_args()

    # Get query text
    if args.query_file:
        try:
            with open(args.query_file, 'r') as f:
                query_text = f.read().strip()
        except FileNotFoundError:
            print(f"{Colors.FAIL}Error: File '{args.query_file}' not found{Colors.ENDC}")
            sys.exit(1)
    elif args.query:
        query_text = args.query.strip()
    elif args.stats:
        query_text = None  # No query needed for stats
    else:
        parser.print_help()
        sys.exit(1)

    # Connect to HBase
    print(f"{Colors.OKBLUE}Connecting to HBase at {args.host}:{args.port}...{Colors.ENDC}")
    connection = connect_hbase(host=args.host, port=args.port)
    print(f"{Colors.OKGREEN}Connected successfully{Colors.ENDC}")

    # Show statistics if requested
    if args.stats:
        print_statistics(connection)
        if not query_text:
            connection.close()
            return

    # Validate min_score
    if not 0.0 <= args.min_score <= 1.0:
        print(f"{Colors.FAIL}Error: --min-score must be between 0.0 and 1.0{Colors.ENDC}")
        sys.exit(1)

    # Query similarity
    results = query_similarity(
        connection=connection,
        query_text=query_text,
        top_k=args.top,
        min_score=args.min_score
    )

    # Display results
    display_results(query_text, results, args.top)

    # Close connection
    connection.close()

if __name__ == '__main__':
    main()
