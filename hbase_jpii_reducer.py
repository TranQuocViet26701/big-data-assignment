#!/usr/bin/python3

from itertools import groupby
from operator import itemgetter
import sys
import os
import hashlib
from datetime import datetime

"""
HBase-enabled JPII (Jaccard Similarity) Reducer

Computes Jaccard similarity and writes results to HBase table 'similarity_scores'.

Input:  [URL_i-URL_j@W_i@W_j, 1]  (from mapper)
Output: Writes to HBase table 'similarity_scores'
        Row key: <query_hash>:<document_name>
        Columns:
            - score:jaccard       → Jaccard similarity
            - score:shared_terms  → Number of shared terms
            - meta:query_text     → Original query
            - meta:timestamp      → Computation timestamp

HBase Table Schema:
    Table: similarity_scores
    Row Key: <query_hash>:<document> (e.g., "abc123:book1.txt")
    Column Families: score, meta
    Columns:
        - score:jaccard       → float (e.g., "0.7542")
        - score:shared_terms  → int (e.g., "25")
        - meta:query_text     → string (e.g., "animal wildlife")
        - meta:timestamp      → string (e.g., "2023-11-16 14:30:22")

Usage:
    This reducer is used with Hadoop Streaming and TableOutputFormat.
    Output format: row_key\tcolumn_family:column\tvalue
"""

def read_mapper_2_output(file, separator='\t'):
    """Read mapper output from STDIN."""
    for line in file:
        yield line.rstrip().split(separator, 1)

def get_query_hash(query_text):
    """Generate short hash for query text."""
    return hashlib.md5(query_text.encode()).hexdigest()[:8]

def main(separator='\t'):
    """
    Main reducer function that computes Jaccard similarity
    and outputs in HBase TableOutputFormat format.
    """
    data = read_mapper_2_output(sys.stdin, separator=separator)

    # Get query text from environment
    query_text = os.getenv('q_from_user', 'unknown_query')
    query_hash = get_query_hash(query_text)

    # Escape tabs in query text to prevent breaking TSV format
    query_text_escaped = query_text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

    for current_word, group in groupby(data, itemgetter(0)):
        # Generate timestamp per document for accurate timing
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            # Sum shared terms (each "1" represents one shared term)
            total_count = sum(int(count) for current_word, count in group)

            # Parse: URL_i-URL_j@W_i@W_j
            url_ij, w_i, w_j = current_word.split('@')

            # Calculate Jaccard similarity
            # Jaccard = |A ∩ B| / |A ∪ B|
            #         = intersection / (|A| + |B| - intersection)
            w_i = int(w_i)
            w_j = int(w_j)
            jaccard_similarity = float(total_count) / (w_i + w_j - total_count)

            # Extract document names from pair
            # url_ij format: "doc1.txt-query.txt" or "query.txt-doc1.txt"
            parts = url_ij.split('-')
            if len(parts) == 2:
                # Identify which one is the query and which is the document
                if 'query' in parts[0].lower():
                    doc_name = parts[1]
                else:
                    doc_name = parts[0]
            else:
                doc_name = url_ij  # Fallback

            # Create row key: query_hash:document_name
            row_key = f"{query_hash}:{doc_name}"

            # Output in HBase TableOutputFormat format
            # Format: row_key\tcolumn_family:column\tvalue

            # Score metrics
            print(f"{row_key}\tscore:jaccard\t{jaccard_similarity:.6f}")
            print(f"{row_key}\tscore:shared_terms\t{total_count}")

            # Metadata
            print(f"{row_key}\tmeta:query_text\t{query_text_escaped}")
            print(f"{row_key}\tmeta:timestamp\t{timestamp}")

        except ValueError as e:
            # Skip malformed records
            print(f"ERROR: Skipping malformed record: {current_word}", file=sys.stderr)
            continue
        except ZeroDivisionError:
            # Skip if union is zero (shouldn't happen but safeguard)
            continue

if __name__ == "__main__":
    main()
