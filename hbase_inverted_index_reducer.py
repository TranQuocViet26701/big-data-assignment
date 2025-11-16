#!/usr/bin/python3

from itertools import groupby
from operator import itemgetter
import sys
import os

"""
HBase-enabled Inverted Index Reducer

Instead of writing to HDFS, this reducer writes the inverted index
directly to HBase table 'inverted_index'.

Input:  [term_k, URL_i@W_i]  (from mapper)
Output: Writes to HBase table 'inverted_index'
        Row key: term_k
        Column: docs:<URL_i>
        Value: W_i (word count)

HBase Table Schema:
    Table: inverted_index
    Row Key: term (e.g., "elephant")
    Column Family: docs
    Columns: docs:<document_name> (e.g., docs:book1.txt)
    Value: word_count (e.g., "5234")

Usage:
    This reducer is used with Hadoop Streaming and TableOutputFormat.
    Output format: row_key\tcolumn_family:column\tvalue
"""

def read_mapper_output(file, separator='\t'):
    """Read mapper output from STDIN."""
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t', second_sep='@'):
    """
    Main reducer function that outputs in HBase TableOutputFormat format.

    Output Format for HBase:
        row_key\tcolumn_family:column_qualifier\tvalue
    """
    data = read_mapper_output(sys.stdin, separator=separator)

    # Get total number of documents (mappers)
    total_map = os.getenv('total_map_tasks')
    try:
        total_map = int(total_map.strip())
    except:
        total_map = 0

    for current_word, group in groupby(data, itemgetter(0)):
        # Collect all URL@count pairs for this term
        uacs = [uc for _, uc in group]
        uacs = [(url, int(count)) for url, count in read_mapper_output(uacs, second_sep)]

        # Filter: Skip words that appear in ALL documents (too common)
        if len(uacs) == total_map:
            continue

        # Sort by count (descending) for efficient storage
        sorted_uacs = sorted(uacs, key=itemgetter(1), reverse=True)

        # Output in HBase TableOutputFormat format
        # Format: row_key\tcolumn_family:column\tvalue
        for url, count in sorted_uacs:
            # Row key: the term
            # Column: docs:<document_name>
            # Value: word count
            print(f"{current_word}\tdocs:{url}\t{count}")

if __name__ == "__main__":
    main()
