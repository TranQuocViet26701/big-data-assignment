#!/usr/bin/env python3
"""
Import MapReduce output from HDFS to HBase.

This script reads tab-separated output files from HDFS and writes them to HBase
using the happybase library via Thrift API.

Expected input format (TSV):
    row_key\tcolumn_family:qualifier\tvalue

Usage:
    python3 import_to_hbase.py --hdfs-path /output/path --table-name inverted_index
    python3 import_to_hbase.py --hdfs-path /output/path --table-name similarity_scores --batch-size 1000
"""

import argparse
import subprocess
import sys
import happybase
from collections import defaultdict


def read_hdfs_directory(hdfs_path):
    """
    Read all part files from HDFS directory.

    Args:
        hdfs_path: HDFS directory path containing part-* files

    Yields:
        Lines from all part files
    """
    try:
        # List files in HDFS directory
        list_cmd = ['hadoop', 'fs', '-ls', hdfs_path]
        result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)

        # Find part files
        part_files = []
        for line in result.stdout.strip().split('\n'):
            if 'part-' in line:
                parts = line.split()
                if parts:
                    file_path = parts[-1]  # Last column is the path
                    part_files.append(file_path)

        if not part_files:
            print(f"Warning: No part files found in {hdfs_path}", file=sys.stderr)
            return

        print(f"Found {len(part_files)} part files in {hdfs_path}")

        # Read each part file
        for part_file in sorted(part_files):
            print(f"Reading {part_file}...")
            cat_cmd = ['hadoop', 'fs', '-cat', part_file]
            result = subprocess.run(cat_cmd, capture_output=True, text=True, check=True)

            for line in result.stdout.strip().split('\n'):
                if line:  # Skip empty lines
                    yield line

    except subprocess.CalledProcessError as e:
        print(f"Error reading from HDFS: {e}", file=sys.stderr)
        print(f"Command output: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def parse_line(line, table_name):
    """
    Parse tab-separated line into row_key, column, value.

    Supports two formats:
    1. HBase format: row_key\tcolumn_family:qualifier\tvalue
    2. Inverted index format: term\turl1@count1\turl2@count2\t...
       (converted to multiple rows: term\tdocs:url1\tcount1, etc.)

    Args:
        line: Tab-separated line
        table_name: HBase table name (to detect format)

    Yields:
        (row_key, column, value) tuples
    """
    parts = line.split('\t')

    # Check if this is inverted index format (for inverted_index table)
    # Format: term\turl1@count1\turl2@count2\t...
    if table_name == 'inverted_index' and len(parts) >= 2:
        term = parts[0]
        url_counts = parts[1:]  # All remaining parts are url@count pairs

        # Check if this looks like inverted index format (has @ separators)
        if any('@' in uc for uc in url_counts):
            # Parse as inverted index format
            for url_count in url_counts:
                if '@' in url_count:
                    try:
                        url, count = url_count.split('@', 1)
                        yield term, f'docs:{url}', count
                    except ValueError:
                        print(f"Warning: Invalid url@count format: {url_count}", file=sys.stderr)
                        continue
            return

    # Otherwise, parse as standard HBase format
    if len(parts) != 3:
        print(f"Warning: Invalid line format (expected 3 fields or inverted index format): {line[:100]}", file=sys.stderr)
        return

    row_key, column, value = parts

    if ':' not in column:
        print(f"Warning: Invalid column format (expected family:qualifier): {column}", file=sys.stderr)
        return

    yield row_key, column, value


def import_to_hbase(hdfs_path, table_name, host='localhost', port=9090, batch_size=1000):
    """
    Import data from HDFS to HBase.

    Args:
        hdfs_path: HDFS directory containing MapReduce output
        table_name: HBase table name
        host: HBase Thrift server host
        port: HBase Thrift server port
        batch_size: Number of rows to batch before writing
    """
    print(f"\n{'='*60}")
    print(f"Importing HDFS → HBase")
    print(f"{'='*60}")
    print(f"HDFS Path:  {hdfs_path}")
    print(f"HBase Table: {table_name}")
    print(f"Thrift Host: {host}:{port}")
    print(f"Batch Size:  {batch_size}")
    print(f"{'='*60}\n")

    # Connect to HBase
    try:
        print(f"Connecting to HBase Thrift server at {host}:{port}...")
        connection = happybase.Connection(host=host, port=port)
        connection.open()
        print("Connected successfully!")

        # Verify table exists
        tables = [t.decode('utf-8') if isinstance(t, bytes) else t for t in connection.tables()]
        if table_name not in tables:
            print(f"Error: Table '{table_name}' not found in HBase", file=sys.stderr)
            print(f"Available tables: {tables}", file=sys.stderr)
            sys.exit(1)

        table = connection.table(table_name)
        print(f"Table '{table_name}' verified\n")

    except Exception as e:
        print(f"Error connecting to HBase: {e}", file=sys.stderr)
        print(f"\nMake sure HBase Thrift server is running:", file=sys.stderr)
        print(f"  hbase thrift start -p {port}", file=sys.stderr)
        sys.exit(1)

    # Read and import data
    try:
        batch_data = defaultdict(dict)  # {row_key: {column: value}}
        total_lines = 0
        total_rows = 0
        invalid_lines = 0

        for line in read_hdfs_directory(hdfs_path):
            total_lines += 1

            # parse_line now yields tuples (to handle inverted index expansion)
            parsed_entries = list(parse_line(line, table_name))
            if not parsed_entries:
                invalid_lines += 1
                continue

            # Process each parsed entry (may be multiple for inverted index format)
            for row_key, column, value in parsed_entries:
                # Add to batch
                # Convert column from "family:qualifier" to bytes
                batch_data[row_key][column.encode('utf-8')] = value.encode('utf-8')

                # Write batch if size reached
                if len(batch_data) >= batch_size:
                    write_batch(table, batch_data)
                    total_rows += len(batch_data)
                    print(f"Progress: {total_rows} rows imported ({total_lines} lines processed)...")
                    batch_data.clear()

        # Write remaining data
        if batch_data:
            write_batch(table, batch_data)
            total_rows += len(batch_data)

        connection.close()

        # Summary
        print(f"\n{'='*60}")
        print(f"Import Complete!")
        print(f"{'='*60}")
        print(f"Total lines processed: {total_lines}")
        print(f"Total rows imported:   {total_rows}")
        print(f"Invalid lines skipped: {invalid_lines}")
        print(f"{'='*60}\n")

    except KeyboardInterrupt:
        print("\n\nImport interrupted by user", file=sys.stderr)
        connection.close()
        sys.exit(1)
    except Exception as e:
        print(f"\nError during import: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        connection.close()
        sys.exit(1)


def write_batch(table, batch_data):
    """
    Write a batch of data to HBase.

    Args:
        table: HBase table object
        batch_data: Dictionary {row_key: {column: value}}
    """
    try:
        with table.batch() as batch:
            for row_key, columns in batch_data.items():
                batch.put(row_key.encode('utf-8'), columns)
    except Exception as e:
        print(f"Error writing batch: {e}", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Import MapReduce output from HDFS to HBase',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Import inverted index
  python3 import_to_hbase.py --hdfs-path /output/inverted-index --table-name inverted_index

  # Import with custom batch size
  python3 import_to_hbase.py --hdfs-path /output/similarity --table-name similarity_scores --batch-size 5000

  # Use custom Thrift server
  python3 import_to_hbase.py --hdfs-path /output --table-name mydata --host thrift.server.com --port 9090
        '''
    )

    parser.add_argument('--hdfs-path', required=True,
                        help='HDFS directory containing MapReduce output (part-* files)')
    parser.add_argument('--table-name', required=True,
                        help='HBase table name to import into')
    parser.add_argument('--host', default='localhost',
                        help='HBase Thrift server host (default: localhost)')
    parser.add_argument('--port', type=int, default=9090,
                        help='HBase Thrift server port (default: 9090)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows to batch before writing (default: 1000)')

    args = parser.parse_args()

    import_to_hbase(
        hdfs_path=args.hdfs_path,
        table_name=args.table_name,
        host=args.host,
        port=args.port,
        batch_size=args.batch_size
    )


if __name__ == '__main__':
    main()
