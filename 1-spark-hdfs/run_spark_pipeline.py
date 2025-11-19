#!/usr/bin/env python3
"""
Spark Pipeline Execution and Metrics Collection Script

This script executes the Spark-based inverted index and similarity search pipeline,
measures performance metrics, and saves results to mode-specific CSV files.

Supports two modes:
  - jpii: Query-based similarity (requires --query or --query-file)
  - pairwise: All-pairs similarity (no query needed)

Usage:
    # JPII mode with query string:
    python3 run_spark_pipeline.py --mode jpii --num-books 100 \\
        --input-dir /gutenberg-input-100 \\
        --query "wildlife conservation hunting animals"

    # JPII mode with query file:
    python3 run_spark_pipeline.py --mode jpii --num-books 100 \\
        --input-dir /gutenberg-input-100 \\
        --query-file /path/to/query.txt

    # Pairwise mode (no query needed):
    python3 run_spark_pipeline.py --mode pairwise --num-books 100 \\
        --input-dir /gutenberg-input-100
"""

import argparse
import subprocess
import time
import csv
import os
import sys
from datetime import datetime
import tempfile
import re


class SparkPipelineRunner:
    """Manages Spark pipeline execution and metrics collection"""

    def __init__(self, args):
        self.args = args
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.mode = args.mode
        self.metrics = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mode': args.mode,
            'num_books': args.num_books,
            'num_executors': args.num_executors,
            'executor_memory': args.executor_memory,
        }

        # Generate unique output paths based on timestamp
        timestamp_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.index_output = f"{args.index_output}_{timestamp_suffix}" if not args.index_output.endswith(timestamp_suffix) else args.index_output
        self.stage2_output = f"{args.stage2_output}_{timestamp_suffix}" if not args.stage2_output.endswith(timestamp_suffix) else args.stage2_output

    def run_command(self, cmd, description):
        """Execute a shell command and return output"""
        print(f"\n[INFO] {description}")
        print(f"[CMD] {cmd}")

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode != 0:
                print(f"[ERROR] Command failed with return code {result.returncode}")
                print(f"[STDERR] {result.stderr}")
                return None

            return result.stdout.strip()

        except subprocess.TimeoutExpired:
            print(f"[ERROR] Command timed out after 1 hour")
            return None
        except Exception as e:
            print(f"[ERROR] Command execution failed: {e}")
            return None

    def get_hdfs_size_mb(self, hdfs_path):
        """Get size of HDFS path in MB"""
        cmd = f"hdfs dfs -du -s {hdfs_path} 2>/dev/null | awk '{{print $1}}'"
        output = self.run_command(cmd, f"Getting size of {hdfs_path}")

        if output and output.isdigit():
            size_bytes = int(output)
            size_mb = size_bytes / (1024 * 1024)
            return round(size_mb, 2)
        return 0.0

    def count_hdfs_lines(self, hdfs_path):
        """Count number of lines in HDFS file(s)"""
        cmd = f"hdfs dfs -cat {hdfs_path}/part-* 2>/dev/null | wc -l"
        output = self.run_command(cmd, f"Counting lines in {hdfs_path}")

        if output and output.isdigit():
            return int(output)
        return 0

    def validate_hdfs_path(self, hdfs_path):
        """Check if HDFS path exists"""
        cmd = f"hdfs dfs -test -e {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0

    def create_query_file(self):
        """Create or use query file based on arguments"""
        # If query file is provided, use it directly
        if hasattr(self.args, 'query_file') and self.args.query_file:
            query_file_path = self.args.query_file

            # Validate file exists
            if not os.path.exists(query_file_path):
                raise Exception(f"Query file not found: {query_file_path}")

            # Read query content for metrics
            with open(query_file_path, 'r') as f:
                query_content = f.read().strip()

            print(f"[INFO] Using query file: {query_file_path}")
            print(f"[INFO] Query content: {query_content}")

            # Return tuple: (file_path, should_cleanup, query_content)
            return (query_file_path, False, query_content)

        # Otherwise, create temporary file from query string
        else:
            query_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
            query_file.write(self.args.query)
            query_file.close()
            print(f"[INFO] Created temporary query file: {query_file.name}")
            print(f"[INFO] Query text: {self.args.query}")

            # Return tuple: (file_path, should_cleanup, query_content)
            return (query_file.name, True, self.args.query)

    def run_inverted_index(self):
        """Execute Stage 1: Inverted Index with Spark"""
        print("\n" + "="*80)
        print("STAGE 1: Building Inverted Index with Spark")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_inverted_index.py')

        # Construct spark-submit command
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode cluster \\
            --num-executors {self.args.num_executors} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            {spark_script} \\
            {self.args.input_dir} \\
            {self.index_output}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark Inverted Index job")
        stage1_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 1 (Inverted Index) failed")

        self.metrics['stage1_time_sec'] = round(stage1_time, 2)
        print(f"[SUCCESS] Stage 1 completed in {stage1_time:.2f} seconds")

        # Collect Stage 1 metrics
        print("\n[INFO] Collecting Stage 1 metrics...")
        self.metrics['input_size_mb'] = self.get_hdfs_size_mb(self.args.input_dir)
        self.metrics['index_size_mb'] = self.get_hdfs_size_mb(self.index_output)
        self.metrics['unique_words'] = self.count_hdfs_lines(self.index_output)

        print(f"[METRIC] Input size: {self.metrics['input_size_mb']:.2f} MB")
        print(f"[METRIC] Index size: {self.metrics['index_size_mb']:.2f} MB")
        print(f"[METRIC] Unique words: {self.metrics['unique_words']}")

    def run_jpii(self, query_file):
        """Execute Stage 2: JPII Similarity Search with Spark"""
        print("\n" + "="*80)
        print("STAGE 2: Computing Document Similarity with Spark (JPII)")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_jpii.py')

        # Use the actual output path (with part-* files)
        index_files = f"{self.index_output}/part-*"

        # Construct spark-submit command
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode client \\
            --num-executors {self.args.num_executors} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            {spark_script} \\
            {index_files} \\
            {query_file} \\
            {self.stage2_output}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark JPII similarity search")
        stage2_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 2 (JPII) failed")

        self.metrics['stage2_time_sec'] = round(stage2_time, 2)
        print(f"[SUCCESS] Stage 2 completed in {stage2_time:.2f} seconds")

        # Collect Stage 2 metrics
        print("\n[INFO] Collecting Stage 2 metrics...")
        self.metrics['output_size_mb'] = self.get_hdfs_size_mb(self.stage2_output)
        self.metrics['similarity_pairs'] = self.count_hdfs_lines(self.stage2_output)

        print(f"[METRIC] Output size: {self.metrics['output_size_mb']:.2f} MB")
        print(f"[METRIC] Similarity pairs: {self.metrics['similarity_pairs']}")

    def run_pairwise(self):
        """Execute Stage 2: Pairwise Similarity with Spark"""
        print("\n" + "="*80)
        print("STAGE 2: Computing All-Pairs Document Similarity with Spark (Pairwise)")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_pairwise.py')

        # Use the actual output path (with part-* files)
        index_files = f"{self.index_output}/part-*"

        # Construct spark-submit command
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode cluster \\
            --num-executors {self.args.num_executors} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            {spark_script} \\
            {index_files} \\
            {self.stage2_output}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark Pairwise all-pairs similarity")
        stage2_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 2 (Pairwise) failed")

        self.metrics['stage2_time_sec'] = round(stage2_time, 2)
        print(f"[SUCCESS] Stage 2 completed in {stage2_time:.2f} seconds")

        # Collect Stage 2 metrics
        print("\n[INFO] Collecting Stage 2 metrics...")
        self.metrics['output_size_mb'] = self.get_hdfs_size_mb(self.stage2_output)
        self.metrics['similarity_pairs'] = self.count_hdfs_lines(self.stage2_output)

        # Calculate total possible pairs for pairwise mode
        total_possible_pairs = (self.args.num_books * (self.args.num_books - 1)) // 2
        self.metrics['total_possible_pairs'] = total_possible_pairs
        self.metrics['pairs_computed'] = self.metrics['similarity_pairs']

        print(f"[METRIC] Output size: {self.metrics['output_size_mb']:.2f} MB")
        print(f"[METRIC] Similarity pairs: {self.metrics['similarity_pairs']:,}")
        print(f"[METRIC] Total possible pairs: {total_possible_pairs:,}")
        print(f"[METRIC] Pairs computed: {self.metrics['pairs_computed']:,}")

    def calculate_totals(self):
        """Calculate total pipeline metrics"""
        self.metrics['total_time_sec'] = round(
            self.metrics['stage1_time_sec'] + self.metrics['stage2_time_sec'],
            2
        )
        self.metrics['throughput_books_per_sec'] = round(
            self.args.num_books / self.metrics['total_time_sec'] if self.metrics['total_time_sec'] > 0 else 0,
            4
        )

        # Calculate pairwise-specific throughput
        if self.mode == 'pairwise' and 'pairs_computed' in self.metrics:
            self.metrics['throughput_pairs_per_sec'] = round(
                self.metrics['pairs_computed'] / self.metrics['stage2_time_sec'] if self.metrics['stage2_time_sec'] > 0 else 0,
                2
            )

    def save_metrics_to_csv(self):
        """Save metrics to mode-specific CSV file"""
        # Choose CSV file based on mode
        if self.mode == 'jpii':
            csv_file = os.path.join(self.script_dir, 'spark_jpii_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'stage1_time_sec',
                'stage2_time_sec',
                'total_time_sec',
                'input_size_mb',
                'index_size_mb',
                'output_size_mb',
                'unique_words',
                'similarity_pairs',
                'throughput_books_per_sec',
                'num_executors',
                'executor_memory',
                'driver_memory',
                'input_dir',
                'index_output',
                'jpii_output'
            ]
            # Add JPII-specific fields
            self.metrics['jpii_output'] = self.stage2_output

        elif self.mode == 'pairwise':
            csv_file = os.path.join(self.script_dir, 'spark_pairwise_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'total_possible_pairs',
                'stage1_time_sec',
                'stage2_time_sec',
                'total_time_sec',
                'input_size_mb',
                'index_size_mb',
                'output_size_mb',
                'unique_words',
                'similarity_pairs',
                'pairs_computed',
                'throughput_books_per_sec',
                'throughput_pairs_per_sec',
                'num_executors',
                'executor_memory',
                'driver_memory',
                'input_dir',
                'index_output',
                'pairwise_output'
            ]
            # Add pairwise-specific fields
            self.metrics['pairwise_output'] = self.stage2_output

        file_exists = os.path.exists(csv_file)

        # Add common fields to metrics
        self.metrics['driver_memory'] = self.args.driver_memory
        self.metrics['input_dir'] = self.args.input_dir
        self.metrics['index_output'] = self.index_output

        try:
            with open(csv_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # Write header if file is new
                if not file_exists:
                    writer.writeheader()

                # Write metrics row
                writer.writerow(self.metrics)

            print(f"\n[SUCCESS] Metrics saved to {csv_file}")

        except Exception as e:
            print(f"[ERROR] Failed to save metrics to CSV: {e}")
            raise

    def display_summary(self):
        """Display summary of pipeline execution"""
        print("\n" + "="*80)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*80)
        print(f"Mode:                   {self.mode.upper()}")
        print(f"Timestamp:              {self.metrics['timestamp']}")
        print(f"Dataset:                {self.metrics['num_books']} books")
        print(f"Input Directory:        {self.args.input_dir}")
        print(f"")
        print(f"Stage 1 (Index):        {self.metrics['stage1_time_sec']:.2f} sec")
        print(f"Stage 2 (Similarity):   {self.metrics['stage2_time_sec']:.2f} sec")
        print(f"Total Pipeline Time:    {self.metrics['total_time_sec']:.2f} sec")
        print(f"")
        print(f"Input Size:             {self.metrics['input_size_mb']:.2f} MB")
        print(f"Index Size:             {self.metrics['index_size_mb']:.2f} MB")
        print(f"Output Size:            {self.metrics['output_size_mb']:.2f} MB")
        print(f"")
        print(f"Unique Words:           {self.metrics['unique_words']:,}")
        print(f"Similarity Pairs:       {self.metrics['similarity_pairs']:,}")

        # Mode-specific metrics
        if self.mode == 'pairwise':
            print(f"Total Possible Pairs:   {self.metrics['total_possible_pairs']:,}")
            print(f"Pairs Computed:         {self.metrics['pairs_computed']:,}")
            print(f"Throughput (Books):     {self.metrics['throughput_books_per_sec']:.4f} books/sec")
            print(f"Throughput (Pairs):     {self.metrics['throughput_pairs_per_sec']:.2f} pairs/sec")
        else:
            print(f"Throughput:             {self.metrics['throughput_books_per_sec']:.4f} books/sec")

        print(f"")
        print(f"Spark Config:")
        print(f"  Executors:            {self.metrics['num_executors']}")
        print(f"  Executor Memory:      {self.metrics['executor_memory']}")
        print(f"  Driver Memory:        {self.args.driver_memory}")
        print("="*80)

    def run(self):
        """Execute the complete Spark pipeline"""
        try:
            # Validate input directory
            print(f"\n[INFO] Validating HDFS input directory: {self.args.input_dir}")
            if not self.validate_hdfs_path(self.args.input_dir):
                raise Exception(f"Input directory does not exist in HDFS: {self.args.input_dir}")

            # Handle query file for JPII mode
            query_file_path = None
            should_cleanup = False

            if self.mode == 'jpii':
                # Create or get query file
                query_file_path, should_cleanup, query_content = self.create_query_file()
                # Store query content for metrics
                self.query_content = query_content

            try:
                # Run Stage 1: Inverted Index
                self.run_inverted_index()

                # Run Stage 2: Mode-specific similarity computation
                if self.mode == 'jpii':
                    self.run_jpii(query_file_path)
                elif self.mode == 'pairwise':
                    self.run_pairwise()

                # Calculate totals
                self.calculate_totals()

                # Display summary
                self.display_summary()

                # Save metrics to CSV
                self.save_metrics_to_csv()

                print(f"\n[SUCCESS] Pipeline completed successfully!")
                print(f"[INFO] Results available at: {self.stage2_output}")

            finally:
                # Clean up temporary query file only if we created it (JPII mode)
                if should_cleanup and query_file_path and os.path.exists(query_file_path):
                    os.remove(query_file_path)
                    print(f"[INFO] Cleaned up temporary query file: {query_file_path}")

        except Exception as e:
            print(f"\n[FATAL ERROR] Pipeline failed: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Execute Spark pipeline for inverted index and similarity search',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # JPII mode with query string
  python3 run_spark_pipeline.py --mode jpii --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query "wildlife conservation hunting animals"

  # JPII mode with query file
  python3 run_spark_pipeline.py --mode jpii --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query-file /path/to/query.txt

  # Pairwise mode (no query needed)
  python3 run_spark_pipeline.py --mode pairwise --num-books 50 \\
      --input-dir hdfs:///gutenberg-input-50

  # With custom Spark configuration
  python3 run_spark_pipeline.py --mode jpii --num-books 200 \\
      --input-dir hdfs:///gutenberg-input-200 \\
      --query "science technology innovation" \\
      --num-executors 8 \\
      --executor-memory 8G
        """
    )

    # Required arguments
    parser.add_argument('--mode', type=str, choices=['jpii', 'pairwise'], default='jpii',
                        help='Pipeline mode: jpii (query-based) or pairwise (all-pairs) (default: jpii)')
    parser.add_argument('--num-books', type=int, required=True,
                        help='Number of books in the dataset')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='HDFS input directory containing text files (e.g., hdfs:///gutenberg-input-100)')

    # Query arguments (mutually exclusive - required only for jpii mode)
    query_group = parser.add_mutually_exclusive_group(required=False)
    query_group.add_argument('--query', type=str,
                            help='Query text for JPII similarity search (e.g., "wildlife conservation hunting")')
    query_group.add_argument('--query-file', type=str,
                            help='Path to local file containing query text (for JPII mode)')

    # Optional arguments with defaults
    parser.add_argument('--index-output', type=str, default=None,
                        help='HDFS output directory for inverted index (default: hdfs:///spark-index-{num_books})')
    parser.add_argument('--stage2-output', type=str, default=None,
                        help='HDFS output directory for stage 2 results (default: hdfs:///spark-{mode}-{num_books})')

    # Spark configuration
    parser.add_argument('--num-executors', type=int, default=4,
                        help='Number of Spark executors (default: 4)')
    parser.add_argument('--executor-memory', type=str, default='4G',
                        help='Memory per executor (default: 4G)')
    parser.add_argument('--driver-memory', type=str, default='2G',
                        help='Driver memory (default: 2G)')

    args = parser.parse_args()

    # Validate mode-specific requirements
    if args.mode == 'jpii' and not args.query and not args.query_file:
        parser.error("--mode jpii requires either --query or --query-file")

    # Set default output paths if not provided
    if args.index_output is None:
        args.index_output = f"hdfs:///spark-index-{args.num_books}"
    if args.stage2_output is None:
        args.stage2_output = f"hdfs:///spark-{args.mode}-{args.num_books}"

    # Run the pipeline
    runner = SparkPipelineRunner(args)
    runner.run()


if __name__ == '__main__':
    main()
