#!/usr/bin/env python3
"""
MapReduce Pipeline Execution and Metrics Collection Script

This script executes the MapReduce-based inverted index and similarity search pipeline,
measures performance metrics, and saves results to a CSV file.

Usage:
    # With query string:
    python3 run_mapreduce_pipeline.py --num-books 100 --input-dir /gutenberg-input-100 \
        --query "wildlife conservation hunting animals"

    # With query file:
    python3 run_mapreduce_pipeline.py --num-books 100 --input-dir /gutenberg-input-100 \
        --query-file /path/to/query.txt
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


class MapReducePipelineRunner:
    """Manages MapReduce pipeline execution and metrics collection"""

    def __init__(self, args):
        self.args = args
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.metrics = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'num_books': args.num_books,
            'num_reducers': args.num_reducers,
        }

        # Generate unique output paths based on timestamp
        timestamp_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.index_output = f"{args.index_output}_{timestamp_suffix}" if not args.index_output.endswith(timestamp_suffix) else args.index_output
        self.jpii_output = f"{args.jpii_output}_{timestamp_suffix}" if not args.jpii_output.endswith(timestamp_suffix) else args.jpii_output

        # Find Hadoop streaming jar
        self.hadoop_streaming_jar = self.find_hadoop_streaming_jar()

    def find_hadoop_streaming_jar(self):
        """Find the Hadoop streaming jar file"""
        # Common locations for Hadoop streaming jar
        possible_paths = [
            "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar",
            "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar",
            "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar",
            "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
        ]

        # Try to find using hadoop classpath
        try:
            result = subprocess.run(
                "hadoop classpath | tr ':' '\n' | grep streaming",
                shell=True,
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                jar_path = result.stdout.strip().split('\n')[0]
                if os.path.exists(jar_path):
                    return jar_path
        except:
            pass

        # Check common paths
        for path_pattern in possible_paths:
            # Expand environment variables
            path = os.path.expandvars(path_pattern)

            # Handle wildcards
            if '*' in path:
                import glob
                matches = glob.glob(path)
                if matches:
                    return matches[0]
            elif os.path.exists(path):
                return path

        # Default to standard path
        return "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

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

    def count_hdfs_files(self, hdfs_path):
        """Count number of files in HDFS directory"""
        cmd = f"hdfs dfs -ls {hdfs_path} 2>/dev/null | grep -v '^d' | wc -l"
        output = self.run_command(cmd, f"Counting files in {hdfs_path}")

        if output and output.isdigit():
            count = int(output)
            # Subtract 1 for the header line "Found X items"
            return max(0, count - 1) if count > 0 else 0
        return 0

    def validate_hdfs_path(self, hdfs_path):
        """Check if HDFS path exists"""
        cmd = f"hdfs dfs -test -e {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0

    def read_query(self):
        """Read query from file or use query string"""
        # If query file is provided, read it
        if hasattr(self.args, 'query_file') and self.args.query_file:
            query_file_path = self.args.query_file

            # Validate file exists
            if not os.path.exists(query_file_path):
                raise Exception(f"Query file not found: {query_file_path}")

            # Read query content
            with open(query_file_path, 'r') as f:
                query_content = f.read().strip()

            print(f"[INFO] Using query file: {query_file_path}")
            print(f"[INFO] Query content: {query_content}")

            return query_content

        # Otherwise, use query string
        else:
            print(f"[INFO] Query text: {self.args.query}")
            return self.args.query

    def run_inverted_index(self):
        """Execute Stage 1: Inverted Index with MapReduce"""
        print("\n" + "="*80)
        print("STAGE 1: Building Inverted Index with MapReduce")
        print("="*80)

        mapper_script = os.path.join(self.script_dir, 'inverted_index_mapper.py')
        reducer_script = os.path.join(self.script_dir, 'inverted_index_reducer.py')

        # Count number of input files for total_map_tasks
        num_files = self.count_hdfs_files(self.args.input_dir)
        print(f"[INFO] Input files: {num_files}")

        # Construct Hadoop streaming command
        cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
            -D mapreduce.job.name="Inverted_Index_Stage1" \\
            -D mapreduce.job.reduces={self.args.num_reducers} \\
            -files {mapper_script},{reducer_script} \\
            -mapper {os.path.basename(mapper_script)} \\
            -reducer {os.path.basename(reducer_script)} \\
            -input {self.args.input_dir} \\
            -output {self.index_output}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running MapReduce Inverted Index job")
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

    def run_jpii(self, query_content):
        """Execute Stage 2: JPII Similarity Search with MapReduce"""
        print("\n" + "="*80)
        print("STAGE 2: Computing Document Similarity with MapReduce (JPII)")
        print("="*80)

        mapper_script = os.path.join(self.script_dir, 'jpii_mapper.py')
        reducer_script = os.path.join(self.script_dir, 'jpii_reducer.py')

        # Create temporary query file to avoid command-line length limits
        query_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_query_temp.txt', dir=self.script_dir)
        query_file_path = query_file.name
        query_file.write(query_content)
        query_file.close()

        # Rename to query_temp.txt for consistency
        query_file_final = os.path.join(self.script_dir, 'query_temp.txt')
        if os.path.exists(query_file_final):
            os.remove(query_file_final)
        os.rename(query_file_path, query_file_final)

        print(f"[INFO] Created query file: {query_file_final}")

        try:
            # Construct Hadoop streaming command with query file instead of cmdenv
            cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
                -D mapreduce.job.name="JPII_Similarity_Stage2" \\
                -D mapreduce.job.reduces={self.args.num_reducers} \\
                -files {mapper_script},{reducer_script},{query_file_final} \\
                -mapper {os.path.basename(mapper_script)} \\
                -reducer {os.path.basename(reducer_script)} \\
                -input {self.index_output} \\
                -output {self.jpii_output}"""

            start_time = time.time()
            result = self.run_command(cmd, "Running MapReduce JPII similarity search")
            stage2_time = time.time() - start_time

            if result is None:
                raise Exception("Stage 2 (JPII) failed")
        finally:
            # Clean up temporary query file
            if os.path.exists(query_file_final):
                os.remove(query_file_final)
                print(f"[INFO] Cleaned up query file: {query_file_final}")

        self.metrics['stage2_time_sec'] = round(stage2_time, 2)
        print(f"[SUCCESS] Stage 2 completed in {stage2_time:.2f} seconds")

        # Collect Stage 2 metrics
        print("\n[INFO] Collecting Stage 2 metrics...")
        self.metrics['output_size_mb'] = self.get_hdfs_size_mb(self.jpii_output)
        self.metrics['similarity_pairs'] = self.count_hdfs_lines(self.jpii_output)

        print(f"[METRIC] Output size: {self.metrics['output_size_mb']:.2f} MB")
        print(f"[METRIC] Similarity pairs: {self.metrics['similarity_pairs']}")

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

    def save_metrics_to_csv(self):
        """Save metrics to CSV file"""
        csv_file = os.path.join(self.script_dir, 'mapreduce_metrics.csv')
        file_exists = os.path.exists(csv_file)

        # Define CSV columns (matching Spark version format)
        fieldnames = [
            'timestamp',
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
            'num_reducers',
            'input_dir',
            'index_output',
            'jpii_output'
        ]

        # Add additional fields to metrics
        self.metrics['input_dir'] = self.args.input_dir
        self.metrics['index_output'] = self.index_output
        self.metrics['jpii_output'] = self.jpii_output

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
        print(f"Throughput:             {self.metrics['throughput_books_per_sec']:.4f} books/sec")
        print(f"")
        print(f"MapReduce Config:")
        print(f"  Reducers:             {self.metrics['num_reducers']}")
        print("="*80)

    def run(self):
        """Execute the complete MapReduce pipeline"""
        try:
            # Validate input directory
            print(f"\n[INFO] Validating HDFS input directory: {self.args.input_dir}")
            if not self.validate_hdfs_path(self.args.input_dir):
                raise Exception(f"Input directory does not exist in HDFS: {self.args.input_dir}")

            # Read query content
            query_content = self.read_query()

            # Run Stage 1: Inverted Index
            self.run_inverted_index()

            # Run Stage 2: JPII Similarity Search
            self.run_jpii(query_content)

            # Calculate totals
            self.calculate_totals()

            # Display summary
            self.display_summary()

            # Save metrics to CSV
            self.save_metrics_to_csv()

            print(f"\n[SUCCESS] Pipeline completed successfully!")
            print(f"[INFO] Results available at: {self.jpii_output}")
            print(f"\n[INFO] To view top results, run:")
            print(f"       hdfs dfs -cat {self.jpii_output}/part-* | sort -t$'\\t' -k2 -rn | head -20")

        except Exception as e:
            print(f"\n[FATAL ERROR] Pipeline failed: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Execute MapReduce pipeline for inverted index and similarity search',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with query string
  python3 run_mapreduce_pipeline.py --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query "wildlife conservation hunting animals"

  # Run with query file
  python3 run_mapreduce_pipeline.py --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query-file /path/to/query.txt

  # Run with custom MapReduce configuration
  python3 run_mapreduce_pipeline.py --num-books 200 \\
      --input-dir hdfs:///gutenberg-input-200 \\
      --query "science technology innovation" \\
      --num-reducers 8
        """
    )

    # Required arguments
    parser.add_argument('--num-books', type=int, required=True,
                        help='Number of books in the dataset')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='HDFS input directory containing text files (e.g., hdfs:///gutenberg-input-100)')

    # Query arguments (mutually exclusive - one required)
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument('--query', type=str,
                            help='Query text for similarity search (e.g., "wildlife conservation hunting")')
    query_group.add_argument('--query-file', type=str,
                            help='Path to local file containing query text')

    # Optional arguments with defaults
    parser.add_argument('--index-output', type=str, default=None,
                        help='HDFS output directory for inverted index (default: hdfs:///mapreduce-index-{num_books})')
    parser.add_argument('--jpii-output', type=str, default=None,
                        help='HDFS output directory for JPII results (default: hdfs:///mapreduce-jpii-{num_books})')

    # MapReduce configuration
    parser.add_argument('--num-reducers', type=int, default=None,
                        help='Number of reducers (default: auto-scale based on num-books)')

    args = parser.parse_args()

    # Set default output paths if not provided
    if args.index_output is None:
        args.index_output = f"hdfs:///mapreduce-index-{args.num_books}"
    if args.jpii_output is None:
        args.jpii_output = f"hdfs:///mapreduce-jpii-{args.num_books}"

    # Auto-scale reducers based on dataset size if not specified
    if args.num_reducers is None:
        if args.num_books <= 10:
            args.num_reducers = 2
        elif args.num_books <= 100:
            args.num_reducers = 4
        else:
            args.num_reducers = 8
        print(f"[INFO] Auto-scaled reducers to {args.num_reducers} based on {args.num_books} books")

    # Run the pipeline
    runner = MapReducePipelineRunner(args)
    runner.run()


if __name__ == '__main__':
    main()
