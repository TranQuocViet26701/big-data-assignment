#!/usr/bin/env python3
"""
Spark HBase Pipeline Execution and Metrics Collection Script

This script executes the Spark-HBase pipeline for inverted index and similarity search,
using HBase as the storage backend instead of HDFS.

Supports two modes:
  - jpii: Query-based similarity (requires --query-file)
  - pairwise: All-pairs similarity (no query needed)

Usage:
    # JPII mode:
    python3 run_spark_hbase_pipeline.py --mode jpii --num-books 100 \\
        --input-dir hdfs:///gutenberg-input-100 \\
        --query-file /path/to/query.txt \\
        --thrift-host localhost --thrift-port 9090

    # Pairwise mode:
    python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 100 \\
        --input-dir hdfs:///gutenberg-input-100 \\
        --thrift-host localhost --thrift-port 9090
"""

import argparse
import subprocess
import time
import csv
import os
import sys
from datetime import datetime


class SparkHBasePipelineRunner:
    """Manages Spark-HBase pipeline execution and metrics collection"""

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
            'thrift_host': args.thrift_host,
            'thrift_port': args.thrift_port,
        }

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

    def validate_hdfs_path(self, hdfs_path):
        """Check if HDFS path exists"""
        cmd = f"hdfs dfs -test -e {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0

    def check_hbase_connection(self):
        """Test HBase Thrift server connection"""
        print(f"\n[INFO] Testing HBase connection to {self.args.thrift_host}:{self.args.thrift_port}")

        # Use hbase_connector to test connection
        test_script = os.path.join(self.script_dir, 'hbase_connector.py')

        try:
            import sys
            sys.path.append(self.script_dir)
            from hbase_connector import test_connection

            success = test_connection(self.args.thrift_host, self.args.thrift_port)

            if not success:
                raise Exception(f"Cannot connect to HBase Thrift server at {self.args.thrift_host}:{self.args.thrift_port}")

            print("[SUCCESS] HBase connection verified")
            return True

        except Exception as e:
            print(f"[ERROR] HBase connection failed: {e}")
            return False

    def get_hbase_metrics(self):
        """Get metrics from HBase tables"""
        print("\n[INFO] Collecting HBase metrics...")

        try:
            import sys
            sys.path.append(self.script_dir)
            from hbase_connector import HBaseConnector

            connector = HBaseConnector(
                host=self.args.thrift_host,
                port=self.args.thrift_port
            )

            # Get inverted index metrics
            if connector.table_exists('inverted_index'):
                index_count = connector.get_row_count('inverted_index')
                self.metrics['unique_terms'] = index_count
                print(f"[METRIC] Unique terms in HBase: {index_count:,}")
            else:
                self.metrics['unique_terms'] = 0
                print("[WARNING] inverted_index table not found")

            # Get similarity scores metrics
            if connector.table_exists('similarity_scores'):
                similarity_count = connector.get_row_count('similarity_scores')
                self.metrics['similarity_pairs'] = similarity_count
                print(f"[METRIC] Similarity pairs in HBase: {similarity_count:,}")
            else:
                self.metrics['similarity_pairs'] = 0
                print("[WARNING] similarity_scores table not found")

            connector.close()

        except Exception as e:
            print(f"[WARNING] Could not collect HBase metrics: {e}")
            self.metrics['unique_terms'] = 0
            self.metrics['similarity_pairs'] = 0

    def run_inverted_index(self):
        """Execute Stage 1: Inverted Index with Spark-HBase"""
        print("\n" + "="*80)
        print("STAGE 1: Building Inverted Index in HBase with Spark")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_hbase_inverted_index.py')
        hbase_connector = os.path.join(self.script_dir, 'hbase_connector.py')
        happybase_deps = os.path.join(self.script_dir, 'happybase_deps.zip')

        # Use hadoop-master for Thrift host to ensure worker nodes can connect
        thrift_host = 'hadoop-master' if self.args.thrift_host == 'localhost' else self.args.thrift_host

        # Construct spark-submit command - use client mode with py-files and archives
        # Include optimizations for 6 executors × 6GB configuration
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode client \\
            --num-executors {self.args.num_executors} \\
            --executor-cores {self.args.executor_cores} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            --conf spark.executor.memoryOverhead={self.args.memory_overhead} \\
            --conf spark.default.parallelism={self.args.default_parallelism} \\
            --conf spark.sql.shuffle.partitions={self.args.shuffle_partitions} \\
            --conf spark.memory.fraction=0.8 \\
            --conf spark.memory.storageFraction=0.3 \\
            --conf spark.shuffle.compress=true \\
            --conf spark.shuffle.spill.compress=true \\
            --conf spark.network.timeout=600s \\
            --conf spark.executor.heartbeatInterval=60s \\
            --conf spark.yarn.appMasterEnv.PYTHONPATH=happybase_deps.zip \\
            --conf spark.executorEnv.PYTHONPATH=happybase_deps.zip \\
            --py-files {hbase_connector} \\
            --archives {happybase_deps}#happybase_deps.zip \\
            {spark_script} \\
            {self.args.input_dir} \\
            {thrift_host} \\
            {self.args.thrift_port}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark-HBase Inverted Index job")
        stage1_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 1 (Inverted Index) failed")

        self.metrics['stage1_time_sec'] = round(stage1_time, 2)
        print(f"[SUCCESS] Stage 1 completed in {stage1_time:.2f} seconds")

        # Collect Stage 1 metrics
        print("\n[INFO] Collecting Stage 1 metrics...")
        self.metrics['input_size_mb'] = self.get_hdfs_size_mb(self.args.input_dir)
        print(f"[METRIC] Input size: {self.metrics['input_size_mb']:.2f} MB")

    def run_jpii(self, query_file):
        """Execute Stage 2: JPII Similarity Search with Spark-HBase"""
        print("\n" + "="*80)
        print("STAGE 2: Computing Document Similarity with Spark-HBase (JPII)")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_hbase_jpii.py')
        hbase_connector = os.path.join(self.script_dir, 'hbase_connector.py')
        happybase_deps = os.path.join(self.script_dir, 'happybase_deps.zip')

        # Use hadoop-master for Thrift host to ensure worker nodes can connect
        thrift_host = 'hadoop-master' if self.args.thrift_host == 'localhost' else self.args.thrift_host

        # Construct spark-submit command - use client mode with py-files and archives
        # Include optimizations for 6 executors × 6GB configuration
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode client \\
            --num-executors {self.args.num_executors} \\
            --executor-cores {self.args.executor_cores} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            --conf spark.executor.memoryOverhead={self.args.memory_overhead} \\
            --conf spark.default.parallelism={self.args.default_parallelism} \\
            --conf spark.sql.shuffle.partitions={self.args.shuffle_partitions} \\
            --conf spark.memory.fraction=0.8 \\
            --conf spark.memory.storageFraction=0.3 \\
            --conf spark.shuffle.compress=true \\
            --conf spark.shuffle.spill.compress=true \\
            --conf spark.network.timeout=600s \\
            --conf spark.executor.heartbeatInterval=60s \\
            --conf spark.yarn.appMasterEnv.PYTHONPATH=happybase_deps.zip \\
            --conf spark.executorEnv.PYTHONPATH=happybase_deps.zip \\
            --py-files {hbase_connector} \\
            --archives {happybase_deps}#happybase_deps.zip \\
            {spark_script} \\
            {query_file} \\
            {thrift_host} \\
            {self.args.thrift_port}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark-HBase JPII similarity search")
        stage2_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 2 (JPII) failed")

        self.metrics['stage2_time_sec'] = round(stage2_time, 2)
        print(f"[SUCCESS] Stage 2 completed in {stage2_time:.2f} seconds")

        # Query file name for metrics
        self.metrics['query_file'] = os.path.basename(query_file)

    def run_pairwise(self):
        """Execute Stage 2: Pairwise Similarity with Spark-HBase"""
        print("\n" + "="*80)
        print("STAGE 2: Computing All-Pairs Document Similarity with Spark-HBase (Pairwise)")
        print("="*80)

        spark_script = os.path.join(self.script_dir, 'spark_hbase_pairwise.py')
        hbase_connector = os.path.join(self.script_dir, 'hbase_connector.py')
        happybase_deps = os.path.join(self.script_dir, 'happybase_deps.zip')

        # Use hadoop-master for Thrift host to ensure worker nodes can connect
        thrift_host = 'hadoop-master' if self.args.thrift_host == 'localhost' else self.args.thrift_host

        # Construct spark-submit command - use client mode with py-files and archives
        # Include optimizations for 6 executors × 6GB configuration
        cmd = f"""spark-submit \\
            --master yarn \\
            --deploy-mode client \\
            --num-executors {self.args.num_executors} \\
            --executor-cores {self.args.executor_cores} \\
            --executor-memory {self.args.executor_memory} \\
            --driver-memory {self.args.driver_memory} \\
            --conf spark.dynamicAllocation.enabled=false \\
            --conf spark.executor.memoryOverhead={self.args.memory_overhead} \\
            --conf spark.default.parallelism={self.args.default_parallelism} \\
            --conf spark.sql.shuffle.partitions={self.args.shuffle_partitions} \\
            --conf spark.memory.fraction=0.8 \\
            --conf spark.memory.storageFraction=0.3 \\
            --conf spark.shuffle.compress=true \\
            --conf spark.shuffle.spill.compress=true \\
            --conf spark.network.timeout=600s \\
            --conf spark.executor.heartbeatInterval=60s \\
            --conf spark.yarn.appMasterEnv.PYTHONPATH=happybase_deps.zip \\
            --conf spark.executorEnv.PYTHONPATH=happybase_deps.zip \\
            --py-files {hbase_connector} \\
            --archives {happybase_deps}#happybase_deps.zip \\
            {spark_script} \\
            {thrift_host} \\
            {self.args.thrift_port}"""

        start_time = time.time()
        result = self.run_command(cmd, "Running Spark-HBase Pairwise all-pairs similarity")
        stage2_time = time.time() - start_time

        if result is None:
            raise Exception("Stage 2 (Pairwise) failed")

        self.metrics['stage2_time_sec'] = round(stage2_time, 2)
        print(f"[SUCCESS] Stage 2 completed in {stage2_time:.2f} seconds")

        # Calculate total possible pairs for pairwise mode
        total_possible_pairs = (self.args.num_books * (self.args.num_books - 1)) // 2
        self.metrics['total_possible_pairs'] = total_possible_pairs

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
        if self.mode == 'pairwise' and 'similarity_pairs' in self.metrics and self.metrics['similarity_pairs'] > 0:
            self.metrics['throughput_pairs_per_sec'] = round(
                self.metrics['similarity_pairs'] / self.metrics['stage2_time_sec'] if self.metrics['stage2_time_sec'] > 0 else 0,
                2
            )

    def save_metrics_to_csv(self):
        """Save metrics to mode-specific CSV file"""
        metrics_dir = os.path.join(self.script_dir, 'metrics')
        os.makedirs(metrics_dir, exist_ok=True)

        # Choose CSV file based on mode
        if self.mode == 'jpii':
            csv_file = os.path.join(metrics_dir, 'spark_hbase_jpii_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'stage1_time_sec',
                'stage2_time_sec',
                'total_time_sec',
                'input_size_mb',
                'unique_terms',
                'similarity_pairs',
                'throughput_books_per_sec',
                'num_executors',
                'executor_memory',
                'driver_memory',
                'thrift_host',
                'thrift_port',
                'input_dir',
                'query_file'
            ]

        elif self.mode == 'pairwise':
            csv_file = os.path.join(metrics_dir, 'spark_hbase_pairwise_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'total_possible_pairs',
                'stage1_time_sec',
                'stage2_time_sec',
                'total_time_sec',
                'input_size_mb',
                'unique_terms',
                'similarity_pairs',
                'throughput_books_per_sec',
                'throughput_pairs_per_sec',
                'num_executors',
                'executor_memory',
                'driver_memory',
                'thrift_host',
                'thrift_port',
                'input_dir'
            ]

        file_exists = os.path.exists(csv_file)

        # Add common fields to metrics
        self.metrics['driver_memory'] = self.args.driver_memory
        self.metrics['input_dir'] = self.args.input_dir

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
        print(f"HBase Thrift Server:    {self.args.thrift_host}:{self.args.thrift_port}")
        print(f"")
        print(f"Stage 1 (Index):        {self.metrics['stage1_time_sec']:.2f} sec")
        print(f"Stage 2 (Similarity):   {self.metrics['stage2_time_sec']:.2f} sec")
        print(f"Total Pipeline Time:    {self.metrics['total_time_sec']:.2f} sec")
        print(f"")
        print(f"Input Size:             {self.metrics['input_size_mb']:.2f} MB")
        print(f"Unique Terms (HBase):   {self.metrics.get('unique_terms', 0):,}")
        print(f"Similarity Pairs:       {self.metrics.get('similarity_pairs', 0):,}")

        # Mode-specific metrics
        if self.mode == 'pairwise':
            print(f"Total Possible Pairs:   {self.metrics['total_possible_pairs']:,}")
            print(f"Throughput (Books):     {self.metrics['throughput_books_per_sec']:.4f} books/sec")
            if 'throughput_pairs_per_sec' in self.metrics:
                print(f"Throughput (Pairs):     {self.metrics['throughput_pairs_per_sec']:.2f} pairs/sec")
        else:
            print(f"Query File:             {self.metrics.get('query_file', 'N/A')}")
            print(f"Throughput:             {self.metrics['throughput_books_per_sec']:.4f} books/sec")

        print(f"")
        print(f"Spark Config:")
        print(f"  Executors:            {self.metrics['num_executors']}")
        print(f"  Executor Memory:      {self.metrics['executor_memory']}")
        print(f"  Driver Memory:        {self.args.driver_memory}")
        print("="*80)

    def run(self):
        """Execute the complete Spark-HBase pipeline"""
        try:
            # Check HBase connection
            if not self.check_hbase_connection():
                raise Exception("HBase connection test failed")

            # Validate input directory
            print(f"\n[INFO] Validating HDFS input directory: {self.args.input_dir}")
            if not self.validate_hdfs_path(self.args.input_dir):
                raise Exception(f"Input directory does not exist in HDFS: {self.args.input_dir}")

            # Handle query file for JPII mode
            query_file_path = None

            if self.mode == 'jpii':
                # Validate query file
                if not os.path.exists(self.args.query_file):
                    raise Exception(f"Query file not found: {self.args.query_file}")

                query_file_path = self.args.query_file
                print(f"[INFO] Using query file: {query_file_path}")

            # Run Stage 1: Inverted Index
            self.run_inverted_index()

            # Run Stage 2: Mode-specific similarity computation
            if self.mode == 'jpii':
                self.run_jpii(query_file_path)
            elif self.mode == 'pairwise':
                self.run_pairwise()

            # Get HBase metrics
            self.get_hbase_metrics()

            # Calculate totals
            self.calculate_totals()

            # Display summary
            self.display_summary()

            # Save metrics to CSV
            self.save_metrics_to_csv()

            print(f"\n[SUCCESS] Pipeline completed successfully!")
            print(f"[INFO] Results stored in HBase table: similarity_scores")
            print(f"[INFO] Query results using: python3 query_similarity.py --mode {self.mode}")

        except Exception as e:
            print(f"\n[FATAL ERROR] Pipeline failed: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Execute Spark-HBase pipeline for inverted index and similarity search',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # JPII mode
  python3 run_spark_hbase_pipeline.py --mode jpii --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query-file my_query.txt \\
      --thrift-host localhost --thrift-port 9090

  # Pairwise mode
  python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 50 \\
      --input-dir hdfs:///gutenberg-input-50 \\
      --thrift-host localhost --thrift-port 9090

  # With custom Spark configuration
  python3 run_spark_hbase_pipeline.py --mode jpii --num-books 200 \\
      --input-dir hdfs:///gutenberg-input-200 \\
      --query-file query.txt \\
      --thrift-host hbase-server --thrift-port 9090 \\
      --num-executors 8 --executor-memory 8G
        """
    )

    # Required arguments
    parser.add_argument('--mode', type=str, choices=['jpii', 'pairwise'], default='jpii',
                        help='Pipeline mode: jpii (query-based) or pairwise (all-pairs) (default: jpii)')
    parser.add_argument('--num-books', type=int, required=True,
                        help='Number of books in the dataset')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='HDFS input directory containing text files (e.g., hdfs:///gutenberg-input-100)')

    # HBase connection
    parser.add_argument('--thrift-host', type=str, default='localhost',
                        help='HBase Thrift server hostname (default: localhost)')
    parser.add_argument('--thrift-port', type=int, default=9090,
                        help='HBase Thrift server port (default: 9090)')

    # Query file (required for jpii mode)
    parser.add_argument('--query-file', type=str,
                        help='Path to local file containing query text (required for JPII mode)')

    # Spark configuration
    parser.add_argument('--num-executors', type=int, default=6,
                        help='Number of Spark executors (default: 6, optimized for 6x6GB)')
    parser.add_argument('--executor-cores', type=int, default=4,
                        help='Number of cores per executor (default: 4)')
    parser.add_argument('--executor-memory', type=str, default='6G',
                        help='Memory per executor (default: 6G)')
    parser.add_argument('--driver-memory', type=str, default='2G',
                        help='Driver memory (default: 2G)')
    parser.add_argument('--memory-overhead', type=str, default='1G',
                        help='Executor memory overhead (default: 1G)')
    parser.add_argument('--default-parallelism', type=int, default=48,
                        help='Default parallelism (default: 48 = 6 executors × 4 cores × 2)')
    parser.add_argument('--shuffle-partitions', type=int, default=48,
                        help='Shuffle partitions (default: 48)')

    args = parser.parse_args()

    # Validate mode-specific requirements
    if args.mode == 'jpii' and not args.query_file:
        parser.error("--mode jpii requires --query-file")

    # Run the pipeline
    runner = SparkHBasePipelineRunner(args)
    runner.run()


if __name__ == '__main__':
    main()
