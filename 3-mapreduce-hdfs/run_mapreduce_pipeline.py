#!/usr/bin/env python3
"""
MapReduce Pipeline Execution and Metrics Collection Script

This script executes the MapReduce-based inverted index and similarity search pipeline,
measures performance metrics, and saves results to mode-specific CSV files.

Supports two modes:
  - jpii: Query-based similarity (requires --query or --query-file)
  - pairwise: All-pairs similarity (query file passed but ignored by mapper)

Usage:
    # JPII mode with query string:
    python3 run_mapreduce_pipeline.py --mode jpii --num-books 100 \
        --input-dir /gutenberg-input-100 \
        --query "wildlife conservation hunting animals"

    # JPII mode with query file:
    python3 run_mapreduce_pipeline.py --mode jpii --num-books 100 \
        --input-dir /gutenberg-input-100 \
        --query-file /path/to/query.txt

    # Pairwise mode (no query needed):
    python3 run_mapreduce_pipeline.py --mode pairwise --num-books 100 \
        --input-dir /gutenberg-input-100 \
        --query-file my_query.txt
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
        self.mode = args.mode
        self.metrics = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mode': args.mode,
            'num_books': args.num_books,
            'num_reducers': args.num_reducers,
        }

        # Generate unique output paths based on timestamp
        timestamp_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.index_output = f"{args.index_output}_{timestamp_suffix}" if not args.index_output.endswith(timestamp_suffix) else args.index_output
        self.stage2_output = f"{args.stage2_output}_{timestamp_suffix}" if not args.stage2_output.endswith(timestamp_suffix) else args.stage2_output
        self.ranking_search_output = f"{args.ranking_search_output}_{timestamp_suffix}" if not args.ranking_search_output.endswith(timestamp_suffix) else args.ranking_search_output

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

    def run_command(self, cmd, description, printinfo):
        """Execute a shell command and return output"""
        if printinfo == "":
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
        output = self.run_command(cmd, f"Getting size of {hdfs_path}","")

        if output and output.isdigit():
            size_bytes = int(output)
            size_mb = size_bytes / (1024 * 1024)
            return round(size_mb, 2)
        return 0.0

    def count_hdfs_lines(self, hdfs_path):
        """Count number of lines in HDFS file(s)"""
        cmd = f"hdfs dfs -cat {hdfs_path}/part-* 2>/dev/null | wc -l"
        output = self.run_command(cmd, f"Counting lines in {hdfs_path}","")

        if output and output.isdigit():
            return int(output)
        return 0

    def count_hdfs_files(self, hdfs_path):
        """Count number of files in HDFS directory"""
        cmd = f"hdfs dfs -ls {hdfs_path} 2>/dev/null | grep -v '^d' | wc -l"
        output = self.run_command(cmd, f"Counting files in {hdfs_path}","")

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
        result = self.run_command(cmd, "Running MapReduce Inverted Index job","")
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
            # Construct Hadoop streaming command with query file
            cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
                -D mapreduce.job.name="JPII_Similarity_Stage2" \\
                -D mapreduce.job.reduces={self.args.num_reducers} \\
                -files {mapper_script},{reducer_script},{query_file_final} \\
                -mapper {os.path.basename(mapper_script)} \\
                -reducer {os.path.basename(reducer_script)} \\
                -cmdenv q_from_user={os.path.basename(query_file_final)} \\
                -input {self.index_output} \\
                -output {self.stage2_output}"""

            start_time = time.time()
            result = self.run_command(cmd, "Running MapReduce JPII similarity search","")
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
        self.metrics['output_size_mb'] = self.get_hdfs_size_mb(self.stage2_output)
        self.metrics['similarity_pairs'] = self.count_hdfs_lines(self.stage2_output)

        print(f"[METRIC] Output size: {self.metrics['output_size_mb']:.2f} MB")
        print(f"[METRIC] Similarity pairs: {self.metrics['similarity_pairs']}")

    def run_pairwise(self, query_content):
        """Execute Stage 2: Pairwise Similarity with MapReduce"""
        print("\n" + "="*80)
        print("STAGE 2: Computing All-Pairs Document Similarity with MapReduce (Pairwise)")
        print("="*80)

        mapper_script = os.path.join(self.script_dir, 'pairwise_mapper.py')
        reducer_script = os.path.join(self.script_dir, 'jpii_reducer.py')  # Uses same reducer

        # Create query file for consistency (though pairwise_mapper ignores it)
        query_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_query_temp.txt', dir=self.script_dir)
        query_file_path = query_file.name
        query_file.write(query_content if query_content else "dummy query for pairwise")
        query_file.close()

        query_file_final = os.path.join(self.script_dir, 'query_temp.txt')
        if os.path.exists(query_file_final):
            os.remove(query_file_final)
        os.rename(query_file_path, query_file_final)

        print(f"[INFO] Query file created (not used by pairwise_mapper): {query_file_final}")

        try:
            # Construct Hadoop streaming command
            cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
                -D mapreduce.job.name="Pairwise_Similarity_Stage2" \\
                -D mapreduce.job.reduces={self.args.num_reducers} \\
                -files {mapper_script},{reducer_script},{query_file_final} \\
                -mapper {os.path.basename(mapper_script)} \\
                -reducer {os.path.basename(reducer_script)} \\
                -cmdenv q_from_user={os.path.basename(query_file_final)} \\
                -cmdenv num_docs={self.args.num_books} \\
                -input {self.index_output} \\
                -output {self.stage2_output}"""

            start_time = time.time()
            result = self.run_command(cmd, "Running MapReduce Pairwise all-pairs similarity","")
            stage2_time = time.time() - start_time

            if result is None:
                raise Exception("Stage 2 (Pairwise) failed")
        finally:
            # Clean up temporary query file
            if os.path.exists(query_file_final):
                os.remove(query_file_final)
                print(f"[INFO] Cleaned up query file: {query_file_final}")

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
            self.metrics['stage1_time_sec'] + self.metrics['stage2_time_sec'] + self.metrics['stage3_time_sec'],
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
        metrics_dir = os.path.join(self.script_dir, 'metrics')
        os.makedirs(metrics_dir, exist_ok=True)

        # Choose CSV file based on mode
        if self.mode == 'jpii':
            csv_file = os.path.join(metrics_dir, 'mapreduce_jpii_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'stage1_time_sec',
                'stage2_time_sec',
                'stage3_time_sec',
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
            # Add JPII-specific fields
            self.metrics['jpii_output'] = self.stage2_output

        elif self.mode == 'pairwise':
            csv_file = os.path.join(metrics_dir, 'idf_mapreduce_pairwise_metrics.csv')
            fieldnames = [
                'timestamp',
                'mode',
                'num_books',
                'total_possible_pairs',
                'stage1_time_sec',
                'stage2_time_sec',
                'stage3_time_sec',
                'total_time_sec',
                'input_size_mb',
                'index_size_mb',
                'output_size_mb',
                'unique_words',
                'similarity_pairs',
                'pairs_computed',
                'throughput_books_per_sec',
                'throughput_pairs_per_sec',
                'num_reducers',
                'input_dir',
                'index_output',
                'pairwise_output'
            ]
            # Add pairwise-specific fields
            self.metrics['pairwise_output'] = self.stage2_output

        file_exists = os.path.exists(csv_file)

        # Add common fields to metrics
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
        print(f"Stage 3 (Search):       {self.metrics['stage3_time_sec']:.2f} sec")
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
        print(f"MapReduce Config:")
        print(f"  Reducers:             {self.metrics['num_reducers']}")
        print("="*80)
        
    def ranking_search(self):
        """Execute Stage 3: Ranking Search based on Similarity with MapReduce"""
        print("\n" + "="*80)
        print("STAGE 3: Ranking Search based on Similarity with MapReduce")
        print("="*80)

        ranking_search_script = os.path.join(self.script_dir, 'ranking_search.py')
        try:
            # Construct Hadoop streaming command
            cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
                -D mapreduce.job.name="Ranking_Search_Stage3" \\
                -D mapreduce.job.reduces=1 \\
                -files {ranking_search_script} \\
                -mapper /bin/cat \\
                -reducer "python3 {os.path.basename(ranking_search_script)}" \\
                -cmdenv options={self.mode} \\
                -input {self.stage2_output} \\
                -output {self.ranking_search_output}"""

            start_time = time.time()
            result = self.run_command(cmd, "Running MapReduce Ranking Search similarity","")
            stage3_time = time.time() - start_time

            if result is None:
                raise Exception("Stage 2 (Pairwise) failed")

            self.metrics['stage3_time_sec'] = round(stage3_time, 2)
            print(f"[SUCCESS] Stage 3 completed in {stage3_time:.2f} seconds")
        except Exception as e:
            print(f"[ERROR] Ranking Search has error: {e}")
            raise
            
        print("\n## 🏆 Top Ebooks Ranking 🏆", file=sys.stderr)
        print("--------------------------------------------------------------------------------------------", file=sys.stderr)
        print("{:<5} | {:<30} | {:<10} | {:<10} | {:<10} | {:<12} ".format(
            "Rank", "Ebook Name", "Jaccard", "F1-Score", "Overlap", "Shared Terms"
        ), file=sys.stderr)
        print("--------------------------------------------------------------------------------------------", file=sys.stderr)
        outputfile = "part-00000"
        outputpath = os.path.join(self.ranking_search_output,outputfile) 
        output = subprocess.check_output(["hdfs", "dfs", "-cat", outputpath], text=True)
        print(output)
        print("--------------------------------------------------------------------------------------------", file=sys.stderr)
        
        

    def run(self):
        """Execute the complete MapReduce pipeline"""
        try:
            # Validate input directory
            print(f"\n[INFO] Validating HDFS input directory: {self.args.input_dir}")
            if not self.validate_hdfs_path(self.args.input_dir):
                raise Exception(f"Input directory does not exist in HDFS: {self.args.input_dir}")

            # Read query content (if provided)
            query_content = self.read_query() if (hasattr(self.args, 'query') or hasattr(self.args, 'query_file')) else None

            # Run Stage 1: Inverted Index
            self.run_inverted_index()

            # Run Stage 2: Mode-specific similarity computation
            if self.mode == 'jpii':
                self.run_jpii(query_content)
            elif self.mode == 'pairwise':
                self.run_pairwise(query_content)
                
            #Raning search
            self.ranking_search()

            # Calculate totals
            self.calculate_totals()
            
            # Display summary
            self.display_summary()

            # Save metrics to CSV
            #self.save_metrics_to_csv()

            print(f"\n[SUCCESS] Pipeline completed successfully!")
            print(f"[INFO] Results available at: {self.stage2_output}")
            print(f"\n[INFO] To view top results, run:")
            print(f"       hdfs dfs -cat {self.stage2_output}/part-* | sort -t$'\\t' -k5 -rn | head -20")

        except Exception as e:
            print(f"\n[FATAL ERROR] Pipeline failed: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Execute MapReduce pipeline for inverted index and similarity search',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # JPII mode with query string
  python3 run_mapreduce_pipeline.py --mode jpii --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query "wildlife conservation hunting animals"

  # JPII mode with query file
  python3 run_mapreduce_pipeline.py --mode jpii --num-books 100 \\
      --input-dir hdfs:///gutenberg-input-100 \\
      --query-file my_query.txt

  # Pairwise mode (no query needed)
  python3 run_mapreduce_pipeline.py --mode pairwise --num-books 50 \\
      --input-dir hdfs:///gutenberg-input-50 \\
      --query-file my_query.txt

  # With custom MapReduce configuration
  python3 run_mapreduce_pipeline.py --mode jpii --num-books 200 \\
      --input-dir hdfs:///gutenberg-input-200 \\
      --query "science technology innovation" \\
      --num-reducers 12
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
                        help='HDFS output directory for inverted index (default: hdfs:///mapreduce-index-{num_books})')
    parser.add_argument('--stage2-output', type=str, default=None,
                        help='HDFS output directory for stage 2 results (default: hdfs:///mapreduce-{mode}-{num_books})')
    parser.add_argument('--ranking-search-output', type=str, default=None,
                        help='HDFS ranking search directory for stage 3 results (default: hdfs:///ranking-search-{args.mode}-{args.num_books})')

    # MapReduce configuration
    parser.add_argument('--num-reducers', type=int, default=None,
                        help='Number of reducers (default: auto-scale based on num-books and cluster size)')

    args = parser.parse_args()

    # Validate mode-specific requirements
    if args.mode == 'jpii' and not args.query and not args.query_file:
        parser.error("--mode jpii requires either --query or --query-file")

    # Set default output paths if not provided
    if args.index_output is None:
        args.index_output = f"hdfs:///mapreduce-index-{args.num_books}"
    if args.stage2_output is None:
        args.stage2_output = f"hdfs:///mapreduce-{args.mode}-{args.num_books}"
    if args.ranking_search_output is None:
        args.ranking_search_output = f"hdfs:///ranking-search-{args.mode}-{args.num_books}"

    # Auto-scale reducers based on dataset size and 3-node cluster (12 vCPU total)
    if args.num_reducers is None:
        if args.num_books <= 10:
            args.num_reducers = 4
        elif args.num_books <= 50:
            args.num_reducers = 6
        elif args.num_books <= 100:
            args.num_reducers = 8
        else:
            args.num_reducers = 12
        print(f"[INFO] Auto-scaled reducers to {args.num_reducers} based on {args.num_books} books (3-node cluster)")

    # Run the pipeline
    runner = MapReducePipelineRunner(args)
    runner.run()


if __name__ == '__main__':
    main()
