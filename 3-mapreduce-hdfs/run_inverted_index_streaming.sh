#!/bin/bash
#
# Standalone script to run the Inverted Index MapReduce job (Stage 1 only)
#
# Usage:
#   ./run_inverted_index_streaming.sh <input_dir> <output_dir> [num_reducers]
#
# Example:
#   ./run_inverted_index_streaming.sh hdfs:///gutenberg-input-100 hdfs:///mapreduce-index-100 4
#

set -e  # Exit on error

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_dir> <output_dir> [num_reducers]"
    echo ""
    echo "Arguments:"
    echo "  input_dir      HDFS directory containing text files"
    echo "  output_dir     HDFS output directory for inverted index"
    echo "  num_reducers   Number of reducers (default: 4)"
    echo ""
    echo "Example:"
    echo "  $0 hdfs:///gutenberg-input-100 hdfs:///mapreduce-index-100 4"
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"
NUM_REDUCERS="${3:-4}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find Hadoop streaming jar
HADOOP_STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

if [ ! -f "$HADOOP_STREAMING_JAR" ]; then
    echo "[ERROR] Hadoop streaming jar not found at: $HADOOP_STREAMING_JAR"
    echo "[INFO] Trying to locate streaming jar..."

    # Try to find it using hadoop classpath
    FOUND_JAR=$(hadoop classpath | tr ':' '\n' | grep streaming | head -1)
    if [ -n "$FOUND_JAR" ] && [ -f "$FOUND_JAR" ]; then
        HADOOP_STREAMING_JAR="$FOUND_JAR"
        echo "[INFO] Found streaming jar at: $HADOOP_STREAMING_JAR"
    else
        echo "[ERROR] Could not locate Hadoop streaming jar"
        exit 1
    fi
fi

# Validate input directory
echo "[INFO] Validating HDFS input directory: $INPUT_DIR"
if ! hdfs dfs -test -e "$INPUT_DIR"; then
    echo "[ERROR] Input directory does not exist in HDFS: $INPUT_DIR"
    exit 1
fi

# Remove output directory if it exists
if hdfs dfs -test -e "$OUTPUT_DIR"; then
    echo "[WARN] Output directory already exists, removing: $OUTPUT_DIR"
    hdfs dfs -rm -r -f "$OUTPUT_DIR"
fi

echo ""
echo "========================================================================"
echo "STAGE 1: Building Inverted Index with MapReduce"
echo "========================================================================"
echo "Input:      $INPUT_DIR"
echo "Output:     $OUTPUT_DIR"
echo "Reducers:   $NUM_REDUCERS"
echo "Mapper:     inverted_index_mapper.py"
echo "Reducer:    inverted_index_reducer.py"
echo "========================================================================"
echo ""

# Run MapReduce job
START_TIME=$(date +%s)

hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.name="Inverted_Index_Stage1" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -files "$SCRIPT_DIR/inverted_index_mapper.py","$SCRIPT_DIR/inverted_index_reducer.py" \
    -mapper inverted_index_mapper.py \
    -reducer inverted_index_reducer.py \
    -input "$INPUT_DIR" \
    -output "$OUTPUT_DIR"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo ""
echo "========================================================================"
echo "[SUCCESS] Inverted Index job completed in $ELAPSED seconds"
echo "========================================================================"
echo ""
echo "To view results:"
echo "  hdfs dfs -cat $OUTPUT_DIR/part-* | head -20"
echo ""
echo "To count unique words:"
echo "  hdfs dfs -cat $OUTPUT_DIR/part-* | wc -l"
echo ""
