#!/bin/bash
#
# Standalone script to run the JPII Similarity Search MapReduce job (Stage 2 only)
#
# Usage:
#   ./run_jpii_streaming.sh <index_dir> <output_dir> <query> [num_reducers]
#
# Examples:
#   # With query string
#   ./run_jpii_streaming.sh hdfs:///mapreduce-index-100 hdfs:///mapreduce-jpii-100 "wildlife conservation hunting" 4
#
#   # With query file
#   ./run_jpii_streaming.sh hdfs:///mapreduce-index-100 hdfs:///mapreduce-jpii-100 query.txt 4
#

set -e  # Exit on error

# Check arguments
if [ $# -lt 3 ]; then
    echo "Usage: $0 <index_dir> <output_dir> <query> [num_reducers]"
    echo ""
    echo "Arguments:"
    echo "  index_dir      HDFS directory containing inverted index from Stage 1"
    echo "  output_dir     HDFS output directory for similarity results"
    echo "  query          Query text string OR path to local query file"
    echo "  num_reducers   Number of reducers (default: 4)"
    echo ""
    echo "Examples:"
    echo "  # With query string"
    echo "  $0 hdfs:///mapreduce-index-100 hdfs:///mapreduce-jpii-100 \"wildlife conservation\" 4"
    echo ""
    echo "  # With query file"
    echo "  $0 hdfs:///mapreduce-index-100 hdfs:///mapreduce-jpii-100 query.txt 4"
    exit 1
fi

INDEX_DIR="$1"
OUTPUT_DIR="$2"
QUERY_INPUT="$3"
NUM_REDUCERS="${4:-4}"

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

# Determine if query is a file or a string
if [ -f "$QUERY_INPUT" ]; then
    # It's a file, read its content
    QUERY_TEXT=$(cat "$QUERY_INPUT")
    echo "[INFO] Using query file: $QUERY_INPUT"
else
    # It's a string
    QUERY_TEXT="$QUERY_INPUT"
    echo "[INFO] Using query string"
fi

echo "[INFO] Query: $QUERY_TEXT"

# Validate index directory
echo "[INFO] Validating HDFS index directory: $INDEX_DIR"
if ! hdfs dfs -test -e "$INDEX_DIR"; then
    echo "[ERROR] Index directory does not exist in HDFS: $INDEX_DIR"
    echo "[INFO] Please run Stage 1 (inverted index) first"
    exit 1
fi

# Remove output directory if it exists
if hdfs dfs -test -e "$OUTPUT_DIR"; then
    echo "[WARN] Output directory already exists, removing: $OUTPUT_DIR"
    hdfs dfs -rm -r -f "$OUTPUT_DIR"
fi

echo ""
echo "========================================================================"
echo "STAGE 2: Computing Document Similarity with MapReduce (JPII)"
echo "========================================================================"
echo "Index:      $INDEX_DIR"
echo "Output:     $OUTPUT_DIR"
echo "Reducers:   $NUM_REDUCERS"
echo "Query:      $QUERY_TEXT"
echo "Mapper:     jpii_mapper.py"
echo "Reducer:    jpii_reducer.py"
echo "========================================================================"
echo ""

# Create temporary query file to avoid command-line length limits
QUERY_FILE="$SCRIPT_DIR/query_temp.txt"
echo "$QUERY_TEXT" > "$QUERY_FILE"
echo "[INFO] Created query file: $QUERY_FILE"

# Run MapReduce job with query passed via file (avoids command-line length limits)
START_TIME=$(date +%s)

hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.name="JPII_Similarity_Stage2" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -files "$SCRIPT_DIR/jpii_mapper.py","$SCRIPT_DIR/jpii_reducer.py","$QUERY_FILE" \
    -mapper jpii_mapper.py \
    -reducer jpii_reducer.py \
    -input "$INDEX_DIR" \
    -output "$OUTPUT_DIR"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# Clean up temporary query file
rm -f "$QUERY_FILE"
echo "[INFO] Cleaned up query file: $QUERY_FILE"

echo ""
echo "========================================================================"
echo "[SUCCESS] JPII Similarity job completed in $ELAPSED seconds"
echo "========================================================================"
echo ""
echo "To view top 20 similar documents (sorted by similarity):"
echo "  hdfs dfs -cat $OUTPUT_DIR/part-* | sort -t\$'\\t' -k2 -rn | head -20"
echo ""
echo "To count total similarity pairs:"
echo "  hdfs dfs -cat $OUTPUT_DIR/part-* | wc -l"
echo ""
