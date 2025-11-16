#!/bin/bash

################################################################################
# HBase-Enabled MapReduce Pipeline for Similarity Search
#
# This script runs the full pipeline but writes results to HBase instead of HDFS:
#   Stage 1: Inverted Index → writes to HBase table 'inverted_index'
#   Stage 2: JPII Similarity → writes to HBase table 'similarity_scores'
#
# Usage:
#   ./run_hbase_pipeline.sh --num-books 100 --reducers 4 --query "animal wildlife"
#
# Parameters:
#   --num-books   Number of books to process (default: 10)
#   --reducers    Number of reducers (default: 4)
#   --query       Search query text (default: from query.txt)
#   --skip-stage1 Skip Stage 1 if inverted index already in HBase
#
# Requirements:
#   - HBase tables created (run ./create_hbase_tables.sh first)
#   - HBase Thrift server running for queries
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default configuration
NUM_BOOKS=10
NUM_REDUCERS=4
QUERY_TEXT=""
SKIP_STAGE1=false
SKIP_DOWNLOAD=false

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --num-books)
            NUM_BOOKS="$2"
            shift 2
            ;;
        --reducers)
            NUM_REDUCERS="$2"
            shift 2
            ;;
        --query)
            QUERY_TEXT="$2"
            shift 2
            ;;
        --skip-stage1)
            SKIP_STAGE1=true
            shift
            ;;
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 --num-books <N> --reducers <R> --query <text>"
            echo ""
            echo "Parameters:"
            echo "  --num-books     Number of books to process (default: 10)"
            echo "  --reducers      Number of reducers (default: 4)"
            echo "  --query         Search query text (default: from query.txt)"
            echo "  --skip-stage1   Skip Stage 1 if inverted index exists"
            echo "  --skip-download Skip download if data already in HDFS"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Helper functions
print_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Get query from file if not provided
QUERY_FILE="query.txt"
if [ -z "$QUERY_TEXT" ]; then
    if [ -f "$QUERY_FILE" ]; then
        QUERY_TEXT=$(cat "$QUERY_FILE")
    else
        print_error "No query provided and $QUERY_FILE not found"
        echo "Provide query with --query or create $QUERY_FILE"
        exit 1
    fi
fi

print_header "HBase-Enabled MapReduce Pipeline"

echo ""
print_status "Configuration:"
echo "  Books: $NUM_BOOKS"
echo "  Reducers: $NUM_REDUCERS"
echo "  Query: \"$QUERY_TEXT\""
echo "  Skip Stage 1: $SKIP_STAGE1"
echo "  Skip Download: $SKIP_DOWNLOAD"
echo ""

# Validate environment
print_status "Validating environment..."

if [ -z "$HADOOP_HOME" ]; then
    print_error "HADOOP_HOME not set"
    exit 1
fi

if ! command -v hadoop &> /dev/null; then
    print_error "Hadoop command not found"
    exit 1
fi

if ! hadoop fs -ls / &> /dev/null; then
    print_error "Cannot connect to HDFS"
    exit 1
fi

# Check HBase connectivity
if ! echo "status" | hbase shell -n 2>/dev/null | grep -q "active"; then
    print_error "Cannot connect to HBase. Ensure HBase is running."
    exit 1
fi

print_success "Environment validated"

# Check HBase tables exist
print_status "Checking HBase tables..."
if ! echo "exists 'inverted_index'" | hbase shell -n 2>/dev/null | grep -q "true"; then
    print_error "HBase table 'inverted_index' does not exist"
    echo "Run: ./create_hbase_tables.sh"
    exit 1
fi

if ! echo "exists 'similarity_scores'" | hbase shell -n 2>/dev/null | grep -q "true"; then
    print_error "HBase table 'similarity_scores' does not exist"
    echo "Run: ./create_hbase_tables.sh"
    exit 1
fi

print_success "HBase tables verified"

# HDFS paths
INPUT_DIR="/gutenberg-input-${NUM_BOOKS}"
TEMP_OUTPUT="/temp-hbase-output-$$"
STAGE1_OUTPUT="/hbase-stage1-output-${NUM_BOOKS}-${NUM_REDUCERS}"
STAGE2_OUTPUT="/hbase-stage2-output-${NUM_BOOKS}-${NUM_REDUCERS}"

# Total timer
PIPELINE_START=$(date +%s)

################################################################################
# Step 1: Download books to HDFS
################################################################################

if [ "$SKIP_DOWNLOAD" = false ]; then
    print_header "Step 1: Download Books"

    if ! hadoop fs -test -d "$INPUT_DIR" 2>/dev/null; then
        print_status "Downloading $NUM_BOOKS books to $INPUT_DIR..."

        python3 donwload_file.py \
            --csv gutenberg_metadata.csv \
            --num-books $NUM_BOOKS \
            --hdfs-dir "$INPUT_DIR"

        if [ $? -ne 0 ]; then
            print_error "Failed to download books"
            exit 1
        fi

        print_success "Books downloaded"
    else
        print_status "Input directory $INPUT_DIR already exists, skipping download"
    fi
else
    print_status "Skipping download (--skip-download flag)"
fi

# Verify HDFS directory and files
print_status "Verifying HDFS directory and files..."

# Check directory exists
if ! hadoop fs -test -d "$INPUT_DIR" 2>/dev/null; then
    print_error "HDFS directory $INPUT_DIR does not exist or is not a directory"
    print_error "Download may have failed silently"
    exit 1
fi

# Count files in directory
FILE_COUNT=$(hadoop fs -ls "$INPUT_DIR" 2>/dev/null | grep "^-" | wc -l | tr -d ' ')

if [ "$FILE_COUNT" -eq 0 ]; then
    print_error "No files found in $INPUT_DIR"
    print_error "Expected $NUM_BOOKS files but found 0"
    print_error "Please check download logs and HDFS connectivity"
    exit 1
fi

if [ "$FILE_COUNT" -lt "$NUM_BOOKS" ]; then
    print_warning "Expected $NUM_BOOKS files but found only $FILE_COUNT in $INPUT_DIR"
    print_warning "Some downloads may have failed"
else
    print_success "Verified $FILE_COUNT files in $INPUT_DIR"
fi

################################################################################
# Step 2: Build Inverted Index → HBase
################################################################################

if [ "$SKIP_STAGE1" = false ]; then
    print_header "Step 2: Build Inverted Index (HDFS → HBase)"

    STAGE1_START=$(date +%s)

    # Find Hadoop streaming jar
    HADOOP_STREAMING_JAR=$(find $HADOOP_HOME/share/hadoop/tools/lib/ -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)

    if [ -z "$HADOOP_STREAMING_JAR" ]; then
        print_error "Hadoop streaming jar not found"
        exit 1
    fi

    # Clean output directory
    hadoop fs -rm -r -f "$STAGE1_OUTPUT" 2>/dev/null

    # Count input files for total_map_tasks
    INPUT_FILE_COUNT=$(hadoop fs -ls "$INPUT_DIR" 2>/dev/null | grep "^-" | wc -l | tr -d ' ')

    print_status "Running MapReduce (Stage 1a: Build Inverted Index → HDFS)..."

    # Run MapReduce with HDFS output
    hadoop jar "$HADOOP_STREAMING_JAR" \
        -D mapreduce.job.reduces="$NUM_REDUCERS" \
        -D mapreduce.job.name="Inverted_Index_${NUM_BOOKS}books" \
        -D mapreduce.map.memory.mb=2048 \
        -D mapreduce.reduce.memory.mb=2048 \
        -D total_map_tasks="$INPUT_FILE_COUNT" \
        -input "$INPUT_DIR" \
        -output "$STAGE1_OUTPUT" \
        -mapper "python3 inverted_index_mapper.py" \
        -reducer "python3 hbase_inverted_index_reducer.py" \
        -file inverted_index_mapper.py \
        -file hbase_inverted_index_reducer.py

    STAGE1A_EXIT=$?

    if [ $STAGE1A_EXIT -ne 0 ]; then
        print_error "Stage 1a (MapReduce) failed"
        exit $STAGE1A_EXIT
    fi

    print_success "MapReduce completed, output in $STAGE1_OUTPUT"

    # Import to HBase
    print_status "Running Stage 1b: Import HDFS → HBase (inverted_index table)..."

    python3 import_to_hbase.py \
        --hdfs-path "$STAGE1_OUTPUT" \
        --table-name inverted_index \
        --batch-size 5000

    STAGE1B_EXIT=$?

    STAGE1_END=$(date +%s)
    STAGE1_TIME=$((STAGE1_END - STAGE1_START))

    if [ $STAGE1B_EXIT -eq 0 ]; then
        print_success "Stage 1 completed in ${STAGE1_TIME}s (MapReduce + HBase import)"
        print_status "HDFS output retained at: $STAGE1_OUTPUT"
    else
        print_error "Stage 1b (HBase import) failed"
        exit $STAGE1B_EXIT
    fi
else
    print_status "Skipping Stage 1 (--skip-stage1 flag)"
    STAGE1_TIME=0
fi

################################################################################
# Step 3: Compute Similarities → HBase
################################################################################

print_header "Step 3: Compute Similarities (HDFS → HBase)"

STAGE2_START=$(date +%s)

# Use inverted index from Stage 1 or fallback to standard location
if hadoop fs -test -d "$STAGE1_OUTPUT" 2>/dev/null; then
    INDEX_DIR="$STAGE1_OUTPUT"
    print_status "Using inverted index from Stage 1: $INDEX_DIR"
else
    # Fallback to standard inverted index location
    INDEX_DIR="/gutenberg-output-${NUM_BOOKS}-${NUM_REDUCERS}"

    if ! hadoop fs -test -d "$INDEX_DIR" 2>/dev/null; then
        print_warning "HDFS inverted index not found at $INDEX_DIR"
        print_status "Running Stage 1 with HDFS output first..."

        ./run_inverted_index_mapreduce.sh \
            --input "$INPUT_DIR" \
            --output "$INDEX_DIR" \
            --reducers "$NUM_REDUCERS"

        if [ $? -ne 0 ]; then
            print_error "Failed to create HDFS inverted index"
            exit 1
        fi
    fi
fi

# Clean output directory
hadoop fs -rm -r -f "$STAGE2_OUTPUT" 2>/dev/null

print_status "Running MapReduce (Stage 2a: JPII Similarity → HDFS)..."

# Run MapReduce with HDFS output
hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -D mapreduce.job.name="JPII_Similarity_${NUM_BOOKS}books" \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=2048 \
    -input "$INDEX_DIR" \
    -output "$STAGE2_OUTPUT" \
    -mapper "python3 jpii_mapper.py" \
    -reducer "python3 hbase_jpii_reducer.py" \
    -file jpii_mapper.py \
    -file hbase_jpii_reducer.py \
    -cmdenv q_from_user="$QUERY_TEXT"

STAGE2A_EXIT=$?

if [ $STAGE2A_EXIT -ne 0 ]; then
    print_error "Stage 2a (MapReduce) failed"
    exit $STAGE2A_EXIT
fi

print_success "MapReduce completed, output in $STAGE2_OUTPUT"

# Import to HBase
print_status "Running Stage 2b: Import HDFS → HBase (similarity_scores table)..."

python3 import_to_hbase.py \
    --hdfs-path "$STAGE2_OUTPUT" \
    --table-name similarity_scores \
    --batch-size 5000

STAGE2B_EXIT=$?

STAGE2_END=$(date +%s)
STAGE2_TIME=$((STAGE2_END - STAGE2_START))

if [ $STAGE2B_EXIT -eq 0 ]; then
    print_success "Stage 2 completed in ${STAGE2_TIME}s (MapReduce + HBase import)"
    print_status "HDFS output retained at: $STAGE2_OUTPUT"
else
    print_error "Stage 2b (HBase import) failed"
    exit $STAGE2B_EXIT
fi

################################################################################
# Step 4: Query Results from HBase
################################################################################

print_header "Step 4: Query Results from HBase"

PIPELINE_END=$(date +%s)
PIPELINE_TIME=$((PIPELINE_END - PIPELINE_START))

print_status "Pipeline Summary:"
echo "  Stage 1 Time: ${STAGE1_TIME}s"
echo "  Stage 2 Time: ${STAGE2_TIME}s"
echo "  Total Time: ${PIPELINE_TIME}s"
echo ""

print_status "Querying HBase for similar documents..."
echo ""

# Check if HBase Thrift server is running
if nc -z localhost 9090 2>/dev/null; then
    python3 query_similarity.py "$QUERY_TEXT" --top 10
else
    print_warning "HBase Thrift server not running on port 9090"
    print_status "Start it with: hbase thrift start -p 9090"
    print_status "Then query with: python3 query_similarity.py \"$QUERY_TEXT\""
fi

################################################################################
# Final Summary
################################################################################

print_header "Pipeline Complete!"

echo ""
print_success "Results stored in HBase tables"
echo ""
print_status "To query results:"
echo "  python3 query_similarity.py \"$QUERY_TEXT\""
echo "  python3 query_similarity.py \"$QUERY_TEXT\" --top 20 --min-score 0.3"
echo ""
print_status "To view HBase tables directly:"
echo "  hbase shell"
echo "  scan 'inverted_index', {LIMIT => 10}"
echo "  scan 'similarity_scores', {LIMIT => 10}"
echo ""
print_status "To start HBase Thrift server (if not running):"
echo "  hbase thrift start -p 9090"
echo ""

print_success "All done! 🎉"
