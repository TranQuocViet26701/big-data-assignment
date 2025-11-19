#!/bin/bash

################################################################################
# Hadoop Streaming Job for Inverted Index on Project Gutenberg Books
#
# This script executes a MapReduce job to build an inverted index from
# Project Gutenberg books stored in HDFS.
#
# Usage: ./run_inverted_index_mapreduce.sh --input <path> --output <path> --reducers <num>
#
# Parameters:
#   --input:     HDFS input directory path (default: /gutenberg-input)
#   --output:    HDFS output directory path (default: /gutenberg-output)
#   --reducers:  Number of reducer tasks (default: 2)
#
# Examples:
#   ./run_inverted_index_mapreduce.sh --input /gutenberg-input-100 --output /gutenberg-output-100 --reducers 4
#   ./run_inverted_index_mapreduce.sh --input /gutenberg-input-200 --output /gutenberg-output-200 --reducers 8
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default Configuration
HDFS_INPUT_PATH="/gutenberg-output"
HDFS_OUTPUT_PATH="/jpii-output"
RANKING_SEARCH_SCRIPT="ranking_search.py"
NUM_REDUCERS=1
QUERY="query.txt"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --input)
            HDFS_INPUT_PATH="$2"
            shift 2
            ;;
        --output)
            HDFS_OUTPUT_PATH="$2"
            shift 2
            ;;
        --reducers)
            NUM_REDUCERS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 --input <path> --output <path> --reducers <num>"
            echo ""
            echo "Parameters:"
            echo "  --input      HDFS input directory path (default: /gutenberg-input)"
            echo "  --output     HDFS output directory path (default: /gutenberg-output)"
            echo "  --reducers   Number of reducer tasks (default: 2)"
            echo ""
            echo "Examples:"
            echo "  $0 --input /gutenberg-input-100 --output /gutenberg-output-100 --reducers 4"
            echo "  $0 --input /gutenberg-input-200 --output /gutenberg-output-200 --reducers 8"
            exit 0
            ;;
        *)
            # Support old positional argument for backwards compatibility
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                NUM_REDUCERS="$1"
                print_warning "Positional argument for reducers is deprecated. Use --reducers instead."
                shift
            else
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
            fi
            ;;
    esac
done

# Print colored message
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

print_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

################################################################################
# 1. ENVIRONMENT VALIDATION
################################################################################

print_header "Step 1: Environment Validation"

# Check if HADOOP_HOME is set
if [ -z "$HADOOP_HOME" ]; then
    print_error "HADOOP_HOME is not set. Please set it to your Hadoop installation directory."
    exit 1
fi
print_success "HADOOP_HOME is set to: $HADOOP_HOME"

# Check if Hadoop streaming jar exists
HADOOP_STREAMING_JAR=$(find $HADOOP_HOME/share/hadoop/tools/lib/ -name 'hadoop-streaming*.jar' 2>/dev/null | head -1)
if [ -z "$HADOOP_STREAMING_JAR" ]; then
    print_error "Hadoop streaming jar not found in $HADOOP_HOME/share/hadoop/tools/lib/"
    exit 1
fi
print_success "Found Hadoop streaming jar: $HADOOP_STREAMING_JAR"

# Check if mapper script exists
if [ ! -f "$RANKING_SEARCH_SCRIPT" ]; then
    print_error "Ranking search script not found: $RANKING_SEARCH_SCRIPT"
    exit 1
fi
print_success "Found ranking search script: $RANKING_SEARCH_SCRIPT"

# Make scripts executable
chmod +x "$RANKING_SEARCH_SCRIPT"
print_status "Made scripts executable"

# Check if Hadoop is accessible
if ! command -v hadoop &> /dev/null; then
    print_error "Hadoop command not found. Please ensure Hadoop is in your PATH."
    exit 1
fi
print_success "Hadoop command is accessible"

# Check if HDFS is running
if ! hadoop fs -ls / &> /dev/null; then
    print_error "Cannot connect to HDFS. Please ensure HDFS is running."
    exit 1
fi
print_success "HDFS is accessible"

################################################################################
# 2. HDFS PREPARATION
################################################################################

print_header "Step 2: HDFS Preparation"

# Check if input directory exists
print_status "Checking input directory: $HDFS_INPUT_PATH"
if ! hadoop fs -test -d "$HDFS_INPUT_PATH"; then
    print_error "Input directory does not exist: $HDFS_INPUT_PATH"
    print_status "Please run the download script (donwload_file.py) first to populate the input directory."
    exit 1
fi

# Count input files
INPUT_FILE_COUNT=$(hadoop fs -ls "$HDFS_INPUT_PATH" | grep -c "^-" || true)
if [ "$INPUT_FILE_COUNT" -eq 0 ]; then
    print_error "No files found in input directory: $HDFS_INPUT_PATH"
    exit 1
fi
print_success "Found $INPUT_FILE_COUNT input files in $HDFS_INPUT_PATH"

# List input files
print_status "Input files:"
hadoop fs -ls "$HDFS_INPUT_PATH" | grep "^-" | awk '{print "  - " $8}'

# Clean existing output directory
print_status "Checking output directory: $HDFS_OUTPUT_PATH"
if hadoop fs -test -d "$HDFS_OUTPUT_PATH"; then
    print_warning "Output directory already exists. Removing..."
    hadoop fs -rm -r "$HDFS_OUTPUT_PATH"
    print_success "Output directory removed"
else
    print_status "Output directory does not exist (this is expected)"
fi

################################################################################
# 3. HADOOP STREAMING JOB CONFIGURATION
################################################################################

print_header "Step 3: Hadoop Streaming Job Configuration"

print_status "Job Configuration:"
echo "  - Input Path: $HDFS_INPUT_PATH"
echo "  - Output Path: $HDFS_OUTPUT_PATH"
echo "  - Ranking search script: $RANKING_SEARCH_SCRIPT"
echo "  - Number of Reducers: $NUM_REDUCERS"
echo "  - Total Documents: $INPUT_FILE_COUNT"
echo "  - QUERY: $QUERY"

################################################################################
# 4. JOB EXECUTION
################################################################################

print_header "Step 4: Executing Hadoop Streaming Job"

print_status "Starting MapReduce job..."
echo ""

# Execute Hadoop streaming job
# NOTE: -D options (generic options) MUST come before streaming options
hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.reduces=1	 \
    -D mapreduce.job.name="Ranking Search" \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=2048 \
    -input "$HDFS_INPUT_PATH" \
    -output "$HDFS_OUTPUT_PATH" \
	-mapper "/bin/cat" \
	-reducer "python3 $RANKING_SEARCH_SCRIPT" \
	-file "$RANKING_SEARCH_SCRIPT" \
	-cmdenv options="query"
	

JOB_EXIT_CODE=$?

echo ""

if [ $JOB_EXIT_CODE -eq 0 ]; then
    print_success "Hadoop MapReduce job completed successfully!"
else
    print_error "Hadoop MapReduce job failed with exit code: $JOB_EXIT_CODE"
    exit $JOB_EXIT_CODE
fi

################################################################################
# 5. POST-EXECUTION
################################################################################
print_header "Step 5: Results Summary"

# Check output directory
if hadoop fs -test -d "$HDFS_OUTPUT_PATH"; then
    print_success "Output directory created: $HDFS_OUTPUT_PATH"

    # List output files
    print_status "Output files:"
    hadoop fs -ls "$HDFS_OUTPUT_PATH" | grep "^-" | awk '{print "  - " $8 " (" $5 " bytes)"}'

    echo ""
    print_status "Sample output (first 20 lines):"
    echo "----------------------------------------"
    hadoop fs -cat "$HDFS_OUTPUT_PATH/part-*" | head -20
    echo "----------------------------------------"
    echo ""

    print_success "Inverted index created successfully!"
    echo ""
    print_status "To view full results, use:"
    echo "  hadoop fs -cat $HDFS_OUTPUT_PATH/part-* | less"
    echo ""
    print_status "To download results to local filesystem:"
    echo "  hadoop fs -get $HDFS_OUTPUT_PATH ./inverted_index_output"
    echo ""
    print_status "To count total unique words:"
    echo "  hadoop fs -cat $HDFS_OUTPUT_PATH/part-* | wc -l"
else
    print_error "Output directory was not created"
    exit 1
fi
echo "----------------------------------------------------"

print_header "Job Complete!"
