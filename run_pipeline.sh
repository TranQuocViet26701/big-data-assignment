#!/bin/bash

################################################################################
# Master Pipeline Script for Inverted Index MapReduce
#
# This script orchestrates the complete workflow:
#   1. Environment validation
#   2. Download books from Project Gutenberg
#   3. Upload to HDFS
#   4. Run MapReduce job
#   5. Display results summary
#
# Usage: ./run_pipeline.sh --num-books <count> --reducers <num> [OPTIONS]
#
# Required Parameters:
#   --num-books:  Number of books to process (e.g., 100, 200, 500)
#   --reducers:   Number of reducer tasks for MapReduce
#
# Optional Parameters:
#   --csv:        Path to metadata CSV file (default: gutenberg_metadata.csv)
#   --start-row:  Starting row in CSV (default: 0)
#   --skip-download: Skip download if data already exists in HDFS
#
# Examples:
#   ./run_pipeline.sh --num-books 100 --reducers 4
#   ./run_pipeline.sh --num-books 200 --reducers 8 --csv custom_metadata.csv
#   ./run_pipeline.sh --num-books 500 --reducers 12 --start-row 100
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default Configuration
CSV_FILE="gutenberg_metadata.csv"
NUM_BOOKS=""
NUM_REDUCERS=""
START_ROW=0
SKIP_DOWNLOAD=false

# Print functions
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
        --csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --start-row)
            START_ROW="$2"
            shift 2
            ;;
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 --num-books <count> --reducers <num> [OPTIONS]"
            echo ""
            echo "Required Parameters:"
            echo "  --num-books   Number of books to process"
            echo "  --reducers    Number of reducer tasks"
            echo ""
            echo "Optional Parameters:"
            echo "  --csv         Path to metadata CSV file (default: gutenberg_metadata.csv)"
            echo "  --start-row   Starting row in CSV (default: 0)"
            echo "  --skip-download  Skip download if data already in HDFS"
            echo ""
            echo "Examples:"
            echo "  $0 --num-books 100 --reducers 4"
            echo "  $0 --num-books 200 --reducers 8 --csv custom_metadata.csv"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Validate required parameters
if [ -z "$NUM_BOOKS" ]; then
    print_error "Missing required parameter: --num-books"
    echo "Use --help for usage information"
    exit 1
fi

if [ -z "$NUM_REDUCERS" ]; then
    print_error "Missing required parameter: --reducers"
    echo "Use --help for usage information"
    exit 1
fi

# Validate numeric parameters
if ! [[ "$NUM_BOOKS" =~ ^[0-9]+$ ]]; then
    print_error "--num-books must be a positive integer"
    exit 1
fi

if ! [[ "$NUM_REDUCERS" =~ ^[0-9]+$ ]]; then
    print_error "--reducers must be a positive integer"
    exit 1
fi

if ! [[ "$START_ROW" =~ ^[0-9]+$ ]]; then
    print_error "--start-row must be a non-negative integer"
    exit 1
fi

# Generate HDFS directory names based on book count
HDFS_INPUT_PATH="/gutenberg-input-${NUM_BOOKS}"
HDFS_OUTPUT_PATH="/gutenberg-output-${NUM_BOOKS}"

# Start timer
START_TIME=$(date +%s)

################################################################################
# PIPELINE EXECUTION
################################################################################

print_header "Inverted Index MapReduce Pipeline"
echo "Configuration:"
echo "  - Books to process: $NUM_BOOKS"
echo "  - CSV file: $CSV_FILE"
echo "  - Starting row: $START_ROW"
echo "  - Reducers: $NUM_REDUCERS"
echo "  - Input HDFS: $HDFS_INPUT_PATH"
echo "  - Output HDFS: $HDFS_OUTPUT_PATH"
echo ""

################################################################################
# Step 1: Environment Validation
################################################################################

print_header "Step 1: Environment Validation"

# Check if HADOOP_HOME is set
if [ -z "$HADOOP_HOME" ]; then
    print_error "HADOOP_HOME is not set. Please set it to your Hadoop installation directory."
    exit 1
fi
print_success "HADOOP_HOME is set to: $HADOOP_HOME"

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

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found. Please install Python 3."
    exit 1
fi
print_success "Python 3 is available: $(python3 --version)"

# Check if CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    print_error "CSV file not found: $CSV_FILE"
    exit 1
fi
print_success "Found CSV file: $CSV_FILE"

# Check if scripts exist
if [ ! -f "donwload_file.py" ]; then
    print_error "Download script not found: donwload_file.py"
    exit 1
fi
print_success "Found download script: donwload_file.py"

if [ ! -f "run_inverted_index_mapreduce.sh" ]; then
    print_error "MapReduce script not found: run_inverted_index_mapreduce.sh"
    exit 1
fi
print_success "Found MapReduce script: run_inverted_index_mapreduce.sh"

################################################################################
# Step 2: HDFS Directory Preparation
################################################################################

print_header "Step 2: HDFS Directory Preparation"

# Create input directory if it doesn't exist
if ! hadoop fs -test -d "$HDFS_INPUT_PATH"; then
    print_status "Creating HDFS input directory: $HDFS_INPUT_PATH"
    hadoop fs -mkdir -p "$HDFS_INPUT_PATH"
    print_success "Input directory created"
else
    print_status "Input directory already exists: $HDFS_INPUT_PATH"

    # Check if data already exists
    FILE_COUNT=$(hadoop fs -ls "$HDFS_INPUT_PATH" | grep -c "^-" || true)
    if [ "$FILE_COUNT" -gt 0 ]; then
        print_warning "Found $FILE_COUNT files in input directory"
        if [ "$SKIP_DOWNLOAD" = true ]; then
            print_status "Skipping download (--skip-download flag set)"
        else
            print_warning "Existing files will be overwritten during download"
        fi
    fi
fi

# Remove output directory if it exists
if hadoop fs -test -d "$HDFS_OUTPUT_PATH"; then
    print_warning "Output directory already exists. Removing: $HDFS_OUTPUT_PATH"
    hadoop fs -rm -r "$HDFS_OUTPUT_PATH"
    print_success "Output directory removed"
fi

################################################################################
# Step 3: Download and Upload Books
################################################################################

if [ "$SKIP_DOWNLOAD" = false ]; then
    print_header "Step 3: Downloading and Uploading Books"

    print_status "Downloading $NUM_BOOKS books from Project Gutenberg..."
    print_status "This may take several minutes depending on network speed..."
    echo ""

    DOWNLOAD_START=$(date +%s)

    python3 donwload_file.py \
        --csv "$CSV_FILE" \
        --num-books "$NUM_BOOKS" \
        --hdfs-dir "$HDFS_INPUT_PATH" \
        --start-row "$START_ROW"

    DOWNLOAD_EXIT=$?
    DOWNLOAD_END=$(date +%s)
    DOWNLOAD_DURATION=$((DOWNLOAD_END - DOWNLOAD_START))

    if [ $DOWNLOAD_EXIT -eq 0 ]; then
        print_success "Download completed in ${DOWNLOAD_DURATION}s"

        # Verify files were uploaded
        FILE_COUNT=$(hadoop fs -ls "$HDFS_INPUT_PATH" | grep -c "^-" || true)
        print_success "Verified $FILE_COUNT files in HDFS: $HDFS_INPUT_PATH"
    else
        print_error "Download failed with exit code: $DOWNLOAD_EXIT"
        exit $DOWNLOAD_EXIT
    fi
else
    print_header "Step 3: Skipping Download (--skip-download)"

    # Verify data exists
    if ! hadoop fs -test -d "$HDFS_INPUT_PATH"; then
        print_error "Input directory does not exist: $HDFS_INPUT_PATH"
        print_error "Cannot skip download - no data available"
        exit 1
    fi

    FILE_COUNT=$(hadoop fs -ls "$HDFS_INPUT_PATH" | grep -c "^-" || true)
    if [ "$FILE_COUNT" -eq 0 ]; then
        print_error "Input directory is empty: $HDFS_INPUT_PATH"
        print_error "Cannot skip download - no data available"
        exit 1
    fi

    print_success "Using existing $FILE_COUNT files in: $HDFS_INPUT_PATH"
fi

################################################################################
# Step 4: Run MapReduce Job
################################################################################

print_header "Step 4: Running MapReduce Job"

print_status "Starting Hadoop Streaming MapReduce job..."
print_status "Input: $HDFS_INPUT_PATH"
print_status "Output: $HDFS_OUTPUT_PATH"
print_status "Reducers: $NUM_REDUCERS"
echo ""

MAPREDUCE_START=$(date +%s)

./run_inverted_index_mapreduce.sh \
    --input "$HDFS_INPUT_PATH" \
    --output "$HDFS_OUTPUT_PATH" \
    --reducers "$NUM_REDUCERS"

MAPREDUCE_EXIT=$?
MAPREDUCE_END=$(date +%s)
MAPREDUCE_DURATION=$((MAPREDUCE_END - MAPREDUCE_START))

if [ $MAPREDUCE_EXIT -eq 0 ]; then
    print_success "MapReduce job completed in ${MAPREDUCE_DURATION}s"
else
    print_error "MapReduce job failed with exit code: $MAPREDUCE_EXIT"
    exit $MAPREDUCE_EXIT
fi

################################################################################
# Step 5: Results Summary
################################################################################

print_header "Step 5: Results Summary"

# Verify output
if hadoop fs -test -d "$HDFS_OUTPUT_PATH"; then
    print_success "Output directory created: $HDFS_OUTPUT_PATH"

    # Check for success file
    if hadoop fs -test -e "$HDFS_OUTPUT_PATH/_SUCCESS"; then
        print_success "Job completed successfully (_SUCCESS file found)"
    else
        print_warning "_SUCCESS file not found - job may have had issues"
    fi

    # List output files
    print_status "Output files:"
    hadoop fs -ls "$HDFS_OUTPUT_PATH" | grep "^-" | awk '{print "  - " $8 " (" $5 " bytes)"}'

    # Count unique words
    WORD_COUNT=$(hadoop fs -cat "$HDFS_OUTPUT_PATH/part-*" | wc -l | tr -d ' ')
    print_success "Total unique words in inverted index: $WORD_COUNT"

    echo ""
    print_status "Sample output (first 20 lines):"
    echo "----------------------------------------"
    hadoop fs -cat "$HDFS_OUTPUT_PATH/part-*" | head -20
    echo "----------------------------------------"
else
    print_error "Output directory was not created"
    exit 1
fi

# Calculate total time
END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
MINUTES=$((TOTAL_DURATION / 60))
SECONDS=$((TOTAL_DURATION % 60))

echo ""
print_header "Pipeline Complete!"
echo ""
echo "Execution Summary:"
echo "  - Books processed: $NUM_BOOKS"
echo "  - Input directory: $HDFS_INPUT_PATH"
echo "  - Output directory: $HDFS_OUTPUT_PATH"
echo "  - Unique words: $WORD_COUNT"
if [ "$SKIP_DOWNLOAD" = false ]; then
    echo "  - Download time: ${DOWNLOAD_DURATION}s"
fi
echo "  - MapReduce time: ${MAPREDUCE_DURATION}s"
echo "  - Total time: ${MINUTES}m ${SECONDS}s"
echo ""
print_status "To view full results:"
echo "  hadoop fs -cat $HDFS_OUTPUT_PATH/part-* | less"
echo ""
print_status "To download results locally:"
echo "  hadoop fs -get $HDFS_OUTPUT_PATH ./inverted_index_output_${NUM_BOOKS}"
echo ""
print_status "To search for a specific word:"
echo "  hadoop fs -cat $HDFS_OUTPUT_PATH/part-* | grep '^yourword'"
echo ""
print_success "All done! 🎉"
