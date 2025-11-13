#!/bin/bash

################################################################################
# Hadoop Streaming Job for Inverted Index on Project Gutenberg Books
#
# This script executes a MapReduce job to build an inverted index from
# Project Gutenberg books stored in HDFS.
#
# Usage: ./run_inverted_index_mapreduce.sh [NUM_REDUCERS]
#   NUM_REDUCERS: Optional number of reducer tasks (default: 2)
#
# Example: ./run_inverted_index_mapreduce.sh 4
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
HDFS_INPUT_PATH="/gutenberg-input"
HDFS_OUTPUT_PATH="/gutenberg-output"
MAPPER_SCRIPT="inverted_index_mapper.py"
REDUCER_SCRIPT="inverted_index_reducer.py"
NUM_REDUCERS=${1:-2}  # Default to 2 reducers if not specified

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
if [ ! -f "$MAPPER_SCRIPT" ]; then
    print_error "Mapper script not found: $MAPPER_SCRIPT"
    exit 1
fi
print_success "Found mapper script: $MAPPER_SCRIPT"

# Check if reducer script exists
if [ ! -f "$REDUCER_SCRIPT" ]; then
    print_error "Reducer script not found: $REDUCER_SCRIPT"
    exit 1
fi
print_success "Found reducer script: $REDUCER_SCRIPT"

# Make scripts executable
chmod +x "$MAPPER_SCRIPT" "$REDUCER_SCRIPT"
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
# 2. NLTK DATA SETUP
################################################################################

print_header "Step 2: NLTK Data Setup"

# Use user's home directory instead of /root (no sudo required)
NLTK_DATA_DIR="$HOME/nltk_data"

# Check if NLTK is installed
if ! python3 -c "import nltk" 2>/dev/null; then
    print_warning "NLTK not installed. Installing..."

    # Detect if we need --break-system-packages (Python 3.12+)
    PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

    PIP_FLAGS="--user"
    if [ -f /etc/debian_version ] && [ "$PYTHON_MAJOR" -ge 3 ] && [ "$PYTHON_MINOR" -ge 12 ]; then
        PIP_FLAGS="--user --break-system-packages"
        print_status "Python $PYTHON_VERSION detected, using --break-system-packages"
    fi

    pip3 install $PIP_FLAGS nltk 2>&1 | grep -v "WARNING" || true
    print_success "NLTK installed"
fi

# Check if NLTK data exists
if [ ! -d "$NLTK_DATA_DIR/corpora/stopwords" ] || [ ! -d "$NLTK_DATA_DIR/corpora/wordnet" ]; then
    print_warning "NLTK data not found at $NLTK_DATA_DIR. Downloading..."

    python3 << 'EOF'
import nltk
import os
import sys

try:
    # Use user's home directory (no sudo required)
    nltk_data_dir = os.path.expanduser('~/nltk_data')

    # Create directory if it doesn't exist
    os.makedirs(nltk_data_dir, exist_ok=True)

    # Download required NLTK data
    print("Downloading NLTK stopwords...")
    nltk.download('stopwords', download_dir=nltk_data_dir, quiet=False)

    print("Downloading NLTK wordnet...")
    nltk.download('wordnet', download_dir=nltk_data_dir, quiet=False)

    print("Downloading NLTK punkt...")
    nltk.download('punkt', download_dir=nltk_data_dir, quiet=False)

    print("NLTK data download completed successfully!")
    sys.exit(0)
except Exception as e:
    print(f"Error downloading NLTK data: {e}")
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        print_success "NLTK data downloaded successfully"
    else
        print_error "Failed to download NLTK data"
        exit 1
    fi
else
    print_success "NLTK data already exists at $NLTK_DATA_DIR"
fi

################################################################################
# 3. HDFS PREPARATION
################################################################################

print_header "Step 3: HDFS Preparation"

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
# 4. HADOOP STREAMING JOB CONFIGURATION
################################################################################

print_header "Step 4: Hadoop Streaming Job Configuration"

print_status "Job Configuration:"
echo "  - Input Path: $HDFS_INPUT_PATH"
echo "  - Output Path: $HDFS_OUTPUT_PATH"
echo "  - Mapper: $MAPPER_SCRIPT"
echo "  - Reducer: $REDUCER_SCRIPT"
echo "  - Number of Reducers: $NUM_REDUCERS"
echo "  - Total Documents: $INPUT_FILE_COUNT"

################################################################################
# 5. JOB EXECUTION
################################################################################

print_header "Step 5: Executing Hadoop Streaming Job"

print_status "Starting MapReduce job..."
echo ""

# Execute Hadoop streaming job
# NOTE: -D options (generic options) MUST come before streaming options
hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -D mapreduce.job.name="Inverted_Index_Project_Gutenberg" \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=2048 \
    -input "$HDFS_INPUT_PATH" \
    -output "$HDFS_OUTPUT_PATH" \
    -mapper "$MAPPER_SCRIPT" \
    -reducer "$REDUCER_SCRIPT" \
    -file "$MAPPER_SCRIPT" \
    -file "$REDUCER_SCRIPT" \
    -cmdenv total_map_tasks="$INPUT_FILE_COUNT"

JOB_EXIT_CODE=$?

echo ""

if [ $JOB_EXIT_CODE -eq 0 ]; then
    print_success "Hadoop MapReduce job completed successfully!"
else
    print_error "Hadoop MapReduce job failed with exit code: $JOB_EXIT_CODE"
    exit $JOB_EXIT_CODE
fi

################################################################################
# 6. POST-EXECUTION
################################################################################

print_header "Step 6: Results Summary"

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

print_header "Job Complete!"
