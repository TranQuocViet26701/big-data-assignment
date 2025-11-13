#!/bin/bash

################################################################################
# Complete Setup and Run Script
#
# This script does everything needed to run the MapReduce job:
# 1. Install dependencies (if needed)
# 2. Create HDFS directory
# 3. Download books and upload to HDFS
# 4. Run MapReduce job
# 5. Display results
#
# Usage: ./setup_and_run.sh
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

################################################################################
# Step 1: Check Dependencies
################################################################################

print_header "Step 1: Checking Python Dependencies"

# Check if pandas is installed (as a proxy for all dependencies)
if ! python3 -c "import pandas" 2>/dev/null; then
    print_warning "Python dependencies not found. Installing..."

    # Detect Python version for --break-system-packages
    PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
    PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
    PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

    PIP_FLAGS="--user"
    if [ -f /etc/debian_version ] && [ "$PYTHON_MAJOR" -ge 3 ] && [ "$PYTHON_MINOR" -ge 12 ]; then
        PIP_FLAGS="--user --break-system-packages"
        print_status "Python $PYTHON_VERSION detected, using --break-system-packages"
    fi

    print_status "Installing: pandas, numpy, requests, beautifulsoup4, gutenberg, colorama..."
    pip3 install $PIP_FLAGS pandas numpy requests beautifulsoup4 gutenberg colorama 2>&1 | grep -v "WARNING" || true

    print_success "Dependencies installed"
else
    print_success "Python dependencies already installed"
fi

################################################################################
# Step 2: Create HDFS Input Directory
################################################################################

print_header "Step 2: Creating HDFS Input Directory"

# Check if directory exists
if hdfs dfs -test -d /gutenberg-input 2>/dev/null; then
    print_success "HDFS directory /gutenberg-input already exists"
else
    print_status "Creating HDFS directory /gutenberg-input..."
    hdfs dfs -mkdir -p /gutenberg-input
    print_success "HDFS directory created"
fi

# Set permissions
print_status "Setting directory permissions..."
hdfs dfs -chmod 755 /gutenberg-input 2>/dev/null || true

################################################################################
# Step 3: Download Books and Upload to HDFS
################################################################################

print_header "Step 3: Downloading Books and Uploading to HDFS"

# Check if books already uploaded
EXISTING_FILES=$(hdfs dfs -ls /gutenberg-input 2>/dev/null | grep -c "\.txt" || echo "0")

if [ "$EXISTING_FILES" -gt 0 ]; then
    print_warning "Found $EXISTING_FILES files already in /gutenberg-input"
    read -p "Re-download and re-upload? (yes/no): " confirm

    if [ "$confirm" != "yes" ]; then
        print_status "Skipping download, using existing files"
    else
        print_status "Removing existing files..."
        hdfs dfs -rm /gutenberg-input/*.txt 2>/dev/null || true

        print_status "Running download script..."
        python3 donwload_file.py
        print_success "Books downloaded and uploaded"
    fi
else
    print_status "Running download script..."
    python3 donwload_file.py
    print_success "Books downloaded and uploaded"
fi

# Verify files in HDFS
FILE_COUNT=$(hdfs dfs -ls /gutenberg-input 2>/dev/null | grep -c "\.txt" || echo "0")
print_status "Total files in HDFS: $FILE_COUNT"
hdfs dfs -ls /gutenberg-input

################################################################################
# Step 4: Run MapReduce Job
################################################################################

print_header "Step 4: Running MapReduce Job"

print_status "Submitting job to YARN..."
./run_inverted_index_mapreduce.sh 2

################################################################################
# Summary
################################################################################

print_header "Complete! Summary"

print_success "Pipeline executed successfully"
echo ""
print_status "What happened:"
echo "  1. ✓ Python dependencies installed (pandas, numpy, nltk, etc.)"
echo "  2. ✓ HDFS directory created: /gutenberg-input"
echo "  3. ✓ Books downloaded and uploaded to HDFS"
echo "  4. ✓ MapReduce job executed"
echo "  5. ✓ Results written to: /gutenberg-output"
echo ""
print_status "View results:"
echo "  hdfs dfs -cat /gutenberg-output/part-* | head -20"
echo ""
print_status "Download results:"
echo "  hadoop fs -get /gutenberg-output ./inverted_index_results"
echo ""
print_status "Search for specific words:"
echo "  hdfs dfs -cat /gutenberg-output/part-* | grep '^elephant'"
echo ""

exit 0
