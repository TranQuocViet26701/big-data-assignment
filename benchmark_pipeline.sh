#!/bin/bash

################################################################################
# Comprehensive Performance Benchmark for MapReduce Similarity Search
#
# This script benchmarks the full pipeline (Inverted Index + JPII) across
# multiple dataset sizes and reducer configurations.
#
# Usage: ./benchmark_pipeline.sh
#
# Output:
#   - Console: Color-coded progress and summary tables
#   - CSV: benchmark_results.csv with detailed metrics
#   - Logs: benchmark_logs/<timestamp>/ directory
#
################################################################################

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
DATASET_SIZES=(10 50 100 200)  # Number of books to test
REDUCER_COUNTS=(2 4 8 16)      # Number of reducers to test
CSV_OUTPUT="benchmark_results.csv"
LOG_DIR="benchmark_logs/$(date +%Y%m%d_%H%M%S)"
QUERY_FILE="query.txt"

# Create default query if it doesn't exist
if [ ! -f "$QUERY_FILE" ]; then
    echo "animal nature wildlife conservation" > "$QUERY_FILE"
fi

################################################################################
# Helper Functions
################################################################################

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

print_metric() {
    printf "${CYAN}%-30s${NC} : ${MAGENTA}%s${NC}\n" "$1" "$2"
}

################################################################################
# Metric Collection Functions
################################################################################

get_hdfs_size_bytes() {
    local path=$1
    if hadoop fs -test -d "$path" 2>/dev/null; then
        hadoop fs -du -s "$path" 2>/dev/null | awk '{print $1}'
    else
        echo "0"
    fi
}

get_hdfs_size_mb() {
    local bytes=$1
    echo "scale=2; $bytes / 1024 / 1024" | bc
}

get_hdfs_file_count() {
    local path=$1
    if hadoop fs -test -d "$path" 2>/dev/null; then
        hadoop fs -ls "$path" 2>/dev/null | grep "^-" | wc -l | tr -d ' '
    else
        echo "0"
    fi
}

get_record_count() {
    local path=$1
    if hadoop fs -test -d "$path" 2>/dev/null; then
        hadoop fs -cat "$path/part-*" 2>/dev/null | wc -l | tr -d ' '
    else
        echo "0"
    fi
}

################################################################################
# Main Benchmark Function
################################################################################

run_benchmark() {
    local num_books=$1
    local num_reducers=$2
    local test_id="${num_books}books_${num_reducers}reducers"
    local test_log="${LOG_DIR}/${test_id}.log"

    print_header "TEST: $num_books books, $num_reducers reducers"

    # HDFS paths
    local input_dir="/gutenberg-input-${num_books}"
    local index_dir="/gutenberg-output-${num_books}-${num_reducers}"
    local jpii_dir="/jpii-output-${num_books}-${num_reducers}"

    # Initialize metrics
    local stage1_time=0
    local stage2_time=0
    local total_time=0
    local input_size_bytes=0
    local index_size_bytes=0
    local jpii_size_bytes=0
    local unique_words=0
    local similar_docs=0
    local throughput=0
    local status="FAILED"

    # Start total timer
    local total_start=$(date +%s)

    {
        echo "=== Benchmark Test: $test_id ==="
        echo "Start time: $(date)"
        echo ""

        # Step 1: Ensure input data exists
        print_status "Checking input data..."
        if ! hadoop fs -test -d "$input_dir" 2>/dev/null; then
            print_status "Downloading $num_books books..."
            python3 donwload_file.py \
                --csv gutenberg_metadata.csv \
                --num-books $num_books \
                --hdfs-dir "$input_dir" >> "$test_log" 2>&1

            if [ $? -ne 0 ]; then
                print_error "Failed to download books"
                echo "FAILED,download,$num_books,$num_reducers,0,0,0,0,0,0,0,0,0" >> "$CSV_OUTPUT"
                return 1
            fi
        fi
        input_size_bytes=$(get_hdfs_size_bytes "$input_dir")
        print_success "Input data ready ($(get_hdfs_size_mb $input_size_bytes) MB)"

        # Step 2: Clean previous outputs
        print_status "Cleaning previous outputs..."
        hadoop fs -rm -r -f "$index_dir" >> "$test_log" 2>&1
        hadoop fs -rm -r -f "$jpii_dir" >> "$test_log" 2>&1

        # Step 3: Stage 1 - Inverted Index
        print_status "Running Stage 1: Inverted Index..."
        local stage1_start=$(date +%s)

        ./run_inverted_index_mapreduce.sh \
            --input "$input_dir" \
            --output "$index_dir" \
            --reducers $num_reducers >> "$test_log" 2>&1

        local stage1_exit=$?
        local stage1_end=$(date +%s)
        stage1_time=$((stage1_end - stage1_start))

        if [ $stage1_exit -ne 0 ]; then
            print_error "Stage 1 failed (exit code: $stage1_exit)"
            echo "FAILED,stage1,$num_books,$num_reducers,$stage1_time,0,0,$input_size_bytes,0,0,0,0,0" >> "$CSV_OUTPUT"
            return 1
        fi

        index_size_bytes=$(get_hdfs_size_bytes "$index_dir")
        unique_words=$(get_record_count "$index_dir")
        print_success "Stage 1 completed in ${stage1_time}s ($(get_hdfs_size_mb $index_size_bytes) MB, $unique_words words)"

        # Step 4: Stage 2 - JPII Similarity
        print_status "Running Stage 2: JPII Similarity..."
        local stage2_start=$(date +%s)

        ./run_jpii.sh \
            --input "$index_dir" \
            --output "$jpii_dir" \
            --reducers $num_reducers >> "$test_log" 2>&1

        local stage2_exit=$?
        local stage2_end=$(date +%s)
        stage2_time=$((stage2_end - stage2_start))

        if [ $stage2_exit -ne 0 ]; then
            print_error "Stage 2 failed (exit code: $stage2_exit)"
            echo "FAILED,stage2,$num_books,$num_reducers,$stage1_time,$stage2_time,0,$input_size_bytes,$index_size_bytes,0,$unique_words,0,0" >> "$CSV_OUTPUT"
            return 1
        fi

        jpii_size_bytes=$(get_hdfs_size_bytes "$jpii_dir")
        similar_docs=$(get_record_count "$jpii_dir")
        print_success "Stage 2 completed in ${stage2_time}s ($similar_docs similar docs found)"

        # Calculate final metrics
        local total_end=$(date +%s)
        total_time=$((total_end - total_start))

        # Calculate throughput (books per second)
        if [ $total_time -gt 0 ]; then
            throughput=$(echo "scale=2; $num_books / $total_time" | bc)
        fi

        status="SUCCESS"

        echo ""
        echo "=== Test Complete ==="
        echo "End time: $(date)"

    } >> "$test_log" 2>&1

    # Display summary
    echo ""
    print_metric "Status" "$status"
    print_metric "Books Processed" "$num_books"
    print_metric "Reducers Used" "$num_reducers"
    print_metric "Stage 1 Time" "${stage1_time}s"
    print_metric "Stage 2 Time" "${stage2_time}s"
    print_metric "Total Time" "${total_time}s"
    print_metric "Input Size" "$(get_hdfs_size_mb $input_size_bytes) MB"
    print_metric "Index Size" "$(get_hdfs_size_mb $index_size_bytes) MB"
    print_metric "JPII Output Size" "$(get_hdfs_size_mb $jpii_size_bytes) MB"
    print_metric "Unique Words" "$unique_words"
    print_metric "Similar Documents" "$similar_docs"
    print_metric "Throughput" "$throughput books/sec"
    echo ""

    # Append to CSV
    echo "$status,$test_id,$num_books,$num_reducers,$stage1_time,$stage2_time,$total_time,$input_size_bytes,$index_size_bytes,$jpii_size_bytes,$unique_words,$similar_docs,$throughput" >> "$CSV_OUTPUT"

    return 0
}

################################################################################
# Main Execution
################################################################################

print_header "Performance Benchmark - MapReduce Similarity Search"

# Validate environment
print_status "Validating environment..."

if [ -z "$HADOOP_HOME" ]; then
    print_error "HADOOP_HOME is not set"
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

if [ ! -f "run_inverted_index_mapreduce.sh" ]; then
    print_error "run_inverted_index_mapreduce.sh not found"
    exit 1
fi

if [ ! -f "run_jpii.sh" ]; then
    print_error "run_jpii.sh not found"
    exit 1
fi

if [ ! -f "donwload_file.py" ]; then
    print_error "donwload_file.py not found"
    exit 1
fi

print_success "Environment validated"

# Create log directory
mkdir -p "$LOG_DIR"
print_status "Log directory: $LOG_DIR"

# Initialize CSV file
echo "Status,TestID,Books,Reducers,Stage1Time(s),Stage2Time(s),TotalTime(s),InputSize(bytes),IndexSize(bytes),JPIISize(bytes),UniqueWords,SimilarDocs,Throughput(books/s)" > "$CSV_OUTPUT"
print_status "CSV output: $CSV_OUTPUT"

# Calculate total tests
total_tests=$((${#DATASET_SIZES[@]} * ${#REDUCER_COUNTS[@]}))
current_test=0
successful_tests=0
failed_tests=0

print_header "Starting Benchmark (${total_tests} tests total)"

# Benchmark start time
benchmark_start=$(date +%s)

# Run all benchmark combinations
for num_books in "${DATASET_SIZES[@]}"; do
    for num_reducers in "${REDUCER_COUNTS[@]}"; do
        current_test=$((current_test + 1))

        echo ""
        print_status "Progress: Test $current_test of $total_tests"

        if run_benchmark $num_books $num_reducers; then
            successful_tests=$((successful_tests + 1))
        else
            failed_tests=$((failed_tests + 1))
            print_warning "Test failed but continuing with remaining tests..."
        fi

        # Brief pause between tests
        sleep 2
    done
done

# Benchmark end time
benchmark_end=$(date +%s)
benchmark_total_time=$((benchmark_end - benchmark_start))

################################################################################
# Final Summary
################################################################################

print_header "Benchmark Complete!"

echo ""
print_metric "Total Tests Run" "$total_tests"
print_metric "Successful Tests" "$successful_tests"
print_metric "Failed Tests" "$failed_tests"
print_metric "Total Benchmark Time" "${benchmark_total_time}s ($(echo "scale=1; $benchmark_total_time / 60" | bc) minutes)"
print_metric "Results CSV" "$CSV_OUTPUT"
print_metric "Logs Directory" "$LOG_DIR"

echo ""
print_header "Results Summary Table"

# Display summary table
echo ""
printf "${CYAN}%-15s %-10s %-12s %-12s %-12s %-15s${NC}\n" \
    "Books" "Reducers" "Stage1(s)" "Stage2(s)" "Total(s)" "Throughput"
echo "--------------------------------------------------------------------------------"

# Read and display CSV results (skip header)
tail -n +2 "$CSV_OUTPUT" | while IFS=, read -r status test_id books reducers s1 s2 total input_size index_size jpii_size words docs throughput; do
    if [ "$status" = "SUCCESS" ]; then
        color=$GREEN
    else
        color=$RED
    fi
    printf "${color}%-15s %-10s %-12s %-12s %-12s %-15s${NC}\n" \
        "$books" "$reducers" "$s1" "$s2" "$total" "$throughput books/s"
done

echo ""
print_success "Benchmark results saved to: $CSV_OUTPUT"
print_status "To analyze results:"
echo "  - View CSV in Excel/LibreOffice: $CSV_OUTPUT"
echo "  - View logs: ls -la $LOG_DIR"
echo "  - Plot with Python/R using the CSV data"

echo ""
print_header "Next Steps"
echo ""
echo "1. Analyze the CSV to find optimal reducer count for each dataset size"
echo "2. Look for performance patterns (linear scaling, bottlenecks, etc.)"
echo "3. Check logs in $LOG_DIR for any warnings or errors"
echo "4. Run YARN ResourceManager UI to see detailed job metrics"
echo ""

print_success "All done! 🎉"
