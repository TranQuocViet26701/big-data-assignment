#!/bin/bash
################################################################################
# Spark-HBase Pipeline Benchmark Script
#
# Automatically runs the Spark-HBase pipeline in both JPII and Pairwise modes
# across multiple dataset sizes for performance comparison and metrics collection.
#
# Usage:
#   ./benchmark_spark_hbase_modes.sh [OPTIONS]
#
# Options:
#   --mode MODE          Run only specific mode: jpii, pairwise, or both (default: both)
#   --datasets "N1 N2"   Space-separated list of dataset sizes (default: "10 50 100 200")
#   --query-file FILE    Query file for JPII mode (default: my_query.txt)
#   --thrift-host HOST   HBase Thrift server host (default: hadoop-master)
#   --thrift-port PORT   HBase Thrift server port (default: 9090)
#   --executors N        Number of Spark executors (default: 4)
#   --executor-memory M  Memory per executor (default: 4G)
#   --help               Show this help message
#
# Examples:
#   # Run both modes on all datasets
#   ./benchmark_spark_hbase_modes.sh
#
#   # Run only JPII mode
#   ./benchmark_spark_hbase_modes.sh --mode jpii
#
#   # Run only pairwise on small datasets
#   ./benchmark_spark_hbase_modes.sh --mode pairwise --datasets "10 50"
#
#   # Custom query file and configuration
#   ./benchmark_spark_hbase_modes.sh --query-file custom_query.txt --executors 8
################################################################################

set -e  # Exit on error

# ============================================================================
# Default Configuration
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="both"
DATASETS="10 50 100 200"
QUERY_FILE="my_query.txt"
THRIFT_HOST="hadoop-master"
THRIFT_PORT="9090"
NUM_EXECUTORS=4
EXECUTOR_MEMORY="4G"
DRIVER_MEMORY="2G"

# ============================================================================
# Color codes for output
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

print_header() {
    echo -e "${CYAN}================================================================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}================================================================================${NC}"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    head -n 33 "$0" | grep "^#" | sed 's/^# \?//'
    exit 0
}

# ============================================================================
# Parse Command Line Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --datasets)
            DATASETS="$2"
            shift 2
            ;;
        --query-file)
            QUERY_FILE="$2"
            shift 2
            ;;
        --thrift-host)
            THRIFT_HOST="$2"
            shift 2
            ;;
        --thrift-port)
            THRIFT_PORT="$2"
            shift 2
            ;;
        --executors)
            NUM_EXECUTORS="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --help)
            show_help
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            ;;
    esac
done

# ============================================================================
# Validation
# ============================================================================

print_header "Benchmark Configuration"
echo "Mode:              $MODE"
echo "Datasets:          $DATASETS"
echo "Query File:        $QUERY_FILE"
echo "HBase Thrift:      $THRIFT_HOST:$THRIFT_PORT"
echo "Executors:         $NUM_EXECUTORS"
echo "Executor Memory:   $EXECUTOR_MEMORY"
echo "Driver Memory:     $DRIVER_MEMORY"
echo ""

# Validate mode
if [[ "$MODE" != "jpii" && "$MODE" != "pairwise" && "$MODE" != "both" ]]; then
    print_error "Invalid mode: $MODE. Must be 'jpii', 'pairwise', or 'both'"
    exit 1
fi

# Validate query file for JPII mode
if [[ "$MODE" == "jpii" || "$MODE" == "both" ]]; then
    # Check in parent directory first
    PARENT_QUERY="../$QUERY_FILE"
    if [[ -f "$PARENT_QUERY" ]]; then
        QUERY_FILE="$PARENT_QUERY"
        print_success "Query file found: $QUERY_FILE"
    elif [[ -f "$QUERY_FILE" ]]; then
        print_success "Query file found: $QUERY_FILE"
    else
        print_error "Query file not found: $QUERY_FILE (also checked $PARENT_QUERY)"
        print_info "Create a query file or specify a different one with --query-file"
        exit 1
    fi
fi

# Test HBase connection
print_info "Testing HBase connection to $THRIFT_HOST:$THRIFT_PORT..."
if python3 -c "from hbase_connector import test_connection; import sys; sys.exit(0 if test_connection('$THRIFT_HOST', $THRIFT_PORT) else 1)" >/dev/null 2>&1; then
    print_success "HBase connection successful"
else
    print_error "Cannot connect to HBase Thrift server at $THRIFT_HOST:$THRIFT_PORT"
    print_info "Make sure HBase Thrift server is running"
    exit 1
fi

# Validate HDFS directories
print_info "Validating HDFS input directories..."
for NUM_BOOKS in $DATASETS; do
    HDFS_INPUT="hdfs:///gutenberg-input-$NUM_BOOKS"
    if ! hdfs dfs -test -e "$HDFS_INPUT" 2>/dev/null; then
        print_error "HDFS directory not found: $HDFS_INPUT"
        print_info "Available directories:"
        hdfs dfs -ls / 2>/dev/null | grep "gutenberg-input" || echo "  (none found)"
        exit 1
    fi
    print_success "Found: $HDFS_INPUT"
done

echo ""

# ============================================================================
# Benchmark Execution
# ============================================================================

START_TIME=$(date +%s)
TOTAL_RUNS=0
SUCCESSFUL_RUNS=0
FAILED_RUNS=0

# Count total runs
if [[ "$MODE" == "both" ]]; then
    TOTAL_RUNS=$(( $(echo $DATASETS | wc -w) * 2 ))
else
    TOTAL_RUNS=$(echo $DATASETS | wc -w)
fi

CURRENT_RUN=0

print_header "Starting Benchmark - $TOTAL_RUNS Total Runs"

# ============================================================================
# Run JPII Mode
# ============================================================================

if [[ "$MODE" == "jpii" || "$MODE" == "both" ]]; then
    print_header "Running JPII Mode (Query-based Similarity with HBase)"

    for NUM_BOOKS in $DATASETS; do
        CURRENT_RUN=$((CURRENT_RUN + 1))

        echo ""
        print_info "[$CURRENT_RUN/$TOTAL_RUNS] Running JPII with $NUM_BOOKS books..."

        RUN_START=$(date +%s)

        if python3 "$SCRIPT_DIR/run_spark_hbase_pipeline.py" \
            --mode jpii \
            --num-books "$NUM_BOOKS" \
            --input-dir "hdfs:///gutenberg-input-$NUM_BOOKS" \
            --query-file "$QUERY_FILE" \
            --thrift-host "$THRIFT_HOST" \
            --thrift-port "$THRIFT_PORT" \
            --num-executors "$NUM_EXECUTORS" \
            --executor-memory "$EXECUTOR_MEMORY" \
            --driver-memory "$DRIVER_MEMORY"; then

            RUN_END=$(date +%s)
            RUN_DURATION=$((RUN_END - RUN_START))

            print_success "JPII ($NUM_BOOKS books) completed in ${RUN_DURATION}s"
            SUCCESSFUL_RUNS=$((SUCCESSFUL_RUNS + 1))
        else
            print_error "JPII ($NUM_BOOKS books) failed"
            FAILED_RUNS=$((FAILED_RUNS + 1))
        fi

        echo ""
    done
fi

# ============================================================================
# Run Pairwise Mode
# ============================================================================

if [[ "$MODE" == "pairwise" || "$MODE" == "both" ]]; then
    print_header "Running Pairwise Mode (All-Pairs Similarity with HBase)"

    for NUM_BOOKS in $DATASETS; do
        CURRENT_RUN=$((CURRENT_RUN + 1))

        echo ""
        print_info "[$CURRENT_RUN/$TOTAL_RUNS] Running Pairwise with $NUM_BOOKS books..."

        # Calculate expected pairs for info
        TOTAL_PAIRS=$(( NUM_BOOKS * (NUM_BOOKS - 1) / 2 ))
        print_info "Expected pairs: $TOTAL_PAIRS"

        RUN_START=$(date +%s)

        if python3 "$SCRIPT_DIR/run_spark_hbase_pipeline.py" \
            --mode pairwise \
            --num-books "$NUM_BOOKS" \
            --input-dir "hdfs:///gutenberg-input-$NUM_BOOKS" \
            --thrift-host "$THRIFT_HOST" \
            --thrift-port "$THRIFT_PORT" \
            --num-executors "$NUM_EXECUTORS" \
            --executor-memory "$EXECUTOR_MEMORY" \
            --driver-memory "$DRIVER_MEMORY"; then

            RUN_END=$(date +%s)
            RUN_DURATION=$((RUN_END - RUN_START))

            print_success "Pairwise ($NUM_BOOKS books) completed in ${RUN_DURATION}s"
            SUCCESSFUL_RUNS=$((SUCCESSFUL_RUNS + 1))
        else
            print_error "Pairwise ($NUM_BOOKS books) failed"
            FAILED_RUNS=$((FAILED_RUNS + 1))
        fi

        echo ""
    done
fi

# ============================================================================
# Summary Report
# ============================================================================

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

print_header "Benchmark Summary"
echo "Total Runs:        $TOTAL_RUNS"
echo "Successful:        $SUCCESSFUL_RUNS"
echo "Failed:            $FAILED_RUNS"
echo "Total Duration:    ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""

if [[ "$MODE" == "jpii" || "$MODE" == "both" ]]; then
    echo "JPII Metrics:      $SCRIPT_DIR/metrics/spark_hbase_jpii_metrics.csv"
fi

if [[ "$MODE" == "pairwise" || "$MODE" == "both" ]]; then
    echo "Pairwise Metrics:  $SCRIPT_DIR/metrics/spark_hbase_pairwise_metrics.csv"
fi

echo ""

# ============================================================================
# Quick Results Preview
# ============================================================================

print_header "Quick Results Preview"

if [[ "$MODE" == "jpii" || "$MODE" == "both" ]] && [[ -f "$SCRIPT_DIR/metrics/spark_hbase_jpii_metrics.csv" ]]; then
    echo ""
    print_info "JPII Results (last runs):"
    echo ""
    echo "Books | Stage1(s) | Stage2(s) | Total(s) | Pairs | Throughput(books/s)"
    echo "------|-----------|-----------|----------|-------|--------------------"
    tail -n $(( SUCCESSFUL_RUNS > 0 ? SUCCESSFUL_RUNS : 5 )) "$SCRIPT_DIR/metrics/spark_hbase_jpii_metrics.csv" | \
        awk -F',' 'NR>1 {printf "%5s | %9.2f | %9.2f | %8.2f | %5s | %18.4f\n", $3, $5, $6, $7, $10, $11}'
fi

if [[ "$MODE" == "pairwise" || "$MODE" == "both" ]] && [[ -f "$SCRIPT_DIR/metrics/spark_hbase_pairwise_metrics.csv" ]]; then
    echo ""
    print_info "Pairwise Results (last runs):"
    echo ""
    echo "Books | Stage1(s) | Stage2(s) | Total(s) | Pairs | Throughput(pairs/s)"
    echo "------|-----------|-----------|----------|-------|--------------------"
    tail -n $(( SUCCESSFUL_RUNS > 0 ? SUCCESSFUL_RUNS : 5 )) "$SCRIPT_DIR/metrics/spark_hbase_pairwise_metrics.csv" | \
        awk -F',' 'NR>1 {printf "%5s | %9.2f | %9.2f | %8.2f | %5s | %18.2f\n", $3, $5, $6, $7, $10, $12}'
fi

echo ""

# ============================================================================
# HBase Statistics
# ============================================================================

print_header "HBase Statistics"
print_info "Querying HBase table statistics..."

# Get inverted index stats
if python3 -c "from hbase_connector import HBaseConnector; c = HBaseConnector('$THRIFT_HOST', $THRIFT_PORT); print(f'Inverted Index Terms: {c.get_row_count(\"inverted_index\"):,}'); c.close()" 2>/dev/null; then
    :
else
    print_warning "Could not retrieve inverted_index statistics"
fi

# Get similarity scores stats
if python3 -c "from hbase_connector import HBaseConnector; c = HBaseConnector('$THRIFT_HOST', $THRIFT_PORT); print(f'Similarity Scores: {c.get_row_count(\"similarity_scores\"):,}'); c.close()" 2>/dev/null; then
    :
else
    print_warning "Could not retrieve similarity_scores statistics"
fi

echo ""

# ============================================================================
# Exit
# ============================================================================

if [[ $FAILED_RUNS -eq 0 ]]; then
    print_success "All benchmarks completed successfully!"
    exit 0
else
    print_warning "Benchmark completed with $FAILED_RUNS failed run(s)"
    exit 1
fi
