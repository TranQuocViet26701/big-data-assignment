#!/bin/bash
################################################################################
# MapReduce Pipeline Benchmark Script
#
# Automatically runs the MapReduce pipeline in both JPII and Pairwise modes
# across multiple dataset sizes for performance comparison and metrics collection.
#
# Optimized for 3-node cluster (12 vCPU, 48GB RAM total)
#
# Usage:
#   ./benchmark_mapreduce_modes.sh [OPTIONS]
#
# Options:
#   --mode MODE          Run only specific mode: jpii, pairwise, or both (default: both)
#   --datasets "N1 N2"   Space-separated list of dataset sizes (default: "10 50 100 200")
#   --query-file FILE    Query file for JPII mode (default: my_query.txt)
#   --num-reducers N     Number of reducers (default: auto-scale)
#   --help               Show this help message
#
# Examples:
#   # Run both modes on all datasets
#   ./benchmark_mapreduce_modes.sh
#
#   # Run only JPII mode
#   ./benchmark_mapreduce_modes.sh --mode jpii
#
#   # Run only pairwise on small datasets
#   ./benchmark_mapreduce_modes.sh --mode pairwise --datasets "10 50"
#
#   # Custom query file and configuration
#   ./benchmark_mapreduce_modes.sh --query-file custom_query.txt --num-reducers 8
################################################################################

set -e  # Exit on error

# ============================================================================
# Default Configuration
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="both"
DATASETS="10 50 100 200"
QUERY_FILE="my_query.txt"
NUM_REDUCERS=""  # Empty means auto-scale

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
    head -n 35 "$0" | grep "^#" | sed 's/^# \?//'
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
        --num-reducers)
            NUM_REDUCERS="$2"
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
echo "Reducers:          ${NUM_REDUCERS:-auto-scale}"
echo "Cluster:           3 nodes, 12 vCPU, 48GB RAM total"
echo ""

# Validate mode
if [[ "$MODE" != "jpii" && "$MODE" != "pairwise" && "$MODE" != "both" ]]; then
    print_error "Invalid mode: $MODE. Must be 'jpii', 'pairwise', or 'both'"
    exit 1
fi

# Validate query file for JPII mode
if [[ "$MODE" == "jpii" || "$MODE" == "both" ]]; then
    if [[ ! -f "$QUERY_FILE" ]]; then
        print_error "Query file not found: $QUERY_FILE"
        print_info "Create a query file or specify a different one with --query-file"
        exit 1
    fi
    print_success "Query file found: $QUERY_FILE"
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
    print_header "Running JPII Mode (Query-based Similarity)"

    for NUM_BOOKS in $DATASETS; do
        CURRENT_RUN=$((CURRENT_RUN + 1))

        echo ""
        print_info "[$CURRENT_RUN/$TOTAL_RUNS] Running JPII with $NUM_BOOKS books..."

        RUN_START=$(date +%s)

        # Build command
        CMD="python3 \"$SCRIPT_DIR/run_mapreduce_pipeline.py\" \
            --mode jpii \
            --num-books \"$NUM_BOOKS\" \
            --input-dir \"hdfs:///gutenberg-input-$NUM_BOOKS\" \
            --query-file \"$QUERY_FILE\""

        # Add num-reducers if specified
        if [[ -n "$NUM_REDUCERS" ]]; then
            CMD="$CMD --num-reducers \"$NUM_REDUCERS\""
        fi

        if eval $CMD; then
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
    print_header "Running Pairwise Mode (All-Pairs Similarity)"

    for NUM_BOOKS in $DATASETS; do
        CURRENT_RUN=$((CURRENT_RUN + 1))

        echo ""
        print_info "[$CURRENT_RUN/$TOTAL_RUNS] Running Pairwise with $NUM_BOOKS books..."

        # Calculate expected pairs for info
        TOTAL_PAIRS=$(( NUM_BOOKS * (NUM_BOOKS - 1) / 2 ))
        print_info "Expected pairs: $TOTAL_PAIRS"

        RUN_START=$(date +%s)

        # Build command
        CMD="python3 \"$SCRIPT_DIR/run_mapreduce_pipeline.py\" \
            --mode pairwise \
            --num-books \"$NUM_BOOKS\" \
            --input-dir \"hdfs:///gutenberg-input-$NUM_BOOKS\" \
            --query-file \"$QUERY_FILE\""

        # Add num-reducers if specified
        if [[ -n "$NUM_REDUCERS" ]]; then
            CMD="$CMD --num-reducers \"$NUM_REDUCERS\""
        fi

        if eval $CMD; then
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
    echo "JPII Metrics:      $SCRIPT_DIR/mapreduce_jpii_metrics.csv"
fi

if [[ "$MODE" == "pairwise" || "$MODE" == "both" ]]; then
    echo "Pairwise Metrics:  $SCRIPT_DIR/mapreduce_pairwise_metrics.csv"
fi

echo ""

# ============================================================================
# Quick Results Preview
# ============================================================================

print_header "Quick Results Preview"

if [[ "$MODE" == "jpii" || "$MODE" == "both" ]] && [[ -f "$SCRIPT_DIR/mapreduce_jpii_metrics.csv" ]]; then
    echo ""
    print_info "JPII Results (last runs):"
    echo ""
    echo "Books | Reducers | Stage1(s) | Stage2(s) | Total(s) | Pairs | Throughput"
    echo "------|----------|-----------|-----------|----------|-------|------------"
    tail -n 10 "$SCRIPT_DIR/mapreduce_jpii_metrics.csv" | \
        awk -F',' 'NR>1 {printf "%5s | %8s | %9.2f | %9.2f | %8.2f | %5s | %10.4f\n", $3, $13, $4, $5, $6, $10, $11}'
fi

if [[ "$MODE" == "pairwise" || "$MODE" == "both" ]] && [[ -f "$SCRIPT_DIR/mapreduce_pairwise_metrics.csv" ]]; then
    echo ""
    print_info "Pairwise Results (last runs):"
    echo ""
    echo "Books | Reducers | Stage1(s) | Stage2(s) | Total(s) | Pairs | Throughput(pairs/s)"
    echo "------|----------|-----------|-----------|----------|-------|-------------------"
    tail -n 10 "$SCRIPT_DIR/mapreduce_pairwise_metrics.csv" | \
        awk -F',' 'NR>1 {printf "%5s | %8s | %9.2f | %9.2f | %8.2f | %5s | %18.2f\n", $3, $16, $5, $6, $7, $12, $15}'
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
