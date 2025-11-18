#!/bin/bash
#
# Example script demonstrating how to run the MapReduce pipeline
# with different configurations and queries
#
# Usage: ./example_run.sh [test_size]
#   test_size: quick (default), medium, or full
#

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test size (default: quick)
TEST_SIZE="${1:-quick}"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}MapReduce Pipeline Example Execution${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to run pipeline with query string
run_pipeline() {
    local num_books=$1
    local query=$2
    local reducers=$3

    echo -e "${GREEN}Running pipeline with:${NC}"
    echo "  Books: $num_books"
    echo "  Query: $query"
    echo "  Reducers: $reducers"
    echo ""

    python3 "${SCRIPT_DIR}/run_mapreduce_pipeline.py" \
        --num-books "$num_books" \
        --input-dir "hdfs:///gutenberg-input-$num_books" \
        --query "$query" \
        --num-reducers "$reducers"

    echo ""
    echo -e "${GREEN}✓ Pipeline completed successfully${NC}"
    echo ""
}

# Function to run pipeline with query file
run_pipeline_with_file() {
    local num_books=$1
    local query_file=$2
    local reducers=$3

    echo -e "${GREEN}Running pipeline with:${NC}"
    echo "  Books: $num_books"
    echo "  Query file: $query_file"
    echo "  Reducers: $reducers"
    echo ""

    python3 "${SCRIPT_DIR}/run_mapreduce_pipeline.py" \
        --num-books "$num_books" \
        --input-dir "hdfs:///gutenberg-input-$num_books" \
        --query-file "$query_file" \
        --num-reducers "$reducers"

    echo ""
    echo -e "${GREEN}✓ Pipeline completed successfully${NC}"
    echo ""
}

case "$TEST_SIZE" in
    quick)
        echo -e "${YELLOW}Running QUICK test (10 books, minimal resources)${NC}"
        echo ""

        # Check if input exists
        if ! hdfs dfs -test -e /gutenberg-input-10 2>/dev/null; then
            echo -e "${YELLOW}Warning: /gutenberg-input-10 not found in HDFS${NC}"
            echo "To create it, run:"
            echo "  python3 ../donwload_file.py --num-books 10 --hdfs-dir /gutenberg-input-10"
            echo ""
            exit 1
        fi

        run_pipeline 10 "adventure travel exploration journey" 2
        ;;

    medium)
        echo -e "${YELLOW}Running MEDIUM test (100 books, standard resources)${NC}"
        echo ""

        # Check if input exists
        if ! hdfs dfs -test -e /gutenberg-input-100 2>/dev/null; then
            echo -e "${YELLOW}Warning: /gutenberg-input-100 not found in HDFS${NC}"
            echo "To create it, run:"
            echo "  python3 ../donwload_file.py --num-books 100 --hdfs-dir /gutenberg-input-100"
            echo ""
            exit 1
        fi

        echo -e "${BLUE}Test 1: Wildlife and conservation${NC}"
        run_pipeline 100 "wildlife conservation hunting animals nature" 4

        echo -e "${BLUE}Test 2: Science and technology${NC}"
        run_pipeline 100 "science technology innovation research discovery" 4
        ;;

    full)
        echo -e "${YELLOW}Running FULL test (multiple sizes, various queries)${NC}"
        echo ""

        # Test with 50 books
        if hdfs dfs -test -e /gutenberg-input-50 2>/dev/null; then
            echo -e "${BLUE}Test 1: 50 books - Literature theme${NC}"
            run_pipeline 50 "literature poetry prose writing author" 3
        else
            echo -e "${YELLOW}Skipping 50 books test (input not found)${NC}"
        fi

        # Test with 100 books
        if hdfs dfs -test -e /gutenberg-input-100 2>/dev/null; then
            echo -e "${BLUE}Test 2: 100 books - Historical theme${NC}"
            run_pipeline 100 "history war battle victory defeat empire" 4

            echo -e "${BLUE}Test 3: 100 books - Nature theme (using query file)${NC}"
            # Create a temporary query file
            QUERY_FILE="/tmp/nature_query_$$.txt"
            echo "nature wilderness forest mountain river ocean environment wildlife conservation" > "$QUERY_FILE"
            run_pipeline_with_file 100 "$QUERY_FILE" 4
            rm -f "$QUERY_FILE"
        else
            echo -e "${YELLOW}Skipping 100 books tests (input not found)${NC}"
        fi

        # Test with 200 books (if available)
        if hdfs dfs -test -e /gutenberg-input-200 2>/dev/null; then
            echo -e "${BLUE}Test 4: 200 books - Philosophy theme${NC}"
            run_pipeline 200 "philosophy truth knowledge wisdom reason thought" 6
        else
            echo -e "${YELLOW}Skipping 200 books test (input not found)${NC}"
        fi
        ;;

    *)
        echo "Usage: $0 [test_size]"
        echo ""
        echo "test_size options:"
        echo "  quick  - Fast test with 10 books (default)"
        echo "  medium - Standard test with 100 books, 2 queries"
        echo "  full   - Comprehensive test with multiple sizes and queries"
        echo ""
        exit 1
        ;;
esac

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}All tests completed!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "View results:"
echo "  cat ${SCRIPT_DIR}/mapreduce_metrics.csv"
echo ""
echo "View latest similarity results:"
echo "  hdfs dfs -ls /mapreduce-jpii-* | tail -1"
echo "  hdfs dfs -cat /mapreduce-jpii-*/part-* | sort -t\$'\\t' -k2 -rn | head -20"
echo ""
