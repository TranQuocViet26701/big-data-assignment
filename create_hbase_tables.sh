#!/bin/bash

################################################################################
# HBase Table Creation Script for MapReduce Similarity Search
#
# Creates HBase tables to store:
#   1. Inverted Index (term → documents mapping)
#   2. Similarity Scores (query → document similarities)
#
# Usage: ./create_hbase_tables.sh [--recreate]
#
# Options:
#   --recreate    Drop existing tables and recreate them
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

# Parse arguments
RECREATE=false
if [[ "$1" == "--recreate" ]]; then
    RECREATE=true
fi

print_header "HBase Table Setup for Similarity Search"

# Check if HBase is accessible
print_status "Checking HBase connectivity..."
if ! echo "status" | hbase shell -n 2>/dev/null | grep -q "active"; then
    print_error "Cannot connect to HBase. Please ensure HBase is running."
    print_status "Check with: hbase shell"
    exit 1
fi
print_success "HBase is accessible"

################################################################################
# Table 1: Inverted Index
################################################################################

print_header "Creating Inverted Index Table"

TABLE_INVERTED_INDEX="inverted_index"

print_status "Table: $TABLE_INVERTED_INDEX"
print_status "Purpose: Store term → documents mapping"
print_status "Schema:"
echo "  Row Key: term (e.g., 'elephant')"
echo "  Column Family: docs"
echo "  Columns: docs:<document_name>"
echo "  Value: word_count (e.g., '5234')"
echo ""

# Check if table exists
if echo "exists '$TABLE_INVERTED_INDEX'" | hbase shell -n 2>/dev/null | grep -q "true"; then
    if [ "$RECREATE" = true ]; then
        print_warning "Table $TABLE_INVERTED_INDEX already exists. Dropping..."
        echo "disable '$TABLE_INVERTED_INDEX'" | hbase shell -n 2>/dev/null
        echo "drop '$TABLE_INVERTED_INDEX'" | hbase shell -n 2>/dev/null
        print_success "Dropped existing table"
    else
        print_warning "Table $TABLE_INVERTED_INDEX already exists. Skipping creation."
        print_status "Use --recreate flag to drop and recreate"
        SKIP_INVERTED_INDEX=true
    fi
fi

if [ "$SKIP_INVERTED_INDEX" != true ]; then
    print_status "Creating table $TABLE_INVERTED_INDEX..."

    hbase shell <<EOF
create '$TABLE_INVERTED_INDEX',
  {NAME => 'docs', VERSIONS => 1, COMPRESSION => 'GZ', BLOCKCACHE => true}
EOF

    if [ $? -eq 0 ]; then
        print_success "Table $TABLE_INVERTED_INDEX created successfully"
    else
        print_error "Failed to create table $TABLE_INVERTED_INDEX"
        exit 1
    fi
fi

################################################################################
# Table 2: Similarity Scores
################################################################################

print_header "Creating Similarity Scores Table"

TABLE_SIMILARITY="similarity_scores"

print_status "Table: $TABLE_SIMILARITY"
print_status "Purpose: Store document similarity scores for queries"
print_status "Schema:"
echo "  Row Key: <query_hash>:<document_name> (e.g., 'abc123:doc1.txt')"
echo "  Column Families:"
echo "    - score: Similarity metrics"
echo "    - meta: Query metadata"
echo "  Columns:"
echo "    - score:jaccard       → Jaccard similarity score"
echo "    - score:shared_terms  → Number of shared terms"
echo "    - meta:query_text     → Original query text"
echo "    - meta:timestamp      → When computed"
echo ""

# Check if table exists
if echo "exists '$TABLE_SIMILARITY'" | hbase shell -n 2>/dev/null | grep -q "true"; then
    if [ "$RECREATE" = true ]; then
        print_warning "Table $TABLE_SIMILARITY already exists. Dropping..."
        echo "disable '$TABLE_SIMILARITY'" | hbase shell -n 2>/dev/null
        echo "drop '$TABLE_SIMILARITY'" | hbase shell -n 2>/dev/null
        print_success "Dropped existing table"
    else
        print_warning "Table $TABLE_SIMILARITY already exists. Skipping creation."
        print_status "Use --recreate flag to drop and recreate"
        SKIP_SIMILARITY=true
    fi
fi

if [ "$SKIP_SIMILARITY" != true ]; then
    print_status "Creating table $TABLE_SIMILARITY..."

    hbase shell <<EOF
create '$TABLE_SIMILARITY',
  {NAME => 'score', VERSIONS => 1, COMPRESSION => 'GZ', BLOCKCACHE => true},
  {NAME => 'meta', VERSIONS => 5, COMPRESSION => 'GZ', BLOCKCACHE => true}
EOF

    if [ $? -eq 0 ]; then
        print_success "Table $TABLE_SIMILARITY created successfully"
    else
        print_error "Failed to create table $TABLE_SIMILARITY"
        exit 1
    fi
fi

################################################################################
# Verification
################################################################################

print_header "Verification"

print_status "Listing created tables..."
echo ""
echo "list" | hbase shell -n 2>/dev/null | grep -E "(inverted_index|similarity_scores)"
echo ""

print_status "Table descriptions..."
echo ""

if [ "$SKIP_INVERTED_INDEX" != true ] || [ "$RECREATE" = true ]; then
    echo "describe '$TABLE_INVERTED_INDEX'" | hbase shell -n 2>/dev/null | grep -A 10 "COLUMN FAMILIES"
    echo ""
fi

if [ "$SKIP_SIMILARITY" != true ] || [ "$RECREATE" = true ]; then
    echo "describe '$TABLE_SIMILARITY'" | hbase shell -n 2>/dev/null | grep -A 15 "COLUMN FAMILIES"
    echo ""
fi

################################################################################
# Summary
################################################################################

print_header "Setup Complete!"

echo ""
print_success "HBase tables are ready for use"
echo ""
print_status "Tables created:"
echo "  1. $TABLE_INVERTED_INDEX  - Stores inverted index (term → documents)"
echo "  2. $TABLE_SIMILARITY      - Stores similarity scores (query → documents)"
echo ""
print_status "Next steps:"
echo "  1. Run MapReduce with HBase reducers:"
echo "     ./run_hbase_pipeline.sh --num-books 100 --reducers 4"
echo ""
echo "  2. Query similarity in real-time:"
echo "     python3 query_similarity.py \"your search query\""
echo ""
echo "  3. Verify data in HBase shell:"
echo "     hbase shell"
echo "     scan '$TABLE_INVERTED_INDEX', {LIMIT => 10}"
echo "     scan '$TABLE_SIMILARITY', {LIMIT => 10}"
echo ""

print_status "To drop tables and recreate:"
echo "  ./create_hbase_tables.sh --recreate"
echo ""

print_success "All done! 🎉"
