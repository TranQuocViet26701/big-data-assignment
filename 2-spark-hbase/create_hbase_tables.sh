#!/bin/bash
#
# Create HBase Tables for Spark-HBase Pipeline
#
# This script creates the required HBase tables with proper schema:
#   - inverted_index: Stores term -> documents mapping
#   - similarity_scores: Stores document similarity results
#
# Usage:
#   bash create_hbase_tables.sh
#

set -e  # Exit on error

echo "=================================="
echo "Creating HBase Tables"
echo "=================================="

# Check if HBase shell is available
if ! command -v hbase &> /dev/null; then
    echo "[ERROR] HBase command not found. Is HBase installed?"
    exit 1
fi

echo ""
echo "[INFO] Creating 'inverted_index' table..."
echo "       Row Key: term (word)"
echo "       Column Family: docs (documents containing this term)"
echo "       Columns: docs:<document_name> = word_count"

hbase shell <<EOF
# Disable table if exists (for re-creation)
disable 'inverted_index' if exists
drop 'inverted_index' if exists

# Create table with single column family
create 'inverted_index', 'docs'

# Verify creation
describe 'inverted_index'
EOF

echo ""
echo "[SUCCESS] 'inverted_index' table created successfully"

echo ""
echo "[INFO] Creating 'similarity_scores' table..."
echo "       Row Key: <mode>:<doc1>-<doc2>"
echo "       Column Family: score (similarity metrics)"
echo "       Column Family: meta (metadata)"

hbase shell <<EOF
# Disable table if exists (for re-creation)
disable 'similarity_scores' if exists
drop 'similarity_scores' if exists

# Create table with two column families
create 'similarity_scores', 'score', 'meta'

# Verify creation
describe 'similarity_scores'
EOF

echo ""
echo "[SUCCESS] 'similarity_scores' table created successfully"

echo ""
echo "=================================="
echo "HBase Tables Summary"
echo "=================================="

hbase shell <<EOF
list
EOF

echo ""
echo "[SUCCESS] All tables created successfully!"
echo ""
echo "Next steps:"
echo "  1. Start HBase Thrift server:"
echo "     hbase thrift start -p 9090"
echo ""
echo "  2. Run the pipeline:"
echo "     python3 run_spark_hbase_pipeline.py --mode jpii --num-books 100 \\"
echo "         --input-dir hdfs:///gutenberg-input-100 \\"
echo "         --query-file my_query.txt"
echo ""
