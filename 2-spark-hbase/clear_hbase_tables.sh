#!/bin/bash
#
# Clear HBase Tables Data
#
# This script truncates (clears all data from) HBase tables
# without dropping the table schema. Useful for re-running pipelines.
#
# Usage:
#   bash clear_hbase_tables.sh
#

set -e  # Exit on error

echo "=================================="
echo "Clearing HBase Tables Data"
echo "=================================="

# Check if HBase shell is available
if ! command -v hbase &> /dev/null; then
    echo "[ERROR] HBase command not found. Is HBase installed?"
    exit 1
fi

# Confirmation prompt
echo ""
echo "[WARNING] This will delete ALL data from the following tables:"
echo "  - inverted_index"
echo "  - similarity_scores"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "[INFO] Operation cancelled"
    exit 0
fi

echo ""
echo "[INFO] Truncating 'inverted_index' table..."

hbase shell <<EOF
truncate 'inverted_index'
EOF

echo "[SUCCESS] 'inverted_index' table cleared"

echo ""
echo "[INFO] Truncating 'similarity_scores' table..."

hbase shell <<EOF
truncate 'similarity_scores'
EOF

echo "[SUCCESS] 'similarity_scores' table cleared"

echo ""
echo "=================================="
echo "Verification"
echo "=================================="

echo ""
echo "[INFO] Checking row counts..."

hbase shell <<EOF
count 'inverted_index', INTERVAL => 10000
count 'similarity_scores', INTERVAL => 10000
EOF

echo ""
echo "[SUCCESS] All tables cleared successfully!"
echo ""
echo "Tables are now empty and ready for new data."
echo ""
