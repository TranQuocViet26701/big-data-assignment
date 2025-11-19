#!/bin/bash
#
# Check HBase Status and Tables
#
# This script checks:
#   - HBase Thrift server status
#   - HBase tables existence
#   - Row counts in tables
#   - Sample data from tables
#
# Usage:
#   bash check_hbase_status.sh
#

set -e  # Exit on error

echo "=================================="
echo "HBase Status Check"
echo "=================================="

# Check if HBase is available
if ! command -v hbase &> /dev/null; then
    echo "[ERROR] HBase command not found. Is HBase installed?"
    exit 1
fi

# Check Thrift server
echo ""
echo "[INFO] Checking HBase Thrift server (port 9090)..."

if netstat -an 2>/dev/null | grep -q ":9090.*LISTEN"; then
    echo "[SUCCESS] Thrift server is running on port 9090"
elif ss -an 2>/dev/null | grep -q ":9090.*LISTEN"; then
    echo "[SUCCESS] Thrift server is running on port 9090"
else
    echo "[WARNING] Thrift server does not appear to be running on port 9090"
    echo "          Start it with: hbase thrift start -p 9090"
fi

# List all tables
echo ""
echo "[INFO] Listing all HBase tables..."

hbase shell <<EOF
list
EOF

# Check if required tables exist
echo ""
echo "[INFO] Checking required tables..."

TABLES_EXIST=true

hbase shell <<EOF 2>&1 | grep -q "inverted_index" || TABLES_EXIST=false
list
EOF

if [ "$TABLES_EXIST" = true ]; then
    echo "[SUCCESS] Required tables found"
else
    echo "[WARNING] Some required tables may be missing"
    echo "          Create them with: bash create_hbase_tables.sh"
fi

# Get row counts
echo ""
echo "=================================="
echo "Table Statistics"
echo "=================================="

echo ""
echo "[INFO] Counting rows in 'inverted_index'..."

hbase shell <<EOF
count 'inverted_index', INTERVAL => 10000
EOF

echo ""
echo "[INFO] Counting rows in 'similarity_scores'..."

hbase shell <<EOF
count 'similarity_scores', INTERVAL => 10000
EOF

# Show sample data
echo ""
echo "=================================="
echo "Sample Data"
echo "=================================="

echo ""
echo "[INFO] Sample data from 'inverted_index' (first 5 rows)..."

hbase shell <<EOF
scan 'inverted_index', {LIMIT => 5}
EOF

echo ""
echo "[INFO] Sample data from 'similarity_scores' (first 5 rows)..."

hbase shell <<EOF
scan 'similarity_scores', {LIMIT => 5}
EOF

# Python connectivity test
echo ""
echo "=================================="
echo "Python HappyBase Connectivity Test"
echo "=================================="

echo ""
echo "[INFO] Testing Python HappyBase connection..."

python3 <<PYTHON
import sys
sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')

try:
    from hbase_connector import test_connection
    success = test_connection('localhost', 9090)
    if success:
        print("\n[SUCCESS] Python HappyBase connection works!")
        sys.exit(0)
    else:
        print("\n[ERROR] Python HappyBase connection failed!")
        sys.exit(1)
except Exception as e:
    print(f"\n[ERROR] Python test failed: {e}")
    sys.exit(1)
PYTHON

echo ""
echo "=================================="
echo "Status Check Complete"
echo "=================================="
echo ""
