# Spark-HBase Implementation Status

## ✅ Completed Files

### 1. `hbase_connector.py` - HappyBase Wrapper
**Status**: ✅ Complete
- HBase connection management via Thrift
- Batch write/read operations for inverted index
- Batch write/read operations for similarity scores
- Query similarity scores with filtering
- Connection testing utilities

### 2. `spark_hbase_inverted_index.py` - Stage 1
**Status**: ✅ Complete
- Reads documents from HDFS
- Processes with stopword removal
- Writes inverted index to HBase table `inverted_index`
- Uses mapPartitions for efficient batch writes
- Filters common/rare terms

## 📋 Files Still Needed

### Core Spark Scripts

#### 3. `spark_hbase_jpii.py` - Stage 2 JPII
**Purpose**: Query-based similarity using HBase
**Key Features Needed**:
- Read inverted index from HBase
- Process query file
- Generate document pairs
- Compute Jaccard similarity
- Write results to HBase `similarity_scores` table

**Implementation Pattern**:
```python
# Read terms for query from HBase
# Use mapPartitions to batch read from HBase
# Generate pairs and compute similarity
# Write results to HBase similarity_scores table
```

#### 4. `spark_hbase_pairwise.py` - Stage 2 Pairwise
**Purpose**: All-pairs similarity using HBase
**Key Features Needed**:
- Read entire inverted index from HBase
- Generate all document pairs
- Compute Jaccard for all pairs
- Write to HBase `similarity_scores` table

### Pipeline Management

#### 5. `run_spark_hbase_pipeline.py` - Orchestration
**Purpose**: Run complete pipeline with metrics
**Key Features Needed**:
- Check HBase tables exist
- Check Thrift server is running
- Run Stage 1 (inverted index)
- Run Stage 2 (JP II or Pairwise based on mode)
- Collect metrics from HBase
- Save to mode-specific CSV files

**Command-line Interface**:
```bash
python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file my_query.txt \
    --thrift-host localhost \
    --thrift-port 9090
```

#### 6. `query_similarity.py` - Real-time Queries
**Purpose**: Query HBase without Spark
**Key Features Needed**:
- Pure Python using HappyBase
- Query by mode (jpii/pairwise)
- Filter by document name
- Filter by similarity threshold
- Sort and limit results
- Export to CSV/JSON

**Usage**:
```bash
# Query JPII results
python3 query_similarity.py --mode jpii --document query.txt --top 20

# Query pairwise above threshold
python3 query_similarity.py --mode pairwise --threshold 0.1
```

### HBase Setup Scripts

#### 7. `create_hbase_tables.sh`
**Purpose**: Create HBase tables with proper schema
```bash
#!/bin/bash
echo "create 'inverted_index', 'docs'" | hbase shell
echo "create 'similarity_scores', 'score', 'meta'" | hbase shell
```

#### 8. `clear_hbase_tables.sh`
**Purpose**: Truncate tables for re-runs
```bash
#!/bin/bash
echo "truncate 'inverted_index'" | hbase shell
echo "truncate 'similarity_scores'" | hbase shell
```

#### 9. `check_hbase_status.sh`
**Purpose**: Check HBase tables and Thrift server
```bash
#!/bin/bash
# Check Thrift server
netstat -an | grep 9090

# List tables
echo "list" | hbase shell

# Count rows
echo "count 'inverted_index'" | hbase shell
echo "count 'similarity_scores'" | hbase shell
```

### Automation

#### 10. `benchmark_spark_hbase_modes.sh`
**Purpose**: Automated testing like HDFS version
**Features**:
- Run JPII and Pairwise modes
- Test with datasets: 10, 50, 100, 200 books
- Collect metrics
- Generate summary report

### Documentation

#### 11. `README.md`
**Sections Needed**:
- Quick start guide
- Requirements (HappyBase, Thrift server)
- Table setup instructions
- Running the pipeline
- Querying results
- Troubleshooting

#### 12. `WORKFLOWS.md`
**Sections Needed**:
- JPII vs Pairwise for HBase
- Real-time query advantages
- Performance characteristics
- When to use HBase vs HDFS
- HBase-specific optimizations
- Examples and use cases

#### 13. `requirements.txt`
```
happybase>=1.2.0
thrift>=0.13.0
pyspark>=3.0.0
```

## Implementation Patterns

### Reading from HBase in Spark

```python
def read_terms_from_hbase(term_batch, thrift_host, thrift_port):
    """Read terms from HBase within Spark partition"""
    import sys
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)

    results = []
    for term in term_batch:
        docs = connector.read_inverted_index_term(term)
        results.append((term, docs))

    connector.close()
    return results

# Use in Spark
terms_rdd = sc.parallelize(all_terms, numSlices=num_executors * 4)
docs_rdd = terms_rdd.mapPartitions(lambda terms:
    read_terms_from_hbase(list(terms), thrift_host, thrift_port)
)
```

### Writing to HBase in Spark

```python
def write_similarity_partition(partition, mode, thrift_host, thrift_port):
    """Write similarity results to HBase"""
    import sys
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)

    batch = []
    for record in partition:
        batch.append(record)
        if len(batch) >= 1000:
            connector.batch_write_similarity(batch, mode)
            batch = []

    if batch:
        connector.batch_write_similarity(batch, mode)

    connector.close()
    yield len(batch)
```

## Priority Order

1. **High Priority** (Core functionality):
   - spark_hbase_jpii.py
   - run_spark_hbase_pipeline.py
   - create_hbase_tables.sh

2. **Medium Priority** (Full feature set):
   - spark_hbase_pairwise.py
   - query_similarity.py
   - README.md

3. **Low Priority** (Nice to have):
   - benchmark_spark_hbase_modes.sh
   - WORKFLOWS.md
   - Utility scripts

## Next Steps

1. Create `spark_hbase_jpii.py` (based on HDFS version)
2. Create `spark_hbase_pairwise.py` (based on HDFS version)
3. Create `run_spark_hbase_pipeline.py` (based on HDFS pipeline)
4. Create HBase setup scripts
5. Test with small dataset (10 books)
6. Create query tool
7. Create documentation
8. Create benchmark automation

## Testing Checklist

- [ ] HBase tables created with correct schema
- [ ] Thrift server running on port 9090
- [ ] Stage 1: Inverted index writes to HBase
- [ ] Stage 1: Can query terms from HBase
- [ ] Stage 2 JPII: Reads from HBase, computes similarity
- [ ] Stage 2 JPII: Writes results to HBase
- [ ] Stage 2 Pairwise: Generates all pairs
- [ ] Query tool: Can retrieve results
- [ ] Metrics: CSV files generated
- [ ] Benchmark: Runs for all dataset sizes
