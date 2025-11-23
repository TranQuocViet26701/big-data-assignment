# Spark-HBase Workflows and Design Decisions

This document explains the workflows, design patterns, and decision-making guidance for the Spark-HBase document similarity pipeline.

## Table of Contents

1. [Pipeline Modes](#pipeline-modes)
2. [When to Use HBase vs HDFS](#when-to-use-hbase-vs-hdfs)
3. [HBase Schema Design](#hbase-schema-design)
4. [Workflow Patterns](#workflow-patterns)
5. [Performance Characteristics](#performance-characteristics)
6. [Decision Trees](#decision-trees)

## Pipeline Modes

### JPII (Jaccard Pairwise Index of Inverted Index)

**Mode**: Query-based similarity

**Use Case**: Find documents similar to a specific query document

**Complexity**: O(Q × D) where Q = query terms, D = documents per term

**When to Use**:
- You have a specific query document
- Need fast results for one query at a time
- Dataset is large (>1000 documents)
- Real-time or near-real-time queries needed

**Example**:
```bash
python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file wildlife_conservation.txt
```

**Results**: Similarity scores for `wildlife_conservation.txt` vs all other documents

### Pairwise (All-pairs Similarity)

**Mode**: Complete similarity matrix

**Use Case**: Compute similarity for ALL document pairs

**Complexity**: O(N²) where N = number of documents

**When to Use**:
- Need complete similarity matrix
- Dataset is small (<500 documents)
- Building a recommendation system
- Creating document clusters
- One-time batch computation

**Example**:
```bash
python3 run_spark_hbase_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50
```

**Results**: Similarity scores for all possible document pairs

## When to Use HBase vs HDFS

### Use Spark-HBase When:

✅ **Real-time Queries Required**
- Need to query similarity results in milliseconds
- Interactive applications
- Web APIs serving similarity data

✅ **Incremental Updates**
- Adding new documents without full re-computation
- Updating similarity scores for specific documents
- Dynamic dataset that changes frequently

✅ **Random Access Patterns**
- Querying specific document pairs
- Filtering by similarity threshold
- Sorting and ranking results

✅ **Storage Efficiency**
- HBase compression for large similarity matrices
- Column-family based storage
- Efficient sparse data storage

### Use Spark-HDFS When:

✅ **Batch Processing**
- One-time computation, no queries
- Results exported to files
- Archival storage

✅ **Sequential Access**
- Processing all results sequentially
- MapReduce-style aggregations
- Full table scans

✅ **Simplicity**
- No HBase infrastructure available
- Simpler deployment model
- File-based workflows

## HBase Schema Design

### Inverted Index Table

**Table Name**: `inverted_index`

**Schema**:
```
Row Key: term (e.g., "wildlife")
Column Family: docs
Columns: docs:<document_name> = <word_count>
```

**Example**:
```
Row Key: "conservation"
  docs:book1.txt = 45
  docs:book2.txt = 23
  docs:book5.txt = 67
```

**Design Rationale**:
- Row key = term enables fast lookup by term
- Column family `docs` groups all documents for a term
- Dynamic columns allow variable number of documents per term
- Word count stored as column value for Jaccard computation

### Similarity Scores Table

**Table Name**: `similarity_scores`

**Schema**:
```
Row Key: <mode>:<doc1>-<doc2> (e.g., "jpii:query-book1")
Column Family: score (similarity metrics)
Column Family: meta (metadata)
```

**Columns**:
- `score:similarity` - Jaccard similarity coefficient
- `score:match_count` - Number of matching terms
- `score:w1` - Word count for first document
- `score:w2` - Word count for second document
- `meta:timestamp` - Computation timestamp
- `meta:mode` - Mode used (jpii/pairwise)

**Example**:
```
Row Key: "jpii:wildlife_conservation-tropical_rainforests"
  score:similarity = 0.42356
  score:match_count = 234
  score:w1 = 1250
  score:w2 = 987
  meta:timestamp = 1699123456
  meta:mode = jpii
```

**Design Rationale**:
- Row key includes mode for multi-mode support
- Row key format enables prefix scans (e.g., all results for a document)
- Separate column families for scores vs metadata
- Denormalized design for query performance

## Workflow Patterns

### Pattern 1: First-Time Pipeline Execution

**Scenario**: Running the pipeline for the first time

**Steps**:

```bash
# 1. Create HBase tables
bash create_hbase_tables.sh

# 2. Start Thrift server
hbase thrift start -p 9090 &

# 3. Verify setup
bash check_hbase_status.sh

# 4. Run pipeline (JPII)
python3 run_spark_hbase_pipeline.py \
    --mode pairwise \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt
    --num-executors 6

python3 run_spark_hbase_pipeline.py \
    --mode pairwise \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-executors 6 \
    --thrift-host hadoop-master \
    --thrift-port 9090


for size in 10 50 100 200; do
    python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books $size \
    --input-dir hdfs:///gutenberg-input-$size \
    --thrift-host hadoop-master \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-executors 6 \
    --thrift-port 9090
done

# 5. Query results
python3 query_similarity.py --mode jpii --document my_query --top 20
```

**Result**: Inverted index and similarity scores stored in HBase

### Pattern 2: Re-running with Different Query

**Scenario**: Already have inverted index, want to run JPII with new query

**Problem**: Stage 1 already completed, don't want to rebuild index

**Solution**: Run only Stage 2 directly

```bash
# Run only Stage 2 (JPII)
spark-submit \
    --master yarn \
    --deploy-mode client \
    spark_hbase_jpii.py \
    new_query.txt \
    localhost 9090

# Query results
python3 query_similarity.py --mode jpii --document new_query --top 20
```

**Benefit**: Skip Stage 1, reuse existing inverted index

### Pattern 3: Incremental Updates

**Scenario**: Adding new documents to existing index

**Steps**:

```bash
# 1. Add new documents to HDFS
hdfs dfs -put new_books/*.txt hdfs:///gutenberg-input-100/

# 2. Run Stage 1 only for new documents
# (This requires filtering in the script or manual inverted index update)

# 3. Re-run Stage 2 for affected queries
spark-submit spark_hbase_jpii.py my_query.txt localhost 9090
```

**Note**: Current implementation rebuilds full index. For true incremental updates, modify Stage 1 to check existing HBase data.

### Pattern 4: Batch Query Processing

**Scenario**: Process multiple queries without re-running Spark

**Steps**:

```bash
# Run pairwise once to get all pairs
python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100

# Query for different documents without Spark
python3 query_similarity.py --mode pairwise --document book1 --top 10 > book1_results.txt
python3 query_similarity.py --mode pairwise --document book2 --top 10 > book2_results.txt
python3 query_similarity.py --mode pairwise --document book3 --top 10 > book3_results.txt
```

**Benefit**: One Spark job, unlimited queries

### Pattern 5: Threshold-Based Filtering

**Scenario**: Find all highly similar document pairs

**Steps**:

```bash
# Run pairwise
python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200

# Query with high threshold (>80% similarity)
python3 query_similarity.py --mode pairwise --threshold 0.8 --output high_similarity.csv
```

**Use Cases**:
- Duplicate detection
- Plagiarism detection
- Document clustering

## Performance Characteristics

### JPII Mode

**Time Complexity**: O(Q × D × T)
- Q = query unique terms
- D = average documents per term
- T = average terms per document

**Space Complexity**: O(Q × D) similarity pairs

**HBase Reads**: Q term lookups (one per query term)

**HBase Writes**: Q × D similarity scores

**Optimal For**:
- Large datasets (>1000 documents)
- Sparse queries (few terms)
- Single query at a time

**Example Performance** (100 books, 5-term query):
- Stage 1: 60-120 seconds
- Stage 2: 20-40 seconds
- Query: <1 second
- Total: 80-160 seconds

### Pairwise Mode

**Time Complexity**: O(N² × T)
- N = number of documents
- T = average terms per document

**Space Complexity**: O(N²) similarity pairs

**HBase Reads**: All terms in index

**HBase Writes**: N × (N-1) / 2 similarity scores

**Optimal For**:
- Small-medium datasets (<500 documents)
- Complete similarity matrix needed
- One-time batch computation

**Example Performance** (100 books):
- Stage 1: 60-120 seconds
- Stage 2: 180-300 seconds
- Query: <1 second
- Total: 240-420 seconds

### HBase-Specific Optimizations

1. **Batch Writes**: Write 1000 records per batch to minimize RPC calls

```python
connector.batch_write_inverted_index(batch_data, batch_size=1000)
```

2. **Connection Pooling**: One HBase connection per Spark partition

```python
terms_rdd.mapPartitions(lambda partition: process_with_hbase(partition))
```

3. **Prefix Scans**: Efficient row key design for range queries

```python
# Query all JPII results for a document
connector.query_similarity(mode='jpii', document='my_query')
# HBase performs prefix scan: "jpii:my_query-*"
```

4. **Column Family Separation**: Separate `score` and `meta` column families

```
score:similarity, score:match_count  -> Frequently accessed
meta:timestamp, meta:mode            -> Metadata only
```

## Decision Trees

### Choosing Pipeline Mode

```
Do you need similarity for a specific query document?
├─ Yes → Use JPII mode
│   └─ Multiple queries?
│       ├─ Yes → Run pairwise once, query as needed
│       └─ No → Run JPII for single query
│
└─ No → Need complete similarity matrix?
    ├─ Yes → Use Pairwise mode
    └─ No → Define query document, use JPII
```

### Choosing Storage Backend

```
Do you need real-time queries (<1 second)?
├─ Yes → Use Spark-HBase
│   └─ Will data be updated incrementally?
│       ├─ Yes → Definitely use HBase
│       └─ No → HBase still good for query speed
│
└─ No → Is deployment complexity a concern?
    ├─ Yes → Use Spark-HDFS (simpler)
    └─ No → HBase provides more features
```

### Tuning Spark Resources

```
Dataset Size?
├─ <50 books
│   └─ num-executors: 2, executor-memory: 2G
│
├─ 50-100 books
│   └─ num-executors: 4, executor-memory: 4G
│
├─ 100-200 books
│   └─ num-executors: 6, executor-memory: 6G
│
└─ >200 books
    └─ num-executors: 8, executor-memory: 8G
```

## Common Use Cases

### Use Case 1: Document Recommendation System

**Scenario**: Web application that recommends similar articles

**Solution**:
1. Run pairwise mode once daily (batch job)
2. Query HBase in real-time for recommendations
3. Incrementally update index when new articles added

**Implementation**:
```bash
# Daily batch job
python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 1000 \
    --input-dir hdfs:///articles

# Real-time API queries
python3 query_similarity.py --mode pairwise --document article_123 --top 5
```

### Use Case 2: Research Paper Search

**Scenario**: User submits query document, find similar papers

**Solution**:
1. Maintain inverted index of all papers (updated weekly)
2. Run JPII for each user query
3. Return results in <1 second

**Implementation**:
```bash
# Weekly index rebuild
spark-submit spark_hbase_inverted_index.py hdfs:///papers localhost 9090

# Per-query (fast, client-mode)
spark-submit --deploy-mode client spark_hbase_jpii.py user_query.txt localhost 9090
```

### Use Case 3: Duplicate Detection

**Scenario**: Find near-duplicate documents in large corpus

**Solution**:
1. Run pairwise mode
2. Query with high threshold (>0.9)
3. Export results to CSV

**Implementation**:
```bash
# Compute all pairs
python3 run_spark_hbase_pipeline.py --mode pairwise --num-books 500 \
    --input-dir hdfs:///corpus

# Find duplicates (>90% similarity)
python3 query_similarity.py --mode pairwise --threshold 0.9 --output duplicates.csv
```

## Best Practices

### 1. HBase Table Management

- **Truncate, don't recreate**: Use `clear_hbase_tables.sh` for re-runs
- **Monitor table sizes**: Use `check_hbase_status.sh` regularly
- **Backup important data**: HBase snapshots before major operations

### 2. Spark Configuration

- **Start small**: Test with 10 books before scaling up
- **Monitor YARN**: Check ResourceManager UI for resource usage
- **Use client mode for JPII**: Faster for interactive queries

### 3. Query Optimization

- **Use thresholds**: Filter low-similarity pairs early
- **Limit results**: Use `--top N` to avoid large result sets
- **Export for analysis**: Save to CSV/JSON for further processing

### 4. Troubleshooting

- **Check Thrift server first**: Most issues are connectivity
- **Verify HBase tables**: Use `check_hbase_status.sh`
- **Monitor YARN logs**: `yarn logs -applicationId <app_id>`
- **Test with small data**: Use 10-book dataset for debugging

## Metrics Interpretation

### Stage 1 Metrics

- `stage1_time_sec`: Index build time (should scale linearly with data)
- `unique_terms`: Number of distinct terms (after stopword removal)
- `input_size_mb`: Raw input data size

**What to Watch**:
- If `unique_terms` is too low (<100): Check stopword filtering
- If `stage1_time_sec` is very high: Check HDFS read performance

### Stage 2 Metrics (JPII)

- `stage2_time_sec`: Similarity computation time
- `similarity_pairs`: Number of similar documents found
- `throughput_books_per_sec`: Overall processing speed

**What to Watch**:
- If `similarity_pairs` is 0: Query terms may not match any documents
- If `stage2_time_sec` >> `stage1_time_sec`: Query may be too broad

### Stage 2 Metrics (Pairwise)

- `total_possible_pairs`: N×(N-1)/2 theoretical maximum
- `similarity_pairs`: Actual pairs computed (should equal total_possible_pairs)
- `throughput_pairs_per_sec`: Pair processing rate

**What to Watch**:
- If `similarity_pairs` < `total_possible_pairs`: Some pairs may be filtered
- If `throughput_pairs_per_sec` is low: Increase Spark resources

## Summary

The Spark-HBase pipeline provides:
- **Flexibility**: JPII or Pairwise modes for different use cases
- **Performance**: Real-time queries via HBase, distributed computation via Spark
- **Scalability**: Handles large datasets with proper Spark configuration
- **Usability**: Simple Python scripts with comprehensive metrics

Choose your mode based on requirements, monitor metrics for optimization, and leverage HBase's real-time query capabilities for interactive applications.
