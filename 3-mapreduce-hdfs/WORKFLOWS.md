# MapReduce Pipeline Workflows: JPII vs Pairwise

This guide explains the two similarity computation modes available in the MapReduce pipeline (Hadoop Streaming) and when to use each approach.

---

## Overview

The MapReduce pipeline supports two distinct workflows for computing document similarity using Hadoop Streaming:

| Mode | Description | Input Required | Output |
|------|-------------|----------------|--------|
| **JPII** | Query-based similarity | Inverted index + Query file | Documents similar to query |
| **Pairwise** | All-pairs similarity | Inverted index only | All document pair similarities |

**Key Feature**: Both modes share the same reducer (`jpii_reducer.py`), which automatically detects the mode based on the presence of a query URL.

---

## JPII Mode (Query-based Similarity)

### What is JPII?

**JPII** (Jaccard Pairwise Index of Inverted Index) computes similarity between a specific query document and all other documents in the corpus using MapReduce.

### Use Cases

- **Document Search**: Find documents similar to a search query
- **Recommendation Systems**: Given a document, find similar documents
- **Query-Driven Analysis**: Focus on specific topics or themes
- **Document Retrieval**: Locate relevant documents for a particular subject

### How It Works

**Stage 1** - Build inverted index:
```
Mapper:  document → (term, doc@word_count)
Reducer: term → term\tdoc1@w1\tdoc2@w2...
```

**Stage 2** - JPII similarity:
```
Mapper (jpii_mapper.py):
  - Reads query file via environment variable
  - For each term in query, generates (query, document) pairs
  - Output: docA-docB@wA@wB\t1

Reducer (jpii_reducer.py):
  - Groups by pair, sums intersection counts
  - Computes Jaccard similarity
  - Output: docA-docB\tmatch\twA\twB\tsim
```

### Computational Complexity

- **Time**: O(Q × D) where Q = query terms, D = documents containing those terms
- **Space**: O(D) for storing similarity pairs
- **Scalability**: Excellent - only compares query against subset of documents

### Running JPII Mode

```bash
# With query file (recommended)
python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file my_query.txt

# With query string
python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "wildlife conservation hunting animals"

# With custom reducer count
python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --query-file my_query.txt \
    --num-reducers 12
```

### Output Format

```
docA-docB   match_count   wA   wB   similarity
book1.txt-book2.txt   15   234   456   0.0234
```

Where:
- `match_count`: Number of shared terms
- `wA`, `wB`: Number of unique words in documents A and B
- `similarity`: Jaccard coefficient (intersection/union)

### Metrics Collected

**File**: `mapreduce_jpii_metrics.csv`

**Fields**:
- Timing: `stage1_time_sec`, `stage2_time_sec`, `total_time_sec`
- Data: `input_size_mb`, `index_size_mb`, `output_size_mb`
- Content: `unique_words`, `similarity_pairs`
- Performance: `throughput_books_per_sec`
- Config: `mode`, `num_reducers`

---

## Pairwise Mode (All-Pairs Similarity)

### What is Pairwise?

**Pairwise** computes Jaccard similarity for **all possible document pairs** in the corpus using MapReduce, without requiring a specific query.

### Use Cases

- **Document Clustering**: Group similar documents together
- **Duplicate Detection**: Find near-duplicate or highly similar documents
- **Corpus Analysis**: Understand overall similarity structure
- **Network Analysis**: Build document similarity graphs
- **Dataset Exploration**: Discover relationships without predefined queries

### How It Works

**Stage 1** - Build inverted index (same as JPII)

**Stage 2** - Pairwise similarity:
```
Mapper (pairwise_mapper.py):
  - For each term, gets all documents containing it
  - Generates all possible pairs from those documents
  - Output: docA-docB@wA@wB\t1

Reducer (jpii_reducer.py):
  - Groups by pair, sums intersection counts
  - Computes Jaccard similarity (same reducer as JPII!)
  - Output: docA-docB\tintersection\twA\twB\tsim
```

### Computational Complexity

- **Time**: O(N²) where N = total number of documents
- **Space**: O(N²) for storing all similarity pairs
- **Scalability**: Challenging for large datasets (100+ documents may take hours)

### Running Pairwise Mode

```bash
# Basic usage (query file passed but ignored by mapper)
python3 run_mapreduce_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50 \
    --query-file my_query.txt

# With optimized reducer count for 3-node cluster
python3 run_mapreduce_pipeline.py \
    --mode pairwise \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file my_query.txt \
    --num-reducers 12


python3 run_mapreduce_pipeline.py \
    --mode pairwise \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-reducers 6

python3 run_mapreduce_pipeline.py \
    --mode pairwise \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-reducers 4
--- 


for size in 50 100 200; do
    python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books $size \
    --input-dir hdfs:///gutenberg-input-$size \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-reducers 6
done

python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-reducers 6
```

**Note**: The `--query-file` parameter is required for consistency but the query content is ignored by `pairwise_mapper.py`.

### Output Format

```
docA-docB   intersection   wA   wB   similarity
book1.txt-book2.txt   42   234   456   0.0647
```

### Metrics Collected

**File**: `mapreduce_pairwise_metrics.csv`

**Fields**:
- Timing: `stage1_time_sec`, `stage2_time_sec`, `total_time_sec`
- Data: `input_size_mb`, `index_size_mb`, `output_size_mb`
- Content: `unique_words`, `similarity_pairs`, `total_possible_pairs`, `pairs_computed`
- Performance: `throughput_books_per_sec`, `throughput_pairs_per_sec`
- Config: `mode`, `num_reducers`

---

## Mode Comparison

### When to Use JPII

✅ **Use JPII when**:
- You have a specific query or document to compare
- You need fast results focused on one topic
- Working with large datasets (200+ documents)
- Building a search or recommendation system
- Cluster resources are limited

❌ **Don't use JPII when**:
- You need complete similarity matrix
- You don't have a specific query
- You want to find all similar pairs in the corpus

### When to Use Pairwise

✅ **Use Pairwise when**:
- You need the complete similarity matrix
- Clustering or grouping all documents
- Finding duplicate/near-duplicate documents
- Analyzing corpus-wide similarity patterns
- Dataset is small-to-medium (< 100 documents)

❌ **Don't use Pairwise when**:
- Dataset is very large (> 200 documents)
- You only care about specific queries
- You need quick results for targeted searches
- Cluster resources are constrained

---

## Performance Guidelines (3-Node Cluster)

**Cluster Specs**: 3 VMs × 4 vCPU × 16GB RAM = 12 vCPU, 48GB RAM total

### JPII Performance

| Dataset Size | Reducers | Typical Stage 2 Time | Total Pipeline |
|--------------|----------|---------------------|----------------|
| 10 books | 4 | 30-60 seconds | 2-3 minutes |
| 50 books | 6 | 1-3 minutes | 5-8 minutes |
| 100 books | 8 | 3-6 minutes | 10-15 minutes |
| 200 books | 12 | 6-12 minutes | 20-30 minutes |

**Stage 1** (inverted index) typically takes 60-70% of total time.

**Optimization Tips**:
- Use more specific queries to reduce document matches
- Increase reducers for larger datasets (up to 12 for 3-node cluster)
- Query terms should exist in corpus (check inverted index first)

### Pairwise Performance

| Dataset Size | Total Pairs | Reducers | Typical Stage 2 Time | Total Pipeline |
|--------------|-------------|----------|---------------------|----------------|
| 10 books | 45 | 4 | 1-2 minutes | 3-5 minutes |
| 50 books | 1,225 | 6 | 10-20 minutes | 15-30 minutes |
| 100 books | 4,950 | 8 | 40-90 minutes | 50-120 minutes |
| 200 books | 19,900 | 12 | 3-6 hours | 4-8 hours |

**Stage 2** (pairwise) dominates total time for larger datasets.

**Optimization Tips**:
- Use maximum reducers (12) for datasets > 50 books
- Consider running overnight for 200+ books
- Monitor shuffle size: `-D mapreduce.reduce.shuffle.memory.limit.percent=0.25`
- Increase reducer memory for large datasets:
  ```bash
  -D mapreduce.reduce.memory.mb=4096
  ```

---

## Hadoop Streaming Details

### How Query Passing Works

**JPII Mode**:
```bash
-files jpii_mapper.py,jpii_reducer.py,query_temp.txt
-mapper jpii_mapper.py
-cmdenv q_from_user=query_temp.txt
```

The mapper reads the query file using `os.getenv('q_from_user')`.

**Pairwise Mode**:
```bash
-files pairwise_mapper.py,jpii_reducer.py,query_temp.txt
-mapper pairwise_mapper.py
-cmdenv q_from_user=query_temp.txt  # Ignored by pairwise_mapper
```

The pairwise mapper doesn't use the query file but it's included for consistency.

### Reducer Auto-Scaling

The pipeline automatically scales reducers based on dataset size and cluster capacity:

```python
if num_books <= 10:
    num_reducers = 4   # Light load
elif num_books <= 50:
    num_reducers = 6   # Medium load
elif num_books <= 100:
    num_reducers = 8   # Heavy load
else:
    num_reducers = 12  # Maximum for 3-node cluster
```

Override with `--num-reducers N` if needed.

---

## Workflow Decision Tree

```
Do you have a specific query or document to compare?
│
├─ YES → Use JPII mode
│   │
│   └─ Is your dataset > 200 documents?
│       │
│       ├─ YES → JPII is your best choice (still efficient)
│       └─ NO  → JPII will be very fast
│
└─ NO → Do you need all document pairs?
    │
    ├─ YES → Is your dataset < 100 documents?
    │   │
    │   ├─ YES → Use Pairwise mode (may take 1-2 hours for 100)
    │   └─ NO  → Pairwise may be too slow, consider:
    │             - Running overnight with 12 reducers
    │             - Sampling the dataset
    │             - Using JPII with representative queries
    │
    └─ NO → Use JPII with exploratory queries
```

---

## Advanced Usage

### Custom Hadoop Configuration

Edit `run_mapreduce_pipeline.py` to add custom Hadoop parameters:

```python
cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
    -D mapreduce.job.name="Custom_Job" \\
    -D mapreduce.map.memory.mb=4096 \\
    -D mapreduce.reduce.memory.mb=8192 \\
    -D mapreduce.task.timeout=1800000 \\
    -D mapreduce.reduce.shuffle.memory.limit.percent=0.25 \\
    ...
```

### Reusing Inverted Index

Build the index once, then run multiple JPII queries:

```bash
# Stage 1: Build index (manual)
hadoop jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.reduces=8 \
    -files inverted_index_mapper.py,inverted_index_reducer.py \
    -mapper inverted_index_mapper.py \
    -reducer inverted_index_reducer.py \
    -input /gutenberg-input-100 \
    -output /shared-index-100

# Stage 2: Run multiple JPII queries using the shared index
# (Requires manual Hadoop Streaming commands or script modification)
```

### Monitoring Jobs

```bash
# View running jobs
yarn application -list

# View job logs
yarn logs -applicationId application_XXXXX_XXXX

# View job details in ResourceManager UI
# Usually: http://<master-node>:8088

# Monitor HDFS usage
hdfs dfs -df -h
```

---

## Example Workflows

### Example 1: Find Documents Similar to "Moby Dick"

```bash
# Extract Moby Dick as query
hdfs dfs -cat /gutenberg-input-100/pg2701.txt > moby_dick_query.txt

# Run JPII
python3 run_mapreduce_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file moby_dick_query.txt

# View top 20 results sorted by similarity
hdfs dfs -cat /mapreduce-jpii-100_*/part-* | \
    sort -t$'\t' -k5 -rn | \
    head -20
```

### Example 2: Find All Similar Document Pairs

```bash
# Compute all pairs for 50 books
python3 run_mapreduce_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50 \
    --query-file my_query.txt \
    --num-reducers 8

# Extract high-similarity pairs (> 0.1)
hdfs dfs -cat /mapreduce-pairwise-50_*/part-* | \
    awk -F'\t' '$5 > 0.1' | \
    sort -t$'\t' -k5 -rn > similar_pairs.txt

# Count total pairs found
wc -l similar_pairs.txt
```

### Example 3: Benchmark Both Modes

```bash
cd /home/ktdl9/big-data-assignment/3-mapreduce-hdfs

# Run automated benchmark
./benchmark_mapreduce_modes.sh

# View metrics
column -t -s, mapreduce_jpii_metrics.csv | less -S
column -t -s, mapreduce_pairwise_metrics.csv | less -S
```

---

## Comparison with Spark

| Aspect | MapReduce (Hadoop Streaming) | Spark |
|--------|------------------------------|-------|
| **Scripts** | 3 mappers + 1 shared reducer | 2 scripts (one per mode) |
| **Execution** | `hadoop jar` + Python scripts | `spark-submit` |
| **Memory** | Disk-based shuffle | In-memory processing |
| **Speed** | Slower (more I/O) | ~2x faster |
| **Query Passing** | Environment variable + file | Local file (client mode) |
| **Configuration** | `num_reducers` | `num_executors`, `executor_memory` |
| **Metrics Files** | `mapreduce_{mode}_metrics.csv` | `spark_{mode}_metrics.csv` |
| **Cluster** | 3 nodes, 12 reducers max | 3 nodes, flexible executors |

### Expected Performance Comparison (100 books, JPII)

- **MapReduce**: ~10-15 minutes total
- **Spark**: ~5-8 minutes total
- **Speedup**: Spark is 1.5-2x faster

### Expected Performance Comparison (50 books, Pairwise)

- **MapReduce**: ~15-30 minutes total
- **Spark**: ~8-15 minutes total
- **Speedup**: Spark is ~2x faster

---

## Troubleshooting

### JPII Issues

**Problem**: Very few or no similarity pairs found
- **Solution**: Query terms may not exist in corpus
- **Check**: `hdfs dfs -cat /mapreduce-index-*/part-* | grep "your_query_term"`
- **Solution**: Try broader or more common query terms

**Problem**: Stage 2 takes too long
- **Solution**: Query may match too many documents (too common)
- **Solution**: Increase reducers: `--num-reducers 12`
- **Solution**: Check query file has reasonable content

### Pairwise Issues

**Problem**: Job runs very slow or hangs
- **Solution**: This is expected for large N (quadratic complexity)
- **Solution**: Increase reducers to maximum: `--num-reducers 12`
- **Solution**: Run overnight for datasets > 100 books

**Problem**: Reducer out of memory
- **Solution**: Increase reducer memory in script:
  ```python
  -D mapreduce.reduce.memory.mb=8192 \\
  -D mapreduce.reduce.java.opts=-Xmx6144m \\
  ```

**Problem**: Shuffle phase takes very long
- **Solution**: Adjust shuffle parameters:
  ```python
  -D mapreduce.reduce.shuffle.parallelcopies=20 \\
  -D mapreduce.reduce.shuffle.memory.limit.percent=0.20 \\
  ```

### General MapReduce Issues

**Problem**: Python script errors
```
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads()
```
- **Solution**: Check script has correct shebang: `#!/usr/bin/python3`
- **Solution**: Ensure scripts are executable: `chmod +x *.py`
- **Solution**: Test locally: `cat sample.txt | ./mapper.py | sort | ./reducer.py`

**Problem**: Hadoop streaming jar not found
- **Solution**: Script auto-detects jar location
- **Solution**: Manually verify: `hadoop classpath | tr ':' '\n' | grep streaming`

---

## Summary

- **JPII**: Fast, focused, query-driven similarity search (recommended for most use cases)
- **Pairwise**: Complete, exhaustive all-pairs similarity computation (use for small datasets or overnight runs)

Both modes share the same inverted index (Stage 1), so you can easily switch between them for the same dataset. The shared reducer (`jpii_reducer.py`) intelligently handles both modes.

For most search and recommendation use cases on the 3-node cluster, **JPII is recommended**. For clustering and corpus analysis of small datasets (< 100 books), **Pairwise is appropriate** with sufficient time allocation.
