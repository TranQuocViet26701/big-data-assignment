# Spark Pipeline Workflows: JPII vs Pairwise

This guide explains the two similarity computation modes available in the Spark pipeline and when to use each approach.

---

## Overview

The Spark pipeline supports two distinct workflows for computing document similarity:

| Mode | Description | Input Required | Output |
|------|-------------|----------------|--------|
| **JPII** | Query-based similarity | Inverted index + Query | Documents similar to query |
| **Pairwise** | All-pairs similarity | Inverted index only | All document pair similarities |

---

## JPII Mode (Query-based Similarity)

### What is JPII?

**JPII** (Jaccard Pairwise Index of Inverted Index) computes similarity between a specific query document and all other documents in the corpus.

### Use Cases

- **Document Search**: Find documents similar to a search query
- **Recommendation Systems**: Given a document, find similar documents
- **Query-Driven Analysis**: Focus on specific topics or themes
- **Document Retrieval**: Locate relevant documents for a particular subject

### How It Works

1. **Stage 1**: Build inverted index from corpus
2. **Stage 2**:
   - Process query text (extract terms, remove stopwords)
   - For each query term, find documents containing it
   - Generate pairs: (query, document)
   - Compute Jaccard similarity for each pair

### Computational Complexity

- **Time**: O(Q × D) where Q = query terms, D = documents containing those terms
- **Space**: O(D) for storing similarity pairs
- **Scalability**: Excellent - only compares query against subset of documents

### Running JPII Mode

```bash
# With query string
python3 run_spark_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "wildlife conservation hunting animals"

# With query file
python3 run_spark_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file data/sample_query.txt
```

### Output Format

```
docA-docB   match_count   query_len   doc_len   similarity
book1.txt-book2.txt   15   234   456   0.0234
```

Where:
- `match_count`: Number of shared terms
- `query_len`: Number of unique words in query
- `doc_len`: Number of unique words in document
- `similarity`: Jaccard coefficient (intersection/union)

### Metrics Collected

**File**: `spark_jpii_metrics.csv`

**Fields**:
- Timing: `stage1_time_sec`, `stage2_time_sec`, `total_time_sec`
- Data: `input_size_mb`, `index_size_mb`, `output_size_mb`
- Content: `unique_words`, `similarity_pairs`
- Performance: `throughput_books_per_sec`
- Config: `num_executors`, `executor_memory`, `driver_memory`

---

## Pairwise Mode (All-Pairs Similarity)

### What is Pairwise?

**Pairwise** computes Jaccard similarity for **all possible document pairs** in the corpus, without requiring a specific query.

### Use Cases

- **Document Clustering**: Group similar documents together
- **Duplicate Detection**: Find near-duplicate or highly similar documents
- **Corpus Analysis**: Understand overall similarity structure
- **Network Analysis**: Build document similarity graphs
- **Dataset Exploration**: Discover relationships without predefined queries

### How It Works

1. **Stage 1**: Build inverted index from corpus
2. **Stage 2**:
   - For each term in the index, get all documents containing it
   - Generate all possible pairs from those documents
   - Aggregate pair counts across all terms
   - Compute Jaccard similarity for each unique pair

### Computational Complexity

- **Time**: O(N²) where N = total number of documents
- **Space**: O(N²) for storing all similarity pairs
- **Scalability**: Challenging for large datasets (100+ documents may be slow)

### Running Pairwise Mode

```bash
# No query needed
python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50

# With custom Spark config for larger datasets
python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --num-executors 8 \
    --executor-memory 8G
```

### Output Format

```
docA-docB   intersection   wA   wB   similarity
book1.txt-book2.txt   42   234   456   0.0647
```

Where:
- `intersection`: Number of shared terms
- `wA`: Number of unique words in document A
- `wB`: Number of unique words in document B
- `similarity`: Jaccard coefficient

### Metrics Collected

**File**: `spark_pairwise_metrics.csv`

**Fields**:
- Timing: `stage1_time_sec`, `stage2_time_sec`, `total_time_sec`
- Data: `input_size_mb`, `index_size_mb`, `output_size_mb`
- Content: `unique_words`, `similarity_pairs`, `total_possible_pairs`, `pairs_computed`
- Performance: `throughput_books_per_sec`, `throughput_pairs_per_sec`
- Config: `num_executors`, `executor_memory`, `driver_memory`

---

## Mode Comparison

### When to Use JPII

✅ **Use JPII when**:
- You have a specific query or document to compare
- You need fast results focused on one topic
- Working with large datasets (1000+ documents)
- Building a search or recommendation system
- Memory and compute resources are limited

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
- Dataset is small-to-medium (< 200 documents)

❌ **Don't use Pairwise when**:
- Dataset is very large (memory/time constraints)
- You only care about specific queries
- You need quick results for targeted searches

---

## Performance Guidelines

### JPII Performance

| Dataset Size | Typical Stage 2 Time | Memory Requirements |
|--------------|---------------------|---------------------|
| 10 books | 10-30 seconds | Low (2-4GB) |
| 50 books | 30-90 seconds | Low (2-4GB) |
| 100 books | 1-3 minutes | Medium (4-6GB) |
| 200+ books | 2-6 minutes | Medium (4-8GB) |

**Optimization Tips**:
- Use more specific queries to reduce document matches
- Increase executors for larger datasets
- Client deploy mode is faster for small datasets

### Pairwise Performance

| Dataset Size | Total Pairs | Typical Stage 2 Time | Memory Requirements |
|--------------|-------------|---------------------|---------------------|
| 10 books | 45 | 30-60 seconds | Low (4GB) |
| 50 books | 1,225 | 3-8 minutes | Medium (6-8GB) |
| 100 books | 4,950 | 10-30 minutes | High (8-12GB) |
| 200 books | 19,900 | 30-120 minutes | Very High (12-16GB+) |

**Optimization Tips**:
- Increase executors and memory for > 50 documents
- Use cluster deploy mode for better resource management
- Consider filtering low-similarity pairs to reduce output size
- Monitor shuffle size and adjust `spark.sql.shuffle.partitions`

---

## Workflow Decision Tree

```
Do you have a specific query or document to compare?
│
├─ YES → Use JPII mode
│   │
│   └─ Is your dataset > 1000 documents?
│       │
│       ├─ YES → JPII is your best choice
│       └─ NO  → JPII will be very fast
│
└─ NO → Do you need all document pairs?
    │
    ├─ YES → Is your dataset < 100 documents?
    │   │
    │   ├─ YES → Use Pairwise mode
    │   └─ NO  → Pairwise may be too slow, consider:
    │             - Sampling the dataset
    │             - Using JPII with representative queries
    │             - Running overnight with high resources
    │
    └─ NO → Use JPII with exploratory queries
```

---

## Advanced Usage

### Custom Output Paths

```bash
# Specify custom output directories
python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50 \
    --index-output hdfs:///my-custom-index \
    --stage2-output hdfs:///my-custom-pairwise-results
```

### High-Performance Configuration

```bash
# For large pairwise computations
python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --num-executors 16 \
    --executor-memory 12G \
    --driver-memory 4G
```

### Batch Processing

```bash
# Run multiple JPII queries in sequence
for query in "wildlife animals" "technology science" "history war"; do
    python3 run_spark_pipeline.py \
        --mode jpii \
        --num-books 100 \
        --input-dir hdfs:///gutenberg-input-100 \
        --query "$query"
done
```

---

## Metrics Analysis

### Viewing Metrics

```bash
# View JPII metrics
column -t -s, spark_jpii_metrics.csv | less -S

# View Pairwise metrics
column -t -s, spark_pairwise_metrics.csv | less -S
```

### Key Metrics to Monitor

**JPII**:
- `stage2_time_sec`: Should be < 10% of stage1 for efficient queries
- `similarity_pairs`: Number of documents matching query terms
- `throughput_books_per_sec`: Overall pipeline efficiency

**Pairwise**:
- `pairs_computed` vs `total_possible_pairs`: Coverage percentage
- `throughput_pairs_per_sec`: Stage 2 efficiency
- `output_size_mb`: Can grow very large for big datasets

---

## Troubleshooting

### JPII Issues

**Problem**: Very few or no similarity pairs found
- **Solution**: Check query terms, ensure they exist in corpus
- **Solution**: Try broader query terms

**Problem**: Stage 2 takes too long
- **Solution**: Query may be too broad (e.g., common words)
- **Solution**: Increase executors or memory

### Pairwise Issues

**Problem**: Out of memory errors
- **Solution**: Increase `executor-memory` and `driver-memory`
- **Solution**: Reduce dataset size
- **Solution**: Increase number of executors to distribute load

**Problem**: Stage 2 extremely slow
- **Solution**: This is expected for large N (quadratic complexity)
- **Solution**: Consider sampling or using JPII instead
- **Solution**: Run overnight with maximum resources

---

## Example Workflows

### Example 1: Find Documents Similar to "Moby Dick"

```bash
# Extract Moby Dick as query
hdfs dfs -cat /gutenberg-input-100/pg2701.txt > moby_dick.txt

# Run JPII
python3 run_spark_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file moby_dick.txt

# View results
hdfs dfs -cat /spark-jpii-100_*/part-* | sort -t$'\t' -k5 -rn | head -20
```

### Example 2: Cluster Documents by Similarity

```bash
# Compute all pairs
python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50

# Extract high-similarity pairs (> 0.1)
hdfs dfs -cat /spark-pairwise-50_*/part-* | \
    awk -F'\t' '$5 > 0.1' | \
    sort -t$'\t' -k5 -rn > similar_pairs.txt

# Analyze clusters (use external tools like networkx, etc.)
```

### Example 3: Benchmark Both Modes

```bash
# JPII benchmark
time python3 run_spark_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "science technology innovation"

# Pairwise benchmark (smaller dataset)
time python3 run_spark_pipeline.py \
    --mode pairwise \
    --num-books 50 \
    --input-dir hdfs:///gutenberg-input-50

# Compare metrics files
paste <(tail -n1 spark_jpii_metrics.csv) \
      <(tail -n1 spark_pairwise_metrics.csv)
```

---

## Summary

- **JPII**: Fast, focused, query-driven similarity search
- **Pairwise**: Complete, exhaustive all-pairs similarity computation

Choose based on your use case, dataset size, and available resources. Both modes share the same Stage 1 (inverted index), so you can easily switch between them for the same dataset.

For most search and recommendation use cases, **JPII is recommended**. For clustering and corpus analysis of small-to-medium datasets, **Pairwise is appropriate**.
