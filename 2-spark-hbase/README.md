# Spark-HBase Document Similarity Pipeline

This directory implements a distributed document similarity pipeline using **Apache Spark** for computation and **Apache HBase** for storage, connected via the **Thrift server** and **HappyBase** Python client.

## Overview

The pipeline consists of two stages:

1. **Stage 1: Inverted Index** - Builds an inverted index from documents stored in HDFS and writes to HBase
2. **Stage 2: Similarity Computation** - Computes document similarity using either JPII (query-based) or Pairwise (all-pairs) mode and stores results in HBase

### Key Features

- **Distributed Processing**: Uses Apache Spark on YARN for parallel computation
- **HBase Storage**: Stores inverted index and similarity scores in HBase for real-time querying
- **Two Modes**: JPII (query-based) and Pairwise (all-pairs) similarity computation
- **Real-time Queries**: Query similarity results without re-running Spark jobs
- **Metrics Collection**: Captures performance metrics in mode-specific CSV files

## Architecture

```
HDFS Documents → Spark → HBase Inverted Index
                   ↓
              HBase Inverted Index → Spark → HBase Similarity Scores
                                        ↓
                              Real-time Queries (HappyBase)
```

### HBase Tables

#### `inverted_index`
- **Row Key**: term (word)
- **Column Family**: `docs`
- **Columns**: `docs:<document_name>` = word_count
- **Purpose**: Maps each term to documents containing it

#### `similarity_scores`
- **Row Key**: `<mode>:<doc1>-<doc2>`
- **Column Families**: `score`, `meta`
- **Columns**:
  - `score:similarity` - Jaccard similarity score
  - `score:match_count` - Number of matching terms
  - `score:w1`, `score:w2` - Word counts for each document
  - `meta:timestamp`, `meta:mode` - Metadata
- **Purpose**: Stores document similarity results

## Requirements

### System Requirements

- **Apache Hadoop** with YARN
- **Apache HBase** (with Thrift server)
- **Apache Spark** 3.0+
- **Python** 3.6+

### Python Dependencies

Install Python dependencies:

```bash
pip3 install -r requirements.txt
```

This installs:
- `happybase>=1.2.0` - HBase client library
- `thrift>=0.13.0` - Thrift protocol for HBase connectivity
- `pyspark>=3.0.0` - Spark Python API

## Setup

### 1. Create HBase Tables

Create the required HBase tables:

```bash
bash create_hbase_tables.sh
```

This creates:
- `inverted_index` table with `docs` column family
- `similarity_scores` table with `score` and `meta` column families

### 2. Start HBase Thrift Server

Start the HBase Thrift server on port 9090:

```bash
hbase thrift start -p 9090 &
```

Verify it's running:

```bash
netstat -an | grep 9090
```

### 3. Verify Setup

Check HBase status and connectivity:

```bash
bash check_hbase_status.sh
```

This checks:
- Thrift server status
- HBase tables existence
- Python HappyBase connectivity
- Sample data in tables

## Usage

### Running the Pipeline

#### JPII Mode (Query-based Similarity)

Query-based similarity finds documents similar to a specific query document:

```bash
python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file my_query.txt \
    --thrift-host localhost \
    --thrift-port 9090
```

**Parameters**:
- `--mode jpii` - Query-based similarity mode
- `--num-books` - Number of books in dataset
- `--input-dir` - HDFS directory containing documents
- `--query-file` - Local file containing query text
- `--thrift-host` - HBase Thrift server hostname
- `--thrift-port` - HBase Thrift server port (default: 9090)

#### Pairwise Mode (All-pairs Similarity)

Pairwise mode computes similarity for all document pairs:

```bash
python3 run_spark_hbase_pipeline.py \
    --mode pairwise \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --thrift-host localhost \
    --thrift-port 9090
```

**Note**: No query file needed for pairwise mode.

### Querying Results

Query similarity results without running Spark:

#### Query JPII Results

```bash
# Top 20 similar documents for a query
python3 query_similarity.py --mode jpii --document my_query --top 20

# Export to CSV
python3 query_similarity.py --mode jpii --document my_query --output results.csv
```

#### Query Pairwise Results

```bash
# All pairs above 0.1 similarity threshold
python3 query_similarity.py --mode pairwise --threshold 0.1 --top 100

# Export to JSON
python3 query_similarity.py --mode pairwise --threshold 0.2 --output results.json
```

### Spark Configuration

Customize Spark resources:

```bash
python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --query-file query.txt \
    --num-executors 8 \
    --executor-memory 8G \
    --driver-memory 4G
```

### Automated Benchmarking

Run automated benchmarks across multiple dataset sizes:

```bash
# Run both modes on all default datasets (10, 50, 100, 200 books)
./benchmark_spark_hbase_modes.sh --executors 6 --executor-memory 6G --datasets "10 50 100 200"

# Run only JPII mode
./benchmark_spark_hbase_modes.sh --mode jpii

# Run pairwise on specific datasets
./benchmark_spark_hbase_modes.sh --mode pairwise --datasets "10 50 100"

./benchmark_spark_hbase_modes.sh --mode jpii --executors 6 --executor-memory 6G --datasets "10"

# Custom configuration
./benchmark_spark_hbase_modes.sh \
    --mode both \
    --datasets "10 50 100 200" \
    --executors 6 \
    --executor-memory 8G
```

The benchmark script:
- Validates HBase connectivity and HDFS directories
- Runs pipeline across multiple dataset sizes
- Collects metrics for each run
- Generates summary report with throughput statistics
- Shows HBase table statistics after completion

## Scripts Reference

### Pipeline Scripts

| Script | Purpose |
|--------|---------|
| `spark_hbase_inverted_index.py` | Stage 1: Build inverted index in HBase |
| `spark_hbase_jpii.py` | Stage 2: JPII similarity computation |
| `spark_hbase_pairwise.py` | Stage 2: Pairwise similarity computation |
| `run_spark_hbase_pipeline.py` | Orchestration script for full pipeline |
| `query_similarity.py` | Query similarity results from HBase |

### Utility Scripts

| Script | Purpose |
|--------|---------|
| `hbase_connector.py` | HappyBase wrapper for HBase operations |
| `create_hbase_tables.sh` | Create HBase tables with schema |
| `clear_hbase_tables.sh` | Truncate HBase tables |
| `check_hbase_status.sh` | Check HBase and Thrift server status |
| `benchmark_spark_hbase_modes.sh` | Automated benchmarking across datasets |

## Metrics

Metrics are saved to mode-specific CSV files:

- **JPII Mode**: `spark_hbase_jpii_metrics.csv`
- **Pairwise Mode**: `spark_hbase_pairwise_metrics.csv`

### JPII Metrics

| Metric | Description |
|--------|-------------|
| `timestamp` | Execution timestamp |
| `num_books` | Dataset size |
| `stage1_time_sec` | Inverted index build time |
| `stage2_time_sec` | Similarity computation time |
| `total_time_sec` | Total pipeline time |
| `input_size_mb` | Input data size |
| `unique_terms` | Number of unique terms in HBase |
| `similarity_pairs` | Number of similar document pairs |
| `throughput_books_per_sec` | Processing throughput |
| `query_file` | Query file used |

### Pairwise Metrics

Includes all JPII metrics plus:

| Metric | Description |
|--------|-------------|
| `total_possible_pairs` | N×(N-1)/2 total pairs |
| `throughput_pairs_per_sec` | Pairs processed per second |

## Maintenance

### Clear HBase Tables

To re-run the pipeline with fresh data:

```bash
bash clear_hbase_tables.sh
```

This truncates tables without dropping the schema.

### Recreate Tables

To completely recreate tables:

```bash
bash create_hbase_tables.sh
```

This drops existing tables and creates new ones.

## Troubleshooting

### HBase Connection Failed

**Error**: `Failed to connect to HBase`

**Solutions**:
1. Check Thrift server is running: `netstat -an | grep 9090`
2. Start Thrift server: `hbase thrift start -p 9090`
3. Verify HBase is running: `hbase shell` and type `status`

### Import Error: happybase

**Error**: `ModuleNotFoundError: No module named 'happybase'`

**Solution**:
```bash
pip3 install happybase thrift
```

### Spark Job Failed

**Error**: Spark job fails during execution

**Solutions**:
1. Check YARN logs: `yarn logs -applicationId <app_id>`
2. Verify HDFS input directory exists: `hdfs dfs -ls <input_dir>`
3. Check HBase tables exist: `bash check_hbase_status.sh`
4. Ensure Thrift server is accessible from all cluster nodes

### Table Not Found

**Error**: `Table 'inverted_index' not found`

**Solution**:
```bash
bash create_hbase_tables.sh
```

## Performance Considerations

### HBase Batch Size

The default batch size for HBase writes is 1000 records. Adjust in scripts if needed:

```python
connector.batch_write_inverted_index(batch_data, batch_size=1000)
```

### Spark Parallelism

For optimal performance, set `numSlices` based on cluster size:

```python
num_slices = max(len(terms) // 10, sc.defaultParallelism * 4)
```

### HBase Connection Pooling

Each Spark partition creates one HBase connection using `mapPartitions` pattern to minimize connection overhead.

## Comparison with HDFS Version

| Feature | Spark-HBase | Spark-HDFS |
|---------|-------------|------------|
| **Storage** | HBase | HDFS |
| **Indexing** | Random access by row key | Sequential scan |
| **Query Speed** | Real-time (milliseconds) | Requires Spark job (minutes) |
| **Updates** | Easy incremental updates | Full re-computation |
| **Storage Format** | Columnar, compressed | Text files |
| **Use Case** | Real-time queries, updates | Batch processing, archival |

## See Also

- `WORKFLOWS.md` - Detailed workflows and decision guide
- `IMPLEMENTATION_STATUS.md` - Implementation roadmap and status
- `requirements.txt` - Python dependencies

## Example Workflow

Complete workflow from setup to query:

```bash
# 1. Create HBase tables
bash create_hbase_tables.sh

# 2. Start Thrift server
hbase thrift start -p 9090 &

# 3. Run JPII pipeline
python3 run_spark_hbase_pipeline.py \
    --mode jpii \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file my_query.txt

# 4. Query results
python3 query_similarity.py --mode jpii --document my_query --top 10

# 5. Export to CSV
python3 query_similarity.py --mode jpii --document my_query --output top_results.csv
```

## License

Part of Big Data Assignment - Document Similarity Pipeline
