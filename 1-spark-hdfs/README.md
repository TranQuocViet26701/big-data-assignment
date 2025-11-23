# Spark Pipeline Execution and Metrics Collection

This directory contains Spark-based implementations of the inverted index and document similarity search pipeline, along with automated execution and metrics collection tools.

## Contents

- `spark_inverted_index.py` - Spark implementation of inverted index builder
- `spark_jpii.py` - Spark implementation of JPII (Jaccard Pairwise Index of Inverted Index) similarity search
- `run_spark_pipeline.py` - **Automated execution script with metrics collection**
- `spark_metrics.csv` - Output file containing performance metrics (generated)

## Quick Start

### Basic Usage (Query String)

```bash
# Run with 100 books using query string
python3 run_spark_pipeline.py \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt

    python3 run_spark_hbase_pipeline.py --mode jpii --num-books 10 \
        --input-dir hdfs:///gutenberg-input-10 \
        --query-file /home/ktdl9/big-data-assignment/my_query.txt
```

### Using Query File

```bash
# First, create a query file
echo "wildlife conservation hunting animals nature environment" > /tmp/my_query.txt

# Run with query file
python3 run_spark_pipeline.py \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file /tmp/my_query.txt
```

### With Custom Spark Configuration

```bash
# Run with more executors and memory
python3 run_spark_pipeline.py \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --query "science technology innovation research" \
    --num-executors 8 \
    --executor-memory 8G \
    --driver-memory 4G
```

### Full Parameter Example

```bash
python3 run_spark_pipeline.py \
    --num-books 500 \
    --input-dir hdfs:///gutenberg-input-500 \
    --query "adventure exploration discovery journey" \
    --index-output hdfs:///my-custom-index \
    --jpii-output hdfs:///my-custom-results \
    --num-executors 10 \
    --executor-memory 6G \
    --driver-memory 3G
```

## Command-Line Arguments

### Required Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--num-books` | Number of books in the dataset | `100` |
| `--input-dir` | HDFS input directory containing text files | `hdfs:///gutenberg-input-100` |

### Query Arguments (One Required)

You must provide **either** `--query` **or** `--query-file` (mutually exclusive):

| Argument | Description | Example |
|----------|-------------|---------|
| `--query` | Query text as a string | `"wildlife conservation hunting"` |
| `--query-file` | Path to local file containing query text | `/tmp/my_query.txt` |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--index-output` | `hdfs:///spark-index-{num_books}` | HDFS output directory for inverted index |
| `--jpii-output` | `hdfs:///spark-jpii-{num_books}` | HDFS output directory for similarity results |
| `--num-executors` | `4` | Number of Spark executors |
| `--executor-memory` | `4G` | Memory per executor |
| `--driver-memory` | `2G` | Driver memory |

## What the Script Does

The `run_spark_pipeline.py` script automates the complete Spark pipeline:

1. **Validates Input**: Checks that the HDFS input directory exists
2. **Prepares Query**: Creates a temporary file from query string, or uses the provided query file
3. **Stage 1 - Inverted Index**:
   - Executes `spark_inverted_index.py` via `spark-submit`
   - Measures execution time
   - Collects output metrics (index size, unique words)
4. **Stage 2 - Similarity Search**:
   - Executes `spark_jpii.py` with the generated index
   - Measures execution time
   - Collects output metrics (similarity pairs, output size)
5. **Saves Metrics**: Appends comprehensive metrics to `spark_metrics.csv`
6. **Displays Summary**: Shows execution summary and results

## Metrics Collected

The script collects and saves the following metrics to `spark_metrics.csv`:

### Timing Metrics
- `timestamp` - When the pipeline was executed
- `stage1_time_sec` - Inverted index execution time (seconds)
- `stage2_time_sec` - JPII similarity search execution time (seconds)
- `total_time_sec` - Total pipeline execution time (seconds)
- `throughput_books_per_sec` - Books processed per second

### Data Size Metrics
- `input_size_mb` - Input data size in HDFS (MB)
- `index_size_mb` - Inverted index output size (MB)
- `output_size_mb` - Similarity results output size (MB)

### Content Metrics
- `unique_words` - Number of unique terms in the inverted index
- `similarity_pairs` - Number of document similarity pairs found

### Configuration Metrics
- `num_books` - Number of books in the dataset
- `num_executors` - Spark executors used
- `executor_memory` - Memory per executor
- `driver_memory` - Driver memory
- `query` - Query text used for similarity search
- `input_dir` - HDFS input directory
- `index_output` - HDFS index output directory
- `jpii_output` - HDFS similarity results directory

## Output CSV Format

The `spark_metrics.csv` file has the following structure:

```csv
timestamp,num_books,stage1_time_sec,stage2_time_sec,total_time_sec,input_size_mb,index_size_mb,output_size_mb,unique_words,similarity_pairs,throughput_books_per_sec,num_executors,executor_memory,driver_memory,query,input_dir,index_output,jpii_output
2025-11-17 14:30:45,100,125.34,67.82,193.16,45.23,12.45,2.34,15234,98,0.5178,4,4G,2G,"wildlife conservation hunting",hdfs:///gutenberg-input-100,hdfs:///spark-index-100_20251117_143045,hdfs:///spark-jpii-100_20251117_143045
```

Each row represents one complete pipeline execution, making it easy to:
- Compare performance across different dataset sizes
- Analyze impact of Spark configuration changes
- Track metrics over time
- Generate performance reports and visualizations

## Example Workflow

### 1. Prepare Input Data

First, ensure you have books downloaded to HDFS:

```bash
# Download 100 books to HDFS
python3 ../donwload_file.py --num-books 100 --hdfs-dir /gutenberg-input-100
```

### 2. Run the Spark Pipeline

```bash
# Execute the complete pipeline
python3 run_spark_pipeline.py \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "adventure exploration discovery travel"
```

### 3. View Results

```bash
# View similarity results
hdfs dfs -cat /spark-jpii-100_*/part-* | head -20

# View metrics
cat spark_metrics.csv
```

### 4. Run Multiple Tests

```bash
# Test different dataset sizes
for size in 10 50 100 200; do
    python3 run_spark_pipeline.py \
        --num-books $size \
        --input-dir hdfs:///gutenberg-input-$size \
        --query "science technology innovation"
done

python3 run_spark_pipeline.py --mode jpii --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-executors 6


for size in 10 50 100 200; do
    python3 run_spark_pipeline.py --mode pairwise --num-books $size \
        --input-dir hdfs:///gutenberg-input-$size \
        --query-file /home/ktdl9/big-data-assignment/my_query.txt \
        --num-executors 6
done

# View all metrics
cat spark_metrics.csv
```

## Viewing Spark Application Logs

To monitor Spark job execution:

```bash
# View YARN application list
yarn application -list

# View application logs (replace <app_id> with actual ID)
yarn logs -applicationId <app_id>

# Access Spark UI via YARN ResourceManager
# Usually available at: http://<resourcemanager>:8088
```

## Troubleshooting

### HDFS Input Directory Not Found
```
[ERROR] Input directory does not exist in HDFS: hdfs:///gutenberg-input-100
```
**Solution**: Verify the path exists: `hdfs dfs -ls /gutenberg-input-100`

### Spark Submit Failed
```
[ERROR] Command failed with return code 1
```
**Solution**: Check YARN logs for details: `yarn logs -applicationId <app_id>`

### Insufficient Memory
```
Container killed by YARN for exceeding memory limits
```
**Solution**: Increase executor memory: `--executor-memory 8G`

### No Similarity Pairs Found
```
[METRIC] Similarity pairs: 0
```
**Solution**: Query words may not match any documents. Try broader query terms.

## Performance Tips

1. **Executor Configuration**: Start with 4 executors and 4G memory, scale up for larger datasets
2. **Driver Memory**: Increase if you see driver OOM errors (use `--driver-memory 4G`)
3. **Deploy Mode**: Script uses `cluster` mode for Stage 1, `client` mode for Stage 2 (required for local file access)
4. **Dataset Size**: Performance scales logarithmically; 500 books ~10x slower than 100 books
5. **Query Length**: Longer queries (more words) increase Stage 2 processing time

## Comparison with MapReduce

This Spark implementation can be compared with the MapReduce version in the parent directory:

| Framework | Scripts | Execution | Performance |
|-----------|---------|-----------|-------------|
| **Spark** | `spark_inverted_index.py`<br>`spark_jpii.py` | Single `spark-submit` per stage | Faster (in-memory) |
| **MapReduce** | `inverted_index_mapper.py`<br>`inverted_index_reducer.py`<br>`jpii_mapper.py`<br>`jpii_reducer.py` | `hadoop jar` with streaming | Slower (disk-based) |

Use `run_spark_pipeline.py` to collect Spark metrics, then compare with MapReduce results from `../benchmark_results.csv`.

## Integration with HBase

The output from `spark_jpii.py` can be imported into HBase for querying:

```bash
# After running the pipeline, import results to HBase
python3 ../import_to_hbase.py \
    --hdfs-input /spark-jpii-100_*/part-* \
    --table-name similarity_results

# Query similar documents
python3 ../query_similarity.py \
    --query-doc "The_Adventures_of_Sherlock_Holmes.txt" \
    --table-name similarity_results
```

## Files Generated

After execution, you'll have:
- **HDFS Directories**:
  - `/spark-index-{num_books}_{timestamp}/` - Inverted index output
  - `/spark-jpii-{num_books}_{timestamp}/` - Similarity results
- **Local Files**:
  - `spark_metrics.csv` - Performance metrics (append mode)

## License

Part of the Big Data Assignment project.
