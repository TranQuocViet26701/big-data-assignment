# MapReduce Pipeline Execution and Metrics Collection

This directory contains MapReduce-based implementations of the inverted index and document similarity search pipeline, along with automated execution and metrics collection tools.

## Contents

### Core MapReduce Scripts
- `inverted_index_mapper.py` - Stage 1 mapper: extracts terms from documents
- `inverted_index_reducer.py` - Stage 1 reducer: builds inverted index
- `jpii_mapper.py` - Stage 2 mapper: generates candidate document pairs
- `jpii_reducer.py` - Stage 2 reducer: computes Jaccard similarity

### Execution and Automation
- `run_mapreduce_pipeline.py` - **Automated execution script with metrics collection**
- `run_inverted_index_streaming.sh` - Standalone script for Stage 1 only
- `run_jpii_streaming.sh` - Standalone script for Stage 2 only
- `example_run.sh` - Test wrapper with quick/medium/full modes
- `mapreduce_metrics.csv` - Output file containing performance metrics (generated)

## Quick Start

### Basic Usage (Query String)

```bash
# Run with 100 books using query string
python3 run_mapreduce_pipeline.py \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "wildlife conservation hunting animals"
```

### Using Query File

```bash
# First, create a query file
echo "wildlife conservation hunting animals nature environment" > /tmp/my_query.txt

# Run with query file
python3 run_mapreduce_pipeline.py \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query-file /tmp/my_query.txt
```

### With Custom Reducer Configuration

```bash
# Run with more reducers for larger datasets
python3 run_mapreduce_pipeline.py \
    --num-books 200 \
    --input-dir hdfs:///gutenberg-input-200 \
    --query "science technology innovation research" \
    --num-reducers 8
```

### Quick Testing

```bash
# Quick test with 10 books
./example_run.sh quick

# Medium test with 100 books
./example_run.sh medium

# Full test with multiple sizes
./example_run.sh full
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
| `--index-output` | `hdfs:///mapreduce-index-{num_books}` | HDFS output directory for inverted index |
| `--jpii-output` | `hdfs:///mapreduce-jpii-{num_books}` | HDFS output directory for similarity results |
| `--num-reducers` | Auto-scaled* | Number of reducers for MapReduce jobs |

**Auto-scaling logic:**
- 10 books or less: 2 reducers
- 11-100 books: 4 reducers
- 101+ books: 8 reducers

## What the Script Does

The `run_mapreduce_pipeline.py` script automates the complete MapReduce pipeline:

1. **Validates Input**: Checks that the HDFS input directory exists
2. **Prepares Query**: Reads query from file or uses query string
3. **Stage 1 - Inverted Index**:
   - Executes MapReduce job via Hadoop Streaming
   - Mapper: Extracts unique terms from each document
   - Reducer: Aggregates terms into inverted index
   - Measures execution time
   - Collects output metrics (index size, unique words)
4. **Stage 2 - Similarity Search**:
   - Executes JPII MapReduce job with inverted index as input
   - Mapper: Generates candidate document pairs (reads query from environment variable)
   - Reducer: Computes Jaccard similarity for each pair
   - Measures execution time
   - Collects output metrics (similarity pairs, output size)
5. **Saves Metrics**: Appends comprehensive metrics to `mapreduce_metrics.csv`
6. **Displays Summary**: Shows execution summary and results

## Metrics Collected

The script collects and saves the following metrics to `mapreduce_metrics.csv`:

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
- `num_reducers` - Number of reducers used
- `input_dir` - HDFS input directory
- `index_output` - HDFS index output directory
- `jpii_output` - HDFS similarity results directory

## Output CSV Format

The `mapreduce_metrics.csv` file has the following structure:

```csv
timestamp,num_books,stage1_time_sec,stage2_time_sec,total_time_sec,input_size_mb,index_size_mb,output_size_mb,unique_words,similarity_pairs,throughput_books_per_sec,num_reducers,input_dir,index_output,jpii_output
2025-11-17 14:30:45,100,145.67,82.34,228.01,45.23,12.45,2.34,15234,98,0.4386,4,hdfs:///gutenberg-input-100,hdfs:///mapreduce-index-100_20251117_143045,hdfs:///mapreduce-jpii-100_20251117_143045
```

**Note**: The CSV format matches the Spark version's format (excluding Spark-specific fields) to enable direct comparison.

## Standalone Scripts

### Run Stage 1 Only (Inverted Index)

```bash
./run_inverted_index_streaming.sh hdfs:///gutenberg-input-100 hdfs:///my-index 4
```

This is useful when:
- Testing Stage 1 independently
- Building an index for multiple different queries
- Debugging mapper/reducer logic

### Run Stage 2 Only (JPII Similarity)

```bash
# With query string
./run_jpii_streaming.sh hdfs:///my-index hdfs:///my-results "wildlife conservation" 4

# With query file
./run_jpii_streaming.sh hdfs:///my-index hdfs:///my-results /tmp/query.txt 4
```

This is useful when:
- Reusing an existing inverted index
- Testing different queries without rebuilding the index
- Debugging similarity calculation logic

## Example Workflow

### 1. Prepare Input Data

First, ensure you have books downloaded to HDFS:

```bash
# Download 100 books to HDFS
python3 ../donwload_file.py --num-books 100 --hdfs-dir /gutenberg-input-100
```

### 2. Run the MapReduce Pipeline

```bash
# Execute the complete pipeline
python3 run_mapreduce_pipeline.py \
    --num-books 100 \
    --input-dir hdfs:///gutenberg-input-100 \
    --query "adventure exploration discovery travel"
```

### 3. View Results

```bash
# View top 20 similar documents (sorted by similarity)
hdfs dfs -cat /mapreduce-jpii-100_*/part-* | sort -t$'\t' -k2 -rn | head -20

# View all results
hdfs dfs -cat /mapreduce-jpii-100_*/part-*

# View metrics
cat mapreduce_metrics.csv
```

### 4. Run Multiple Tests

```bash
# Test different dataset sizes
for size in 10 50 100 200; do
    python3 run_mapreduce_pipeline.py \
        --num-books $size \
        --input-dir hdfs:///gutenberg-input-$size \
        --query "science technology innovation"
done


python3 run_mapreduce_pipeline.py --mode jpii --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --query-file /home/ktdl9/big-data-assignment/my_query.txt \
    --num-executors 6

# View all metrics
cat mapreduce_metrics.csv
```

## How Query Passing Works

MapReduce uses environment variables to pass the query to mappers:

```bash
# In the streaming command:
-cmdenv q_from_user="wildlife conservation hunting"

# In jpii_mapper.py:
query = os.getenv('q_from_user')
```

This approach is necessary because Hadoop Streaming doesn't support local file access in cluster mode like Spark's client mode does.

## Viewing MapReduce Job Logs

To monitor job execution:

```bash
# View YARN application list
yarn application -list

# View application logs (replace <app_id> with actual ID)
yarn logs -applicationId <app_id>

# View job history via YARN ResourceManager UI
# Usually available at: http://<resourcemanager>:8088

# View job counters
mapred job -counter <job_id> <group> <counter>
```

## Troubleshooting

### HDFS Input Directory Not Found
```
[ERROR] Input directory does not exist in HDFS: hdfs:///gutenberg-input-100
```
**Solution**: Verify the path exists: `hdfs dfs -ls /gutenberg-input-100`

### Hadoop Streaming Jar Not Found
```
[ERROR] Hadoop streaming jar not found at: /usr/lib/hadoop-mapreduce/hadoop-streaming.jar
```
**Solution**: The script will auto-detect the jar location. If it fails, manually set the path in the script.

### MapReduce Job Failed
```
[ERROR] Command failed with return code 1
```
**Solution**: Check YARN logs for details: `yarn logs -applicationId <app_id>`

### No Similarity Pairs Found
```
[METRIC] Similarity pairs: 0
```
**Solution**:
- Query words may not match any documents (try broader terms)
- Check that Stage 1 completed successfully
- Verify the inverted index contains data: `hdfs dfs -cat /mapreduce-index-*/part-* | head`

### Python Script Errors in Mapper/Reducer
```
Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
```
**Solution**:
- Check mapper/reducer scripts are executable: `chmod +x *.py`
- Test locally: `cat sample.txt | ./inverted_index_mapper.py | sort | ./inverted_index_reducer.py`
- Check Python path in shebang: `#!/usr/bin/python3`

## Performance Tips

1. **Reducer Configuration**:
   - Small datasets (10 books): 2 reducers
   - Medium datasets (100 books): 4 reducers
   - Large datasets (200+ books): 8+ reducers

2. **Memory Settings**:
   - Adjust mapper memory: `-D mapreduce.map.memory.mb=2048`
   - Adjust reducer memory: `-D mapreduce.reduce.memory.mb=4096`

3. **Dataset Size**: MapReduce performance scales linearly; larger datasets take proportionally longer

4. **Query Length**: Longer queries (more words) increase Stage 2 processing time and output size

5. **HDFS Block Size**: For very large books, consider adjusting block size for better parallelism

## Comparison with Spark

This MapReduce implementation can be compared with the Spark version in `../1-spark-hdfs/`:

| Aspect | MapReduce | Spark |
|--------|-----------|-------|
| **Scripts** | 4 files (mapper + reducer per stage) | 2 files (one per stage) |
| **Execution** | `hadoop jar` with Hadoop Streaming | `spark-submit` |
| **Memory** | Disk-based shuffle | In-memory processing |
| **Speed** | Slower (more I/O) | Faster (RAM-based) |
| **Query Passing** | Environment variable (`-cmdenv`) | Local file (client mode) |
| **Configuration** | `num_reducers` | `num_executors`, `executor_memory` |
| **Metrics File** | `mapreduce_metrics.csv` | `spark_metrics.csv` |

### Performance Comparison

Expected performance (100 books):
- **MapReduce**: ~200-250 seconds total
- **Spark**: ~100-150 seconds total
- **Speedup**: Spark is typically 1.5-2x faster

Use both `mapreduce_metrics.csv` and `../1-spark-hdfs/spark_metrics.csv` to generate comparison reports.

## Testing Individual Components

### Test Mapper Locally

```bash
# Test inverted index mapper
echo "The quick brown fox jumps over the lazy dog" | ./inverted_index_mapper.py

# Test JPII mapper (with query)
export q_from_user="quick fox"
echo "quick\tbook1.txt@10\tbook2.txt@15" | ./jpii_mapper.py
```

### Test Reducer Locally

```bash
# Test inverted index reducer
echo -e "quick\tbook1.txt@10\nquick\tbook2.txt@15" | sort | ./inverted_index_reducer.py

# Test JPII reducer
echo -e "book1-query@10@5\t1\nbook1-query@10@5\t1" | sort | ./jpii_reducer.py
```

### Test Complete Pipeline Locally (Small Sample)

```bash
# Create test input
mkdir -p /tmp/test-input
echo "The quick brown fox jumps over the lazy dog" > /tmp/test-input/doc1.txt
echo "The lazy dog sleeps under the tree" > /tmp/test-input/doc2.txt

# Upload to HDFS
hdfs dfs -mkdir -p /test-input
hdfs dfs -put /tmp/test-input/* /test-input/

# Run pipeline
python3 run_mapreduce_pipeline.py \
    --num-books 2 \
    --input-dir hdfs:///test-input \
    --query "quick fox lazy" \
    --num-reducers 1
```

## Integration with HBase

The output from the JPII stage can be imported into HBase for efficient querying:

```bash
# After running the pipeline, import results to HBase
python3 ../import_to_hbase.py \
    --hdfs-input /mapreduce-jpii-100_*/part-* \
    --table-name similarity_results

# Query similar documents
python3 ../query_similarity.py \
    --query-doc "The_Adventures_of_Sherlock_Holmes.txt" \
    --table-name similarity_results
```

## Files Generated

After execution, you'll have:
- **HDFS Directories**:
  - `/mapreduce-index-{num_books}_{timestamp}/` - Inverted index output
  - `/mapreduce-jpii-{num_books}_{timestamp}/` - Similarity results
- **Local Files**:
  - `mapreduce_metrics.csv` - Performance metrics (append mode)

## Advanced Usage

### Custom Hadoop Configuration

Modify the streaming command in `run_mapreduce_pipeline.py` to add custom configurations:

```python
cmd = f"""hadoop jar {self.hadoop_streaming_jar} \\
    -D mapreduce.job.name="Custom_Job_Name" \\
    -D mapreduce.map.memory.mb=4096 \\
    -D mapreduce.reduce.memory.mb=8192 \\
    -D mapreduce.task.timeout=1800000 \\
    ...
```

### Reusing Inverted Index

Build the index once, then run multiple queries:

```bash
# Stage 1: Build index once
./run_inverted_index_streaming.sh hdfs:///gutenberg-input-100 hdfs:///shared-index 4

# Stage 2: Run multiple queries
./run_jpii_streaming.sh hdfs:///shared-index hdfs:///results-wildlife "wildlife conservation" 4
./run_jpii_streaming.sh hdfs:///shared-index hdfs:///results-science "science technology" 4
./run_jpii_streaming.sh hdfs:///shared-index hdfs:///results-history "history war empire" 4
```

## License

Part of the Big Data Assignment project.
