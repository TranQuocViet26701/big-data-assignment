# Performance Benchmark Guide

## 🎯 Quick Start

### Run the Benchmark

```bash
./benchmark_pipeline.sh
```

This will automatically:
- Test dataset sizes: **10, 50, 100, 200 books**
- Test reducer counts: **2, 4, 8, 16** for each dataset
- Run **16 total test combinations**
- Generate CSV results and detailed logs

**Estimated Time**: 1-3 hours depending on your cluster size

---

## 📊 What Gets Measured

### Performance Metrics
- ⏱️ **Stage 1 Time** - Time to build inverted index
- ⏱️ **Stage 2 Time** - Time to compute similarities
- ⏱️ **Total Time** - End-to-end pipeline time
- 🚀 **Throughput** - Books processed per second

### Data Metrics
- 💾 **Input Size** - Raw text data size
- 💾 **Index Size** - Inverted index size
- 💾 **JPII Output Size** - Similarity results size
- 📝 **Unique Words** - Words in the index
- 🔍 **Similar Documents** - Number of similar docs found

---

## 📁 Output Files

### `benchmark_results.csv`
Machine-readable CSV with all metrics:

```csv
Status,TestID,Books,Reducers,Stage1Time(s),Stage2Time(s),TotalTime(s),...
SUCCESS,10books_2reducers,10,2,45,12,57,...
SUCCESS,10books_4reducers,10,4,38,10,48,...
```

**Columns:**
- Status: SUCCESS or FAILED
- TestID: Descriptive test identifier
- Books: Number of books processed
- Reducers: Number of reducers used
- Stage1Time(s): Inverted index build time
- Stage2Time(s): Similarity computation time
- TotalTime(s): Total pipeline time
- InputSize(bytes): Input data size
- IndexSize(bytes): Inverted index size
- JPIISize(bytes): Similarity output size
- UniqueWords: Unique words indexed
- SimilarDocs: Similar documents found
- Throughput(books/s): Processing speed

### `benchmark_logs/<timestamp>/`
Detailed logs for each test:

```
benchmark_logs/20231116_143022/
├── 10books_2reducers.log
├── 10books_4reducers.log
├── 10books_8reducers.log
├── 10books_16reducers.log
├── 50books_2reducers.log
...
```

Each log contains:
- Full MapReduce job output
- HDFS operations
- Error messages (if any)
- Timing information

---

## 📈 Analyzing Results

### Option 1: Console Summary Table

The script displays a summary table at the end:

```
Books           Reducers   Stage1(s)    Stage2(s)    Total(s)     Throughput
--------------------------------------------------------------------------------
10              2          45           12           57           0.17 books/s
10              4          38           10           48           0.20 books/s
10              8          35           11           46           0.21 books/s
...
```

### Option 2: Excel/LibreOffice

Open `benchmark_results.csv` in Excel or LibreOffice Calc:

1. Import the CSV file
2. Create pivot tables to compare configurations
3. Generate charts:
   - Time vs. Number of Reducers (for each dataset size)
   - Throughput vs. Dataset Size
   - Stage 1 vs. Stage 2 time comparison

### Option 3: Python Analysis

Use the included analysis script:

```bash
python3 analyze_benchmark.py benchmark_results.csv
```

This will:
- Generate comparison charts
- Identify optimal reducer count for each dataset size
- Show scaling efficiency
- Export visualization images

### Option 4: Command-Line Tools

```bash
# Find fastest configuration for 100 books
cat benchmark_results.csv | grep ",100," | sort -t',' -k7 -n | head -1

# Calculate average time by reducer count
awk -F',' 'NR>1 {sum[$4]+=$7; count[$4]++} END {for(r in sum) print r, sum[r]/count[r]}' benchmark_results.csv

# Count successful vs. failed tests
awk -F',' 'NR>1 {print $1}' benchmark_results.csv | sort | uniq -c
```

---

## 🔧 Customizing the Benchmark

### Change Dataset Sizes

Edit `benchmark_pipeline.sh` line 29:

```bash
DATASET_SIZES=(10 50 100 200)  # Change to your desired sizes
```

Examples:
```bash
DATASET_SIZES=(5 10 20)        # Smaller tests
DATASET_SIZES=(100 200 500)    # Medium to large
DATASET_SIZES=(1000)           # Single large test
```

### Change Reducer Counts

Edit `benchmark_pipeline.sh` line 30:

```bash
REDUCER_COUNTS=(2 4 8 16)      # Change to your desired counts
```

Examples:
```bash
REDUCER_COUNTS=(1 2 4)         # Minimal parallelism
REDUCER_COUNTS=(4 8 12 16 20)  # Fine-grained testing
REDUCER_COUNTS=(8)             # Single configuration
```

### Change Query

Edit or create `query.txt`:

```bash
echo "your custom search query here" > query.txt
./benchmark_pipeline.sh
```

The query affects Stage 2 (JPII) performance and number of similar documents found.

---

## 🎯 What to Look For

### Optimal Reducer Count

**Expected Pattern:**
- **Too few reducers**: Slow, underutilized cluster
- **Optimal reducers**: Fastest execution
- **Too many reducers**: Slower due to coordination overhead

**How to find:**
1. Look at CSV for each dataset size
2. Find reducer count with minimum total time
3. Verify the pattern across different dataset sizes

**Example:**
```
100 books, 2 reducers → 180s
100 books, 4 reducers → 120s ← Optimal
100 books, 8 reducers → 125s
100 books, 16 reducers → 135s
```

### Scaling Efficiency

**Linear scaling** (ideal):
- 2x data → 2x time
- Throughput stays constant

**Sub-linear scaling** (good):
- 2x data → <2x time
- Throughput increases with dataset size

**Super-linear scaling** (bottleneck):
- 2x data → >2x time
- Throughput decreases with dataset size

### Stage Comparison

**Check which stage is the bottleneck:**
- **Stage 1 dominates**: Index building is the bottleneck
  - Solution: More reducers, faster disks, compression
- **Stage 2 dominates**: Similarity computation is slow
  - Solution: Optimize query, reduce candidate pairs
- **Balanced**: Both stages take similar time (ideal)

---

## 🐛 Troubleshooting

### Benchmark Fails Immediately

**Check:**
```bash
# Hadoop running?
hadoop fs -ls /

# Scripts exist?
ls -la run_inverted_index_mapreduce.sh run_jpii.sh donwload_file.py

# Permissions?
chmod +x benchmark_pipeline.sh run_inverted_index_mapreduce.sh run_jpii.sh
```

### Individual Test Fails

**Check logs:**
```bash
# Find the failed test log
ls -la benchmark_logs/<timestamp>/

# View the log
cat benchmark_logs/<timestamp>/<failed_test>.log
```

**Common issues:**
- Out of memory: Reduce reducer count or increase memory in scripts
- HDFS full: Clean old data with `hdfs dfs -rm -r /gutenberg-*`
- Network issues: Check YARN NodeManager status

### Results Look Wrong

**Verify manually:**
```bash
# Run a single test manually
./run_pipeline.sh --num-books 10 --reducers 2

# Check HDFS
hdfs dfs -ls /gutenberg-input-10
hdfs dfs -ls /gutenberg-output-10
```

### Benchmark Takes Too Long

**Reduce test scope:**
```bash
# Edit benchmark_pipeline.sh
DATASET_SIZES=(10 50)          # Only 2 sizes
REDUCER_COUNTS=(2 4)           # Only 2 reducer counts
# Total: 4 tests instead of 16
```

---

## 📊 Example Results Interpretation

### Sample Output

```csv
Status,TestID,Books,Reducers,Stage1Time(s),Stage2Time(s),TotalTime(s),Throughput(books/s)
SUCCESS,10books_2reducers,10,2,45,12,57,0.17
SUCCESS,10books_4reducers,10,4,38,10,48,0.20
SUCCESS,50books_4reducers,50,4,120,35,155,0.32
SUCCESS,50books_8reducers,50,8,95,30,125,0.40
SUCCESS,100books_8reducers,100,8,180,55,235,0.42
SUCCESS,200books_16reducers,200,16,310,95,405,0.49
```

### Analysis

**Finding 1: Optimal Reducers Scale with Dataset**
- 10 books → 4 reducers optimal (48s vs 57s)
- 50 books → 8 reducers optimal (125s vs 155s)
- 100 books → 8 reducers optimal
- **Conclusion**: Use ~1 reducer per 6-7 books

**Finding 2: Throughput Increases with Dataset Size**
- 10 books: 0.20 books/s
- 50 books: 0.40 books/s
- 200 books: 0.49 books/s
- **Conclusion**: Cluster overhead amortized with larger datasets

**Finding 3: Stage 1 is the Bottleneck**
- Stage 1 takes ~75% of total time
- Stage 2 takes ~25% of total time
- **Conclusion**: Focus optimization on inverted index building

---

## 🚀 Next Steps

1. **Run the benchmark** with default settings
2. **Analyze the CSV** to find patterns
3. **Optimize configuration** based on findings
4. **Re-run with optimal settings** to verify
5. **Document your findings** for production use

---

## 📝 Tips

- Run benchmark during low-cluster-usage periods
- Ensure consistent cluster state (no other jobs running)
- Run multiple times to account for variance
- Keep logs for troubleshooting
- Monitor YARN ResourceManager UI during execution
- Check network and disk I/O if results are unexpected

---

## 🎓 What You'll Learn

From this benchmark, you'll understand:
- ✅ How reducer count affects performance
- ✅ How your pipeline scales with data size
- ✅ Which stage (index vs. similarity) is the bottleneck
- ✅ Optimal configuration for your cluster
- ✅ Resource utilization patterns
- ✅ Cost-performance trade-offs

Happy benchmarking! 🎉
