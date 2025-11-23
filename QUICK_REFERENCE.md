# Performance Benchmark - Quick Reference

## 🚀 Run Benchmark

```bash
# Full benchmark (16 tests: 10, 50, 100, 200 books × 2, 4, 8, 16 reducers)
./benchmark_pipeline.sh

# Estimated time: 1-3 hours
```

---

## 📊 View Results

### Console Summary
Results are displayed at the end of the benchmark run.

### CSV Analysis
```bash
# View in terminal
cat benchmark_results.csv | column -t -s,

# Analyze with Python script
python3 analyze_benchmark.py benchmark_results.csv

# Open in Excel/LibreOffice
# Import: benchmark_results.csv
```

### Check Logs
```bash
# List all logs
ls -la benchmark_logs/

# View specific test log
cat benchmark_logs/<timestamp>/<testname>.log
```

---

## 📈 Key Metrics Explained

| Metric | What It Measures | Why It Matters |
|--------|------------------|----------------|
| **Stage1Time** | Inverted index build time | Shows indexing performance |
| **Stage2Time** | Similarity computation time | Shows query performance |
| **TotalTime** | End-to-end pipeline time | Overall user experience |
| **Throughput** | Books processed per second | Processing efficiency |
| **UniqueWords** | Words in the index | Index complexity |
| **SimilarDocs** | Documents matching query | Search result count |

---

## 🎯 Find Optimal Configuration

### Method 1: Manual Analysis
```bash
# Find fastest config for 100 books
grep ",100," benchmark_results.csv | sort -t',' -k7 -n | head -1
```

### Method 2: Python Analysis
```bash
python3 analyze_benchmark.py benchmark_results.csv
# Look for "Optimal Reducer Configuration" section
```

### Method 3: Excel Pivot Table
1. Open `benchmark_results.csv` in Excel
2. Create Pivot Table
3. Rows: Books, Columns: Reducers, Values: Min(TotalTime)

---

## 🔧 Customize Benchmark

### Change Dataset Sizes

Edit `benchmark_pipeline.sh` line 29:
```bash
DATASET_SIZES=(10 50 100 200)  # Your sizes here
```

### Change Reducer Counts

Edit `benchmark_pipeline.sh` line 30:
```bash
REDUCER_COUNTS=(2 4 8 16)      # Your counts here
```

### Change Query

```bash
echo "your search terms" > query.txt
./benchmark_pipeline.sh
```

---

## 🐛 Quick Troubleshooting

### Benchmark won't start
```bash
# Check Hadoop
hadoop fs -ls /

# Check scripts exist
ls -la *.sh *.py

# Make executable
chmod +x benchmark_pipeline.sh *.sh
```

### Test failed
```bash
# Check the log
cat benchmark_logs/<timestamp>/<failed_test>.log

# Check HDFS space
hdfs dfs -df -h

# Check YARN nodes
yarn node -list
```

### Out of memory
```bash
# Reduce dataset sizes
DATASET_SIZES=(10 50)

# Or increase memory in run_inverted_index_mapreduce.sh:
# -D mapreduce.map.memory.mb=4096
# -D mapreduce.reduce.memory.mb=4096
```

---

## 📊 Typical Results Pattern

### Optimal Reducers Scale with Data
- 10-50 books → 2-4 reducers
- 100 books → 4-8 reducers
- 200 books → 8-16 reducers
- **Rule of thumb**: 1 reducer per 6-10 books

### Performance Characteristics
- Stage 1 typically takes 70-80% of total time
- Throughput increases with dataset size (overhead amortization)
- Too many reducers → coordination overhead
- Too few reducers → underutilized cluster

---

## 📁 Output Files Reference

```
benchmark_results.csv              ← Main results (Excel-compatible)
benchmark_logs/<timestamp>/        ← Detailed logs directory
  ├── 10books_2reducers.log
  ├── 10books_4reducers.log
  └── ...
BENCHMARK_GUIDE.md                 ← Detailed documentation
QUICK_REFERENCE.md                 ← This file
analyze_benchmark.py               ← Analysis tool
```

---

## 🎓 Common Analysis Tasks

### Find best configuration
```bash
python3 analyze_benchmark.py benchmark_results.csv
# → Look at "Optimal Reducer Configuration"
```

### Compare reducer counts
```bash
# For 100 books
grep ",100," benchmark_results.csv | awk -F',' '{print $4, $7}' | sort -k2 -n
```

### Calculate average time by reducers
```bash
awk -F',' 'NR>1 {sum[$4]+=$7; n[$4]++} END {for(r in sum) print r":", sum[r]/n[r]"s"}' benchmark_results.csv
```

### Show throughput trend
```bash
awk -F',' 'NR>1 {print $3, $13}' benchmark_results.csv | sort -n
```

---

## 💡 Performance Tips

### Before Running
- ✅ No other jobs on cluster
- ✅ Consistent cluster state
- ✅ Sufficient HDFS space
- ✅ All nodes healthy

### During Running
- 📊 Monitor YARN UI: http://master:8088
- 💾 Watch HDFS usage: `hdfs dfs -df -h`
- 🖥️ Check node status: `yarn node -list`

### After Running
- 📈 Analyze results immediately
- 💾 Save logs for reference
- 🔄 Re-run suspicious results
- 📝 Document findings

---

## 🎯 Quick Commands

```bash
# Run benchmark
./benchmark_pipeline.sh

# Analyze results
python3 analyze_benchmark.py benchmark_results.csv

# View CSV in terminal
cat benchmark_results.csv | column -t -s,

# Find optimal for 100 books
grep ",100," benchmark_results.csv | sort -t',' -k7 -n | head -1

# Check logs
ls benchmark_logs/$(ls -t benchmark_logs/ | head -1)/

# Clean up HDFS
hdfs dfs -rm -r /gutenberg-* /jpii-*
```

---

## 📞 Need Help?

- **Full Guide**: `BENCHMARK_GUIDE.md`
- **Analysis Tool**: `python3 analyze_benchmark.py --help`
- **Logs**: `benchmark_logs/<timestamp>/`
- **YARN UI**: `http://<master-node>:8088`

---

**Happy Benchmarking! 🎉**
