# Inverted Index MapReduce - Project Gutenberg Books

Build an inverted index from Project Gutenberg books using Hadoop MapReduce.

---

## 🚀 Quick Start (3 Commands)

```bash
# 1. Create HDFS directory
hdfs dfs -mkdir -p /gutenberg-input

# 2. Download books and upload to HDFS
python3 donwload_file.py

# 3. Run MapReduce job
./run_inverted_index_mapreduce.sh 2
```

**Time:** ~5-10 minutes | **Result:** Inverted index in `/gutenberg-output`

---

## 📋 What This Project Does

Creates an **inverted index** mapping words to documents:

```
word → [document1@count, document2@count, ...]
```

**Example output:**
```
animal    The_Extermination_of_the_American_Bison.txt@5234    Deadfalls_and_Snares.txt@3421
bison     The_Extermination_of_the_American_Bison.txt@5234
trap      Deadfalls_and_Snares.txt@3421    The_Extermination_of_the_American_Bison.txt@5234
```

**Use case:** Search engine backend, text analytics, document similarity

---

## 📦 Prerequisites

### Hadoop Cluster
- ✅ Hadoop 3.x installed and running
- ✅ YARN ResourceManager running
- ✅ HDFS accessible
- ✅ Worker nodes connected

**Verify:**
```bash
hadoop version
hdfs dfs -ls /
yarn node -list
```

### Software (Master Node Only!)
- ✅ Python 3.6+
- ✅ Python libraries: `pandas`, `requests`, `beautifulsoup4`, `gutenberg`, `colorama`

**Install on master:**
```bash
pip3 install --user --break-system-packages pandas requests beautifulsoup4 gutenberg colorama
```

### Worker Nodes
- ✅ **No special dependencies!**
- ✅ Just Python 3 (usually pre-installed)
- ✅ Mapper uses built-in libraries only

---

## 🎯 Step-by-Step Guide

### Step 1: Create HDFS Directory

```bash
# Create input directory
hdfs dfs -mkdir -p /gutenberg-input

# Verify
hdfs dfs -ls /
```

### Step 2: Download Books

The `donwload_file.py` script:
- Reads book metadata from `gutenberg_metadata_2books.csv`
- Downloads books from Project Gutenberg
- Cleans text
- Uploads to HDFS `/gutenberg-input`

**Run:**
```bash
python3 donwload_file.py
```

**Expected output:**
```
[LOCAL] File The_Extermination_of_the_American_Bison.txt saved successfully!
[LOCAL] Uploading The_Extermination_of_the_American_Bison.txt to HDFS...
[HDFS] File successfully uploaded to /gutenberg-input
===============================
[LOCAL] File Deadfalls_and_Snares.txt saved successfully!
[LOCAL] Uploading Deadfalls_and_Snares.txt to HDFS...
[HDFS] File successfully uploaded to /gutenberg-input
===============================
```

**Verify:**
```bash
hdfs dfs -ls /gutenberg-input
hdfs dfs -cat /gutenberg-input/The_Extermination_of_the_American_Bison.txt | head -50
```

### Step 3: Run MapReduce Job

```bash
./run_inverted_index_mapreduce.sh 2
```

**Arguments:**
- `2` = number of reducers (default: 2)
- Use more reducers for larger datasets

**What happens:**
1. ✅ Validates Hadoop environment
2. ✅ Checks HDFS input exists
3. ✅ Cleans output directory (if exists)
4. ✅ Submits job to YARN
5. ✅ **Map Phase:** Extract words, count per document
6. ✅ **Shuffle & Sort:** Group by word
7. ✅ **Reduce Phase:** Create inverted index
8. ✅ Writes output to `/gutenberg-output`
9. ✅ Displays sample results

**Progress monitoring:**
```bash
# Watch job status
yarn application -list

# Get application ID
yarn application -list | grep Inverted_Index

# View logs
yarn logs -applicationId <application_id>
```

### Step 4: View Results

```bash
# Sample output (first 20 lines)
hdfs dfs -cat /gutenberg-output/part-* | head -20

# Count unique words in index
hdfs dfs -cat /gutenberg-output/part-* | wc -l

# Search for specific word
hdfs dfs -cat /gutenberg-output/part-* | grep "^bison"

# Download to local machine
hadoop fs -get /gutenberg-output ./inverted_index_results
```

---

## 🏗️ Architecture

### Components

```
┌─────────────────────────────────────────────────────────────┐
│ MASTER NODE                                                  │
│ • NameNode (HDFS metadata)                                  │
│ • ResourceManager (YARN scheduler)                          │
│ • Python 3 + libraries (for download script)               │
└─────────────────────────────────────────────────────────────┘
         │
         ├─ Manages cluster
         ↓
┌──────────────────────────┐  ┌──────────────────────────────┐
│ WORKER NODE 1            │  │ WORKER NODE 2                │
│ • DataNode (storage)     │  │ • DataNode (storage)         │
│ • NodeManager (compute)  │  │ • NodeManager (compute)      │
│ • Python 3 (built-in)    │  │ • Python 3 (built-in)        │
│ • NO special deps needed!│  │ • NO special deps needed!    │
└──────────────────────────┘  └──────────────────────────────┘
```

### MapReduce Flow

```
Input (HDFS)
└─> book1.txt, book2.txt

Map Phase (Distributed)
├─> Mapper on Worker 1: Process book1.txt
│   ├─ Lowercase text
│   ├─ Remove punctuation
│   ├─ Remove stopwords (hardcoded list)
│   └─ Emit: word→filename@word_count
│
└─> Mapper on Worker 2: Process book2.txt
    └─ Same processing

Shuffle & Sort (YARN)
└─> Group by word key
    elephant → [book1@5000, book2@4500]
    lion → [book1@5000]

Reduce Phase (Distributed)
├─> Reducer 1: Process words A-M
│   ├─ Filter: Skip words in ALL documents
│   └─ Sort by count (descending)
│
└─> Reducer 2: Process words N-Z
    └─ Same processing

Output (HDFS)
└─> /gutenberg-output/part-00000, part-00001
```

---

## 📁 Project Files

### Core Files
- **inverted_index_mapper.py** - Map phase: Extract words, no external deps
- **inverted_index_reducer.py** - Reduce phase: Build inverted index
- **run_inverted_index_mapreduce.sh** - Main job submission script
- **donwload_file.py** - Download books from Project Gutenberg
- **gutenberg_metadata_2books.csv** - Book metadata

### Configuration
- **requirements.txt** - Python dependencies (master node only)

### Documentation
- **README.md** - This file
- **docs/multi-node-setup.md** - Detailed cluster setup
- **docs/troubleshooting.md** - Common issues and solutions

---

## 🔧 Customization

### Add More Books

Edit `gutenberg_metadata_2books.csv`:

```csv
Title,Author,Link,Bookshelf
Your Book Title,Author Name,http://www.gutenberg.org/ebooks/12345,Category
```

Then re-run:
```bash
python3 donwload_file.py
./run_inverted_index_mapreduce.sh 2
```

### Adjust Reducers

More reducers = better parallelism for large datasets:

```bash
# Small dataset (2-10 books)
./run_inverted_index_mapreduce.sh 2

# Medium dataset (10-100 books)
./run_inverted_index_mapreduce.sh 8

# Large dataset (100+ books)
./run_inverted_index_mapreduce.sh 16
```

**Rule of thumb:** 1 reducer per 2-4 worker nodes

### Modify Stopwords

Edit `inverted_index_mapper.py` line 17 - hardcoded stopwords set:

```python
stop_words = {'the', 'a', 'an', ...}  # Add/remove words here
```

---

## 🐛 Troubleshooting

### Issue: "No module named 'pandas'" on master

**Solution:**
```bash
pip3 install --user --break-system-packages pandas requests beautifulsoup4 gutenberg colorama
```

### Issue: "Input directory does not exist"

**Solution:**
```bash
hdfs dfs -mkdir -p /gutenberg-input
python3 donwload_file.py
```

### Issue: "Output directory already exists"

**Solution:**
```bash
# Remove old output
hdfs dfs -rm -r /gutenberg-output

# Re-run job
./run_inverted_index_mapreduce.sh 2
```

### Issue: MapReduce job fails

**Check logs:**
```bash
# Get application ID
yarn application -list

# View logs
yarn logs -applicationId application_XXX_YYYY
```

**Common causes:**
- Worker node down: `yarn node -list`
- HDFS full: `hdfs dfs -df -h`
- Insufficient memory: Adjust memory in script (line ~243-244)

**More help:** See `docs/troubleshooting.md`

---

## 📊 Performance Tips

### For Large Datasets

**Increase memory:**

Edit `run_inverted_index_mapreduce.sh` (lines ~243-244):
```bash
-D mapreduce.map.memory.mb=4096 \
-D mapreduce.reduce.memory.mb=4096 \
```

**Enable compression:**

Add to hadoop command:
```bash
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
```

**Optimize input splits:**

```bash
-D mapreduce.input.fileinputformat.split.maxsize=134217728 \  # 128MB
```

---

## 📚 Additional Resources

- **Multi-Node Setup:** `docs/multi-node-setup.md` - Detailed cluster configuration
- **Troubleshooting:** `docs/troubleshooting.md` - Common issues and solutions
- **Hadoop Docs:** https://hadoop.apache.org/docs/stable/
- **Project Gutenberg:** https://www.gutenberg.org/

---

## 🎓 Learning Objectives

This project demonstrates:
- ✅ Hadoop MapReduce programming model
- ✅ Distributed text processing
- ✅ HDFS file operations
- ✅ YARN job submission and monitoring
- ✅ Inverted index data structure
- ✅ Real-world search engine concepts

---

## 📝 Notes

### Why No NLTK on Workers?

- Mapper uses **hardcoded stopwords** (line 17 in mapper)
- No external dependencies = simpler deployment
- Faster execution (no library loading)
- Works on any Python 3 environment

### Python 3.12+ on Ubuntu

If you see "externally-managed-environment" error:

```bash
# Use --break-system-packages flag
pip3 install --user --break-system-packages pandas requests beautifulsoup4 gutenberg colorama
```

This is a Python 3.12+ security feature. Safe to bypass for user installs.

---

## 🤝 Contributing

To extend this project:
1. Add more books to CSV
2. Modify stopwords in mapper
3. Adjust reducer logic (filter criteria, sorting)
4. Add more reducers for parallelism
5. Enable output compression

---

## 📄 License

Educational project for learning Hadoop MapReduce.

---

**Ready to start?**

```bash
hdfs dfs -mkdir -p /gutenberg-input
python3 donwload_file.py
./run_inverted_index_mapreduce.sh 2
```

Enjoy your inverted index! 🎉
