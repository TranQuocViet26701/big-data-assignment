# Quick Start Guide

## ✅ Verification Complete

Your `donwload_file.py` script is **correct and ready to use**!

- ✅ Script verified
- ✅ CSV metadata file exists (2 books)
- ✅ Will download and upload to HDFS `/gutenberg-input`

---

## 🚀 Three Ways to Run

### Option 1: Automated (Recommended) ⭐

**One command does everything:**

```bash
./setup_and_run.sh
```

This script automatically:
1. ✅ Installs Python dependencies (if needed)
2. ✅ Creates HDFS directory `/gutenberg-input`
3. ✅ Downloads books and uploads to HDFS
4. ✅ Runs MapReduce job
5. ✅ Displays results

**Time:** 5-10 minutes

---

### Option 2: Step-by-Step (Manual Control)

```bash
# Step 1: Install dependencies (one-time)
./install_all_dependencies_ubuntu.sh

# Step 2: Create HDFS directory
hdfs dfs -mkdir -p /gutenberg-input

# Step 3: Download and upload books
python3 donwload_file.py

# Step 4: Run MapReduce
./run_inverted_index_mapreduce.sh 2

# Step 5: View results
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

---

### Option 3: Quick Test (Skip Installation)

If dependencies are already installed:

```bash
# Create HDFS directory
hdfs dfs -mkdir -p /gutenberg-input

# Download books
python3 donwload_file.py

# Run job
./run_inverted_index_mapreduce.sh 2
```

---

## 📋 What You Need

### Already Have:
- ✅ `donwload_file.py` script (verified)
- ✅ `gutenberg_metadata_2books.csv` (2 books)
- ✅ Hadoop cluster running
- ✅ HDFS accessible

### Will Be Installed:
- Python libraries: pandas, numpy, requests, beautifulsoup4, gutenberg, colorama (master only)
- NLTK library + data (all nodes)

---

## 🎯 Expected Flow

```
1. Install Dependencies (2-5 minutes)
   └─ Installs pandas, numpy, requests, etc.

2. Create HDFS Directory (5 seconds)
   └─ mkdir /gutenberg-input

3. Download Books (1-3 minutes)
   ├─ Download "The Extermination of the American Bison"
   ├─ Download "Deadfalls and Snares"
   ├─ Clean text
   ├─ Save locally as .txt files
   ├─ Upload to HDFS /gutenberg-input
   └─ Delete local files

4. Run MapReduce (2-5 minutes)
   ├─ Validate environment
   ├─ Setup NLTK
   ├─ Verify input exists (2 books)
   ├─ Submit job to YARN
   ├─ Map phase: Process books, extract words
   ├─ Shuffle & sort
   ├─ Reduce phase: Create inverted index
   └─ Write output to /gutenberg-output

5. View Results (instant)
   └─ Display sample inverted index
```

---

## 📊 Expected Output

### During Download:
```
[LOCAL] File The_Extermination_of_the_American_Bison.txt saved successfully!
[LOCAL] Uploading The_Extermination_of_the_American_Bison.txt to HDFS path /gutenberg-input...
[HDFS] File The_Extermination_of_the_American_Bison.txt successfully uploaded to /gutenberg-input
===============================
[LOCAL] File Deadfalls_and_Snares.txt saved successfully!
[LOCAL] Uploading Deadfalls_and_Snares.txt to HDFS path /gutenberg-input...
[HDFS] File Deadfalls_and_Snares.txt successfully uploaded to /gutenberg-input
===============================
```

### During MapReduce:
```
========================================
Step 3: HDFS Preparation
========================================
[INFO] Checking input directory: /gutenberg-input
[SUCCESS] Found 2 input files in /gutenberg-input
[INFO] Input files:
  - The_Extermination_of_the_American_Bison.txt
  - Deadfalls_and_Snares.txt
...
========================================
Step 6: Results Summary
========================================
[SUCCESS] Inverted index created successfully!

Sample output (first 20 lines):
----------------------------------------
animal	The_Extermination_of_the_American_Bison.txt@5234	Deadfalls_and_Snares.txt@3421
bison	The_Extermination_of_the_American_Bison.txt@5234
trap	Deadfalls_and_Snares.txt@3421	The_Extermination_of_the_American_Bison.txt@5234
...
```

---

## 🔧 Troubleshooting

### Issue: "No module named 'pandas'"

**Solution:**
```bash
# Install dependencies
./install_all_dependencies_ubuntu.sh

# OR manually
pip3 install --user --break-system-packages pandas numpy requests beautifulsoup4 gutenberg colorama
```

### Issue: "Input directory does not exist"

**Solution:**
```bash
# Create HDFS directory
hdfs dfs -mkdir -p /gutenberg-input

# Then run download script
python3 donwload_file.py
```

### Issue: "Permission denied" when downloading

**Solution:**
```bash
# Check HDFS permissions
hdfs dfs -ls /

# Fix permissions
hdfs dfs -chmod 755 /gutenberg-input
```

### Issue: Download fails (network/API error)

The script has fallback mechanisms:
1. Tries Project Gutenberg mirror
2. Falls back to main Gutenberg site
3. If both fail, prints error but continues

**Manual fix if needed:**
```bash
# Check internet connection
ping www.gutenberg.org

# Try running again
python3 donwload_file.py
```

---

## 📁 Files Created

### Local (temporary):
- `The_Extermination_of_the_American_Bison.txt` (deleted after upload)
- `Deadfalls_and_Snares.txt` (deleted after upload)

### HDFS Input:
```
/gutenberg-input/
├── The_Extermination_of_the_American_Bison.txt
└── Deadfalls_and_Snares.txt
```

### HDFS Output:
```
/gutenberg-output/
├── part-00000
├── part-00001
└── _SUCCESS
```

---

## 🎯 Verification Commands

### Check HDFS input:
```bash
hdfs dfs -ls /gutenberg-input
hdfs dfs -cat /gutenberg-input/The_Extermination_of_the_American_Bison.txt | head -50
```

### Check HDFS output:
```bash
hdfs dfs -ls /gutenberg-output
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

### Count unique words in index:
```bash
hdfs dfs -cat /gutenberg-output/part-* | wc -l
```

### Search for specific word:
```bash
hdfs dfs -cat /gutenberg-output/part-* | grep "^bison"
```

---

## 📖 Next Steps After Success

### 1. Download Results
```bash
hadoop fs -get /gutenberg-output ./inverted_index_results
```

### 2. Analyze Results
```bash
cd inverted_index_results
cat part-* | less
```

### 3. Add More Books
Edit `gutenberg_metadata_2books.csv` to add more books, then re-run:
```bash
python3 donwload_file.py
./run_inverted_index_mapreduce.sh 2
```

### 4. Run with Different Parameters
```bash
# Try different number of reducers
./run_inverted_index_mapreduce.sh 4
```

---

## Summary

**✅ Your download script is ready!**

**Recommended command:**
```bash
./setup_and_run.sh
```

This runs the entire pipeline automatically. Sit back and watch! ☕

**Time:** ~5-10 minutes total

**Result:** Complete inverted index in `/gutenberg-output`

---

**Need help?** See:
- **UBUNTU_PYTHON312_FIX.md** - Python 3.12 issues
- **TROUBLESHOOTING_SUDO.md** - Installation errors
- **MULTI_NODE_DEPLOYMENT_GUIDE.md** - Full deployment guide
