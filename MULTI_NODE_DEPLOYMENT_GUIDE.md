# Multi-Node YARN/Hadoop Deployment Guide
## Inverted Index MapReduce Job on Distributed Cluster

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Cluster Setup Requirements](#cluster-setup-requirements)
4. [Step-by-Step Deployment](#step-by-step-deployment)
5. [Execution Flow](#execution-flow)
6. [Verification and Testing](#verification-and-testing)
7. [Troubleshooting](#troubleshooting)
8. [Performance Tuning](#performance-tuning)

---

## Architecture Overview

### Multi-Node Hadoop Cluster Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        MASTER NODE                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐ │
│  │   NameNode       │  │  ResourceManager │  │  JobHistory   │ │
│  │   (HDFS Master)  │  │   (YARN Master)  │  │    Server     │ │
│  └──────────────────┘  └──────────────────┘  └───────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
┌───────────────▼────────────┐  ┌──────────────▼────────────────┐
│      WORKER NODE 1         │  │      WORKER NODE 2            │
│  ┌──────────┐ ┌──────────┐ │  │  ┌──────────┐ ┌──────────┐   │
│  │DataNode  │ │NodeManager│ │  │  │DataNode  │ │NodeManager│  │
│  │(Storage) │ │(Compute)  │ │  │  │(Storage) │ │(Compute)  │  │
│  └──────────┘ └──────────┘ │  │  └──────────┘ └──────────┘   │
│                             │  │                               │
│  Python3 + NLTK installed   │  │  Python3 + NLTK installed    │
└─────────────────────────────┘  └───────────────────────────────┘
                ...additional worker nodes...
```

### MapReduce Job Flow

```
┌──────────────────────────────────────────────────────────────────┐
│ STEP 1: Data Preparation                                         │
│ ┌──────────────┐                                                 │
│ │ Local Machine│───┐                                             │
│ │ Run:         │   │                                             │
│ │ download     │   │  Upload books to HDFS                       │
│ │ _file.py     │   │                                             │
│ └──────────────┘   │                                             │
│                    ▼                                             │
│              ┌──────────┐                                         │
│              │   HDFS   │                                         │
│              │/gutenberg│                                         │
│              │  -input/ │                                         │
│              │  book1.txt                                        │
│              │  book2.txt                                        │
│              │  ...      │                                        │
│              └──────────┘                                         │
└──────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────┐
│ STEP 2: Map Phase (Distributed across Worker Nodes)             │
│                                                                   │
│  Worker 1: book1.txt        Worker 2: book2.txt                 │
│  ┌────────────────┐         ┌────────────────┐                  │
│  │ Mapper Instance│         │ Mapper Instance│                  │
│  │ - Read text    │         │ - Read text    │                  │
│  │ - Lowercase    │         │ - Lowercase    │                  │
│  │ - Remove punct │         │ - Remove punct │                  │
│  │ - Remove stops │         │ - Remove stops │                  │
│  │ - Lemmatize    │         │ - Lemmatize    │                  │
│  └────────┬───────┘         └────────┬───────┘                  │
│           │                          │                           │
│           │ Output:                  │ Output:                   │
│           │ elephant→book1@5000      │ elephant→book2@4500       │
│           │ lion→book1@5000          │ giraffe→book2@4500        │
│           │ zebra→book1@5000         │ zebra→book2@4500          │
│           └──────────┬───────────────┘                           │
│                      │                                            │
└──────────────────────┼────────────────────────────────────────────┘
                       │
                       ▼ Shuffle & Sort by Key
┌──────────────────────────────────────────────────────────────────┐
│ STEP 3: Shuffle & Sort (YARN handles this)                      │
│                                                                   │
│  Group by word key:                                              │
│  elephant → [book1@5000, book2@4500]                            │
│  giraffe  → [book2@4500]                                         │
│  lion     → [book1@5000]                                         │
│  zebra    → [book1@5000, book2@4500]                            │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ STEP 4: Reduce Phase (Distributed across Worker Nodes)          │
│                                                                   │
│  Reducer 1:                   Reducer 2:                         │
│  ┌────────────────┐           ┌────────────────┐                │
│  │ elephant →     │           │ lion →         │                │
│  │ [book1@5000,   │           │ [book1@5000]   │                │
│  │  book2@4500]   │           │                │                │
│  │                │           │ giraffe →      │                │
│  │ Filter common  │           │ [book2@4500]   │                │
│  │ Sort by count  │           │                │                │
│  └────────┬───────┘           └────────┬───────┘                │
│           │                            │                         │
│           │ Output:                    │ Output:                 │
│           │ elephant→book1@5000→       │ lion→book1@5000         │
│           │          book2@4500        │ giraffe→book2@4500      │
│           └────────┬───────────────────┘                         │
└────────────────────┼──────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ STEP 5: Output (Written to HDFS)                                │
│              ┌──────────────┐                                    │
│              │     HDFS     │                                    │
│              │ /gutenberg-  │                                    │
│              │   output/    │                                    │
│              │ part-00000   │                                    │
│              │ part-00001   │                                    │
│              │ _SUCCESS     │                                    │
│              └──────────────┘                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

### Cluster Requirements

**Minimum Recommended Setup:**
- 1 Master Node (4 CPU cores, 8GB RAM, 50GB disk)
- 2+ Worker Nodes (2 CPU cores, 4GB RAM each, 50GB disk)
- All nodes on same network with SSH access
- Hadoop 3.x installed and configured
- YARN properly configured and running

### Software Requirements (All Nodes)

| Software | Version | Purpose |
|----------|---------|---------|
| Java | JDK 8 or 11 | Hadoop runtime |
| Python | 3.6+ | MapReduce scripts |
| Hadoop | 3.x | Distributed processing |
| NLTK | Latest | Text processing in mapper |

---

## Cluster Setup Requirements

### Network Configuration

All nodes must be able to communicate:

```bash
# On master node, verify connectivity to all workers
ssh worker1
ssh worker2
# etc.

# Verify HDFS and YARN are accessible
hdfs dfsadmin -report
yarn node -list
```

### HDFS and YARN Status Check

```bash
# Check HDFS health
hdfs dfsadmin -report

# Expected output showing DataNodes:
# Live datanodes (2):
# Name: worker1:9866
# Name: worker2:9866

# Check YARN nodes
yarn node -list -all

# Expected output showing NodeManagers:
# worker1:8042    RUNNING
# worker2:8042    RUNNING
```

---

## Step-by-Step Deployment

### Step 1: Install Python Dependencies on ALL Worker Nodes

**WHY THIS IS CRITICAL:**
- Hadoop Streaming executes your Python scripts on worker nodes
- Each mapper/reducer task runs where the data is located
- If NLTK is missing on any worker, tasks will fail on that node

#### Option A: Manual Installation on Each Node

**On Master Node:**
```bash
# Install dependencies
sudo yum install -y python3 python3-pip  # For CentOS/RHEL
# OR
sudo apt-get install -y python3 python3-pip  # For Ubuntu/Debian

# Install NLTK
sudo pip3 install nltk

# Download NLTK data
sudo python3 << 'EOF'
import nltk
import os

nltk_data_dir = '/root/nltk_data'
os.makedirs(nltk_data_dir, exist_ok=True)

nltk.download('stopwords', download_dir=nltk_data_dir)
nltk.download('wordnet', download_dir=nltk_data_dir)
nltk.download('punkt', download_dir=nltk_data_dir)
nltk.download('omw-1.4', download_dir=nltk_data_dir)
EOF
```

**Repeat on Worker Node 1:**
```bash
ssh worker1
sudo yum install -y python3 python3-pip
sudo pip3 install nltk
sudo python3 << 'EOF'
import nltk
import os
nltk_data_dir = '/root/nltk_data'
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.download('stopwords', download_dir=nltk_data_dir)
nltk.download('wordnet', download_dir=nltk_data_dir)
nltk.download('punkt', download_dir=nltk_data_dir)
nltk.download('omw-1.4', download_dir=nltk_data_dir)
EOF
exit
```

**Repeat on Worker Node 2, 3, etc.:**
```bash
ssh worker2
# ... same commands as worker1
exit
```

#### Option B: Automated Installation Using Script

Create a script to install on all nodes:

```bash
# Create installation script
cat > install_dependencies.sh << 'EOF'
#!/bin/bash
echo "Installing Python and dependencies..."
sudo yum install -y python3 python3-pip || sudo apt-get install -y python3 python3-pip

echo "Installing NLTK..."
sudo pip3 install nltk

echo "Downloading NLTK data..."
sudo python3 << 'PYEOF'
import nltk
import os

nltk_data_dir = '/root/nltk_data'
os.makedirs(nltk_data_dir, exist_ok=True)

print("Downloading stopwords...")
nltk.download('stopwords', download_dir=nltk_data_dir)
print("Downloading wordnet...")
nltk.download('wordnet', download_dir=nltk_data_dir)
print("Downloading punkt...")
nltk.download('punkt', download_dir=nltk_data_dir)
print("Downloading omw-1.4...")
nltk.download('omw-1.4', download_dir=nltk_data_dir)
print("Done!")
PYEOF

echo "Installation complete on $(hostname)"
EOF

chmod +x install_dependencies.sh

# Run on all nodes
./install_dependencies.sh  # Master
ssh worker1 'bash -s' < install_dependencies.sh
ssh worker2 'bash -s' < install_dependencies.sh
# Add more workers as needed
```

#### Option C: Using Parallel SSH (pssh)

If you have many nodes:

```bash
# Install pssh
sudo yum install -y pssh  # or apt-get install pssh

# Create hosts file
cat > cluster_hosts.txt << EOF
master
worker1
worker2
worker3
EOF

# Run installation in parallel on all nodes
pssh -h cluster_hosts.txt -i -t 300 'sudo yum install -y python3 python3-pip && sudo pip3 install nltk'

# Download NLTK data on all nodes
pssh -h cluster_hosts.txt -i -t 300 'sudo python3 -c "
import nltk
import os
nltk_data_dir = \"/root/nltk_data\"
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.download(\"stopwords\", download_dir=nltk_data_dir)
nltk.download(\"wordnet\", download_dir=nltk_data_dir)
nltk.download(\"punkt\", download_dir=nltk_data_dir)
nltk.download(\"omw-1.4\", download_dir=nltk_data_dir)
"'
```

### Step 2: Verify Installation on All Nodes

**Create verification script:**

```bash
cat > verify_setup.sh << 'EOF'
#!/bin/bash
echo "Verifying Python and NLTK on $(hostname)..."

# Check Python
python3 --version || { echo "ERROR: Python3 not found"; exit 1; }

# Check NLTK
python3 -c "import nltk; print('NLTK version:', nltk.__version__)" || { echo "ERROR: NLTK not installed"; exit 1; }

# Check NLTK data
python3 << 'PYEOF'
import os
import sys

nltk_data_dir = '/root/nltk_data'
required = ['stopwords', 'wordnet', 'punkt', 'omw-1.4']
missing = []

for item in required:
    path = os.path.join(nltk_data_dir, 'corpora', item)
    if not os.path.exists(path):
        path = os.path.join(nltk_data_dir, 'tokenizers', item)
        if not os.path.exists(path):
            missing.append(item)

if missing:
    print(f"ERROR: Missing NLTK data: {missing}")
    sys.exit(1)
else:
    print("SUCCESS: All NLTK data found")
PYEOF

echo "✓ $(hostname) is ready for MapReduce job"
EOF

chmod +x verify_setup.sh

# Verify all nodes
./verify_setup.sh  # Master
ssh worker1 'bash -s' < verify_setup.sh
ssh worker2 'bash -s' < verify_setup.sh
# Verify all workers
```

### Step 3: Upload Data to HDFS

```bash
# Run the download script to fetch books and upload to HDFS
python3 donwload_file.py

# Verify data in HDFS
hdfs dfs -ls /gutenberg-input

# Check file count
hdfs dfs -ls /gutenberg-input | grep -c "^-"

# View sample data
hdfs dfs -cat /gutenberg-input/*.txt | head -20
```

### Step 4: Verify Hadoop Scripts

```bash
# Make sure mapper and reducer are executable
chmod +x inverted_index_mapper.py
chmod +x inverted_index_reducer.py

# Test mapper locally (not required but helpful for debugging)
echo "The elephant walked through the forest" | ./inverted_index_mapper.py

# Test reducer locally
echo -e "elephant\tbook1@100\nelephant\tbook2@100" | ./inverted_index_reducer.py
```

---

## Execution Flow

### Complete Pipeline Flow

```
1. Data Preparation (One-time)
   ├─ Run: python3 donwload_file.py
   ├─ Downloads books from Project Gutenberg
   ├─ Cleans text content
   └─ Uploads to HDFS: /gutenberg-input/

2. Submit MapReduce Job
   ├─ Run: ./run_inverted_index_mapreduce.sh [NUM_REDUCERS]
   │
   ├─ Phase 1: Environment Validation
   │   ├─ Check HADOOP_HOME
   │   ├─ Find Hadoop streaming jar
   │   ├─ Verify scripts exist
   │   └─ Test HDFS connectivity
   │
   ├─ Phase 2: NLTK Setup (handled by script)
   │   ├─ Check NLTK data exists
   │   └─ Download if missing (master node only)
   │
   ├─ Phase 3: HDFS Preparation
   │   ├─ Verify input files exist
   │   ├─ Count documents
   │   └─ Clean output directory
   │
   ├─ Phase 4: Job Submission
   │   ├─ YARN ResourceManager receives job
   │   ├─ ApplicationMaster started
   │   └─ Tasks distributed to NodeManagers
   │
   ├─ Phase 5: Map Phase Execution
   │   ├─ YARN assigns input splits to mappers
   │   ├─ Each mapper processes assigned files
   │   ├─ Mappers run inverted_index_mapper.py
   │   │   ├─ Read text from STDIN
   │   │   ├─ Load NLTK stopwords (needs to exist on that node!)
   │   │   ├─ Load NLTK lemmatizer (needs to exist on that node!)
   │   │   ├─ Process: lowercase → remove punct → remove stops → lemmatize
   │   │   └─ Emit: word→filename@total_words
   │   └─ Output written to local disk (shuffle buffer)
   │
   ├─ Phase 6: Shuffle & Sort
   │   ├─ YARN sorts mapper output by key
   │   ├─ Partitions data for reducers
   │   └─ Transfers data to reducer nodes
   │
   ├─ Phase 7: Reduce Phase Execution
   │   ├─ Each reducer processes assigned keys
   │   ├─ Reducers run inverted_index_reducer.py
   │   │   ├─ Group by word
   │   │   ├─ Filter words in ALL documents (too common)
   │   │   ├─ Sort documents by word count (descending)
   │   │   └─ Emit: word→doc1@count→doc2@count→...
   │   └─ Output written to HDFS: /gutenberg-output/part-*
   │
   └─ Phase 8: Job Completion
       ├─ ApplicationMaster reports success
       ├─ Creates _SUCCESS file in output
       └─ Script displays results summary

3. View Results
   ├─ Sample output displayed automatically
   ├─ Download: hadoop fs -get /gutenberg-output ./results
   └─ Analyze: Count unique words, search terms, etc.
```

### Running the Job

```bash
# Navigate to project directory
cd /Users/viettq/Desktop/Projects/MasterBK/DataEngineering/big-data-assignment

# Run with default settings (2 reducers)
./run_inverted_index_mapreduce.sh

# Run with custom number of reducers (recommended: 1 reducer per 2-4 worker nodes)
./run_inverted_index_mapreduce.sh 4

# Monitor job progress
yarn application -list

# View job logs
yarn logs -applicationId <application_id>
```

### Understanding Resource Allocation

```
Total Cluster Resources:
├─ Master Node: Runs NameNode + ResourceManager (no tasks executed here)
└─ Worker Nodes: Run DataNode + NodeManager (tasks execute here)

For a 3-node cluster (1 master + 2 workers):
├─ Worker 1: May run 2 mappers + 1 reducer (depends on available memory/cores)
└─ Worker 2: May run 2 mappers + 1 reducer

Task Assignment:
├─ Mappers: Assigned to nodes with data locality (where file blocks are)
└─ Reducers: Assigned to any available node with resources
```

---

## Verification and Testing

### 1. Pre-Execution Checklist

```bash
# Verify all nodes are online
yarn node -list
hdfs dfsadmin -report

# Verify NLTK on all workers
for node in worker1 worker2 worker3; do
    echo "Checking $node..."
    ssh $node "python3 -c 'import nltk; from nltk.corpus import stopwords; print(\"OK\")'"
done

# Verify input data
hdfs dfs -ls /gutenberg-input
hdfs dfs -count /gutenberg-input
```

### 2. During Execution Monitoring

```bash
# Monitor running applications
watch -n 2 'yarn application -list'

# View job details
yarn application -status <application_id>

# Monitor resource usage
yarn top

# View specific task logs (if job fails)
yarn logs -applicationId <application_id>

# Check HDFS usage
hdfs dfs -du -h /
```

### 3. Post-Execution Verification

```bash
# Verify output created
hdfs dfs -ls /gutenberg-output

# Check for _SUCCESS file
hdfs dfs -test -e /gutenberg-output/_SUCCESS && echo "Job completed successfully"

# Count output records
hdfs dfs -cat /gutenberg-output/part-* | wc -l

# View sample output
hdfs dfs -cat /gutenberg-output/part-00000 | head -20

# Download results locally
hadoop fs -get /gutenberg-output ./inverted_index_results
```

### 4. Validate Output Quality

```bash
# Check inverted index structure
hdfs dfs -cat /gutenberg-output/part-* | head -5

# Expected format:
# word<TAB>doc1@count<TAB>doc2@count<TAB>doc3@count
# Example:
# elephant    The_Extermination_of_the_American_Bison.txt@1234    Deadfalls_and_Snares.txt@567

# Search for specific words
hdfs dfs -cat /gutenberg-output/part-* | grep "^elephant"

# Count unique words in index
hdfs dfs -cat /gutenberg-output/part-* | wc -l
```

---

## Troubleshooting

### Common Issues and Solutions

#### Issue 1: Task Failures with "ModuleNotFoundError: No module named 'nltk'"

**Cause:** NLTK not installed on worker node where task executed

**Solution:**
```bash
# Find which node failed
yarn logs -applicationId <app_id> | grep "ModuleNotFoundError"

# SSH to that node and install
ssh <failed_node>
sudo pip3 install nltk
exit

# Re-run job
./run_inverted_index_mapreduce.sh
```

#### Issue 2: Task Failures with NLTK Data Not Found

**Cause:** NLTK data missing on worker node

**Error message:**
```
Resource stopwords not found.
Please use the NLTK Downloader to obtain the resource
```

**Solution:**
```bash
# Install NLTK data on the problematic node
ssh <failed_node>
sudo python3 << 'EOF'
import nltk
import os
nltk_data_dir = '/root/nltk_data'
os.makedirs(nltk_data_dir, exist_ok=True)
nltk.download('stopwords', download_dir=nltk_data_dir)
nltk.download('wordnet', download_dir=nltk_data_dir)
nltk.download('punkt', download_dir=nltk_data_dir)
nltk.download('omw-1.4', download_dir=nltk_data_dir)
EOF
exit

# Verify installation
ssh <failed_node> "python3 -c 'from nltk.corpus import stopwords; print(len(stopwords.words(\"english\")))'"
```

#### Issue 3: Output Directory Already Exists

**Error:**
```
Output directory hdfs://namenode:9000/gutenberg-output already exists
```

**Solution:**
The script handles this automatically, but if running manually:
```bash
hdfs dfs -rm -r /gutenberg-output
```

#### Issue 4: Insufficient YARN Resources

**Error:**
```
Application application_xxx failed due to insufficient resources
```

**Solution:**
```bash
# Check available resources
yarn node -list

# Reduce number of reducers
./run_inverted_index_mapreduce.sh 1

# Or adjust memory settings in script (edit line ~165):
# -D mapreduce.map.memory.mb=1024
# -D mapreduce.reduce.memory.mb=1024
```

#### Issue 5: Python Version Mismatch

**Error:**
```
/usr/bin/env: 'python3': No such file or directory
```

**Solution:**
```bash
# On all nodes, create python3 symlink
sudo ln -s /usr/bin/python3.6 /usr/bin/python3
# Or install python3
sudo yum install -y python3
```

#### Issue 6: Permission Denied on HDFS

**Error:**
```
Permission denied: user=hadoop, access=WRITE
```

**Solution:**
```bash
# Set proper HDFS permissions
hdfs dfs -chmod -R 755 /gutenberg-input
hdfs dfs -chown -R hadoop:hadoop /gutenberg-input

# Or run as hdfs superuser
sudo -u hdfs hadoop jar ...
```

---

## Performance Tuning

### Optimizing for Larger Datasets

#### 1. Adjust Number of Reducers

```bash
# Rule of thumb: 0.95-1.75 × (nodes × mapred.tasktracker.reduce.tasks.maximum)
# For 10 workers with 2 reduce slots each = 10-20 reducers

# Run with optimal reducers
./run_inverted_index_mapreduce.sh 15
```

#### 2. Tune Memory Settings

Edit `run_inverted_index_mapreduce.sh` (around line 165):

```bash
# For large documents or many words, increase memory:
-D mapreduce.map.memory.mb=4096 \
-D mapreduce.reduce.memory.mb=4096 \
-D mapreduce.map.java.opts=-Xmx3072m \
-D mapreduce.reduce.java.opts=-Xmx3072m \
```

#### 3. Enable Compression

Add to Hadoop streaming command:

```bash
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
```

#### 4. Optimize Input Splits

```bash
# For many small files, combine them:
-D mapreduce.input.fileinputformat.split.maxsize=134217728 \  # 128MB
```

### Monitoring Performance

```bash
# View job counters
yarn application -status <app_id>

# View detailed task times
mapred job -history <output_dir>

# Resource usage per node
yarn node -status <node_id>
```

---

## Quick Reference Commands

### Job Execution
```bash
# Submit job
./run_inverted_index_mapreduce.sh [NUM_REDUCERS]

# Monitor job
yarn application -list
yarn application -status <app_id>
yarn logs -applicationId <app_id>

# Kill job if needed
yarn application -kill <app_id>
```

### HDFS Operations
```bash
# View input
hdfs dfs -ls /gutenberg-input
hdfs dfs -cat /gutenberg-input/*.txt | head

# View output
hdfs dfs -ls /gutenberg-output
hdfs dfs -cat /gutenberg-output/part-* | head -20

# Download results
hadoop fs -get /gutenberg-output ./results

# Clean up
hdfs dfs -rm -r /gutenberg-output
```

### Cluster Health
```bash
# Check nodes
yarn node -list
hdfs dfsadmin -report

# Check services
jps  # On each node to see running Java processes

# View logs
yarn logs -applicationId <app_id>
tail -f $HADOOP_HOME/logs/hadoop-*-namenode-*.log
```

---

## Summary

### Key Points for Multi-Node Deployment

1. **Python + NLTK must be installed on ALL worker nodes** - This is critical!
2. **NLTK data must exist at `/root/nltk_data` on ALL worker nodes**
3. **Hadoop streaming distributes your scripts** - They execute where data lives
4. **Test on small dataset first** - Verify setup before processing large data
5. **Monitor YARN during execution** - Watch for task failures
6. **Set appropriate number of reducers** - Usually 1 per 2-4 workers
7. **Check logs if failures occur** - YARN logs show exactly what went wrong

### Execution Checklist

- [ ] All worker nodes have Python 3 installed
- [ ] All worker nodes have NLTK installed (`pip3 install nltk`)
- [ ] All worker nodes have NLTK data at `/root/nltk_data`
- [ ] HDFS has input data at `/gutenberg-input`
- [ ] Mapper and reducer scripts are executable
- [ ] YARN ResourceManager is running
- [ ] All NodeManagers are healthy
- [ ] Sufficient HDFS space available
- [ ] Run verification script on all nodes
- [ ] Execute `./run_inverted_index_mapreduce.sh`
- [ ] Monitor job via `yarn application -list`
- [ ] Verify output in `/gutenberg-output`

---

## Contact and Support

For issues or questions:
- Check Hadoop logs: `$HADOOP_HOME/logs/`
- Check YARN logs: `yarn logs -applicationId <app_id>`
- Verify node health: `yarn node -list`
- Test scripts locally before submitting to cluster

Good luck with your MapReduce job!
