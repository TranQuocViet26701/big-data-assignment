# Multi-Node Hadoop Cluster Setup

Complete guide for deploying the Inverted Index MapReduce job on a multi-node Hadoop cluster.

---

## Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MASTER NODE                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐ │
│  │   NameNode       │  │  ResourceManager │  │  JobHistory   │ │
│  │   (HDFS Master)  │  │   (YARN Master)  │  │    Server     │ │
│  └──────────────────┘  └──────────────────┘  └───────────────┘ │
│                                                                  │
│  Software Requirements:                                         │
│  • Hadoop 3.x                                                   │
│  • Python 3.6+ (for download script)                           │
│  • pandas, requests, beautifulsoup4, gutenberg, colorama       │
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
│  Software Requirements:     │  │  Software Requirements:       │
│  • Python 3 (usually        │  │  • Python 3 (usually          │
│    pre-installed)           │  │    pre-installed)             │
│  • NO special libraries!    │  │  • NO special libraries!      │
└─────────────────────────────┘  └───────────────────────────────┘
```

---

## Prerequisites

### Minimum Cluster Requirements

- **1 Master Node:** 4 CPU cores, 8GB RAM, 50GB disk
- **2+ Worker Nodes:** 2 CPU cores, 4GB RAM each, 50GB disk
- All nodes on same network with connectivity
- Hadoop 3.x installed and configured
- YARN properly running

### Software Versions

| Software | Version | Purpose |
|----------|---------|---------|
| Java | JDK 8 or 11 | Hadoop runtime |
| Python | 3.6+ | MapReduce scripts |
| Hadoop | 3.x | Distributed processing |

---

## Setup Steps

### 1. Verify Cluster Health

**Check HDFS:**
```bash
hdfs dfsadmin -report
```

Expected output:
```
Live datanodes (2):
Name: worker1:9866
Name: worker2:9866
```

**Check YARN:**
```bash
yarn node -list
```

Expected output:
```
worker1:8042    RUNNING
worker2:8042    RUNNING
```

### 2. Install Dependencies on Master Node

**Required for download script only:**

```bash
# Python 3.12+ (Ubuntu/Debian)
pip3 install --user --break-system-packages pandas requests beautifulsoup4 gutenberg colorama

# Python 3.11 and earlier
pip3 install --user pandas requests beautifulsoup4 gutenberg colorama
```

**Verify:**
```bash
python3 -c "import pandas, requests; print('OK')"
```

### 3. Verify Python on Worker Nodes

**Workers only need Python 3 (usually pre-installed):**

```bash
ssh worker1 "python3 --version"
ssh worker2 "python3 --version"
```

Expected: `Python 3.x.x`

**That's it for workers!** No libraries needed - mapper uses built-in modules only.

### 4. Test Network Connectivity

**From master to workers:**
```bash
ssh worker1 "hostname"
ssh worker2 "hostname"
```

If SSH fails, set up passwordless SSH:
```bash
ssh-keygen -t rsa -b 4096 -N ""
ssh-copy-id user@worker1
ssh-copy-id user@worker2
```

### 5. Create HDFS Directories

```bash
# Create input directory
hdfs dfs -mkdir -p /gutenberg-input

# Set permissions
hdfs dfs -chmod 755 /gutenberg-input

# Verify
hdfs dfs -ls /
```

---

## Deployment Workflow

### Step 1: Upload Project Files to Master

```bash
# Upload to master node
scp -r big-data-assignment/ user@master:~/

# SSH to master
ssh user@master
cd ~/big-data-assignment
```

### Step 2: Make Scripts Executable

```bash
chmod +x run_inverted_index_mapreduce.sh
chmod +x inverted_index_mapper.py
chmod +x inverted_index_reducer.py
```

### Step 3: Download Data

```bash
python3 donwload_file.py
```

This uploads books to HDFS `/gutenberg-input`.

### Step 4: Run MapReduce Job

```bash
./run_inverted_index_mapreduce.sh 2
```

YARN automatically:
- Distributes mapper/reducer scripts to workers
- Schedules tasks based on data locality
- Collects results

**No manual distribution needed!**

---

## Monitoring

### During Job Execution

**Watch applications:**
```bash
watch -n 2 'yarn application -list'
```

**Get application details:**
```bash
yarn application -status application_XXX_YYYY
```

**View logs:**
```bash
yarn logs -applicationId application_XXX_YYYY
```

**Check resource usage:**
```bash
yarn top
```

### Check Node Health

**YARN nodes:**
```bash
yarn node -list -all
```

**HDFS health:**
```bash
hdfs dfsadmin -report
```

**Node status:**
```bash
yarn node -status worker1:8042
```

---

## Resource Allocation

### Example: 3-Node Cluster

**Setup:**
- 1 Master (4 CPU, 8GB RAM)
- 2 Workers (2 CPU, 4GB RAM each)

**Job Configuration:**
```
Total Mappers: 2 (one per input file)
Total Reducers: 2 (specified in command)

Worker 1 Resources:
├─ Container 1: Mapper (2GB)
└─ Container 2: Reducer (2GB)

Worker 2 Resources:
├─ Container 1: Mapper (2GB)
└─ Container 2: Reducer (2GB)
```

### Adjusting Resources

**Edit `run_inverted_index_mapreduce.sh` (lines ~243-244):**

```bash
# Default (2GB per task)
-D mapreduce.map.memory.mb=2048 \
-D mapreduce.reduce.memory.mb=2048 \

# For larger datasets (4GB per task)
-D mapreduce.map.memory.mb=4096 \
-D mapreduce.reduce.memory.mb=4096 \
```

### Optimal Reducer Count

| Cluster Size | Recommended Reducers |
|--------------|---------------------|
| 2 workers    | 2 reducers          |
| 4 workers    | 4 reducers          |
| 8 workers    | 8-12 reducers       |
| 16+ workers  | 16-24 reducers      |

**Run with custom reducers:**
```bash
./run_inverted_index_mapreduce.sh 8
```

---

## Verification

### Pre-Job Checklist

```bash
# ✓ HDFS healthy
hdfs dfsadmin -report

# ✓ YARN nodes running
yarn node -list

# ✓ Input data exists
hdfs dfs -ls /gutenberg-input
hdfs dfs -count /gutenberg-input

# ✓ Python 3 on workers
ssh worker1 "python3 --version"
ssh worker2 "python3 --version"

# ✓ Master has pandas
python3 -c "import pandas; print('OK')"
```

### Post-Job Verification

```bash
# ✓ Job completed
hdfs dfs -test -e /gutenberg-output/_SUCCESS && echo "SUCCESS"

# ✓ Output files created
hdfs dfs -ls /gutenberg-output

# ✓ Sample output
hdfs dfs -cat /gutenberg-output/part-* | head -20

# ✓ Word count
hdfs dfs -cat /gutenberg-output/part-* | wc -l
```

---

## Common Issues

### Issue: Worker node not responding

**Check:**
```bash
yarn node -list
ssh worker1  # Can you connect?
```

**Fix:**
```bash
# Restart NodeManager on worker
ssh worker1
sudo systemctl restart hadoop-yarn-nodemanager
```

### Issue: "Container killed by YARN"

**Cause:** Insufficient memory

**Fix:** Increase memory allocation (see Resource Allocation above)

### Issue: Tasks failing on specific worker

**Check logs:**
```bash
yarn logs -applicationId application_XXX_YYYY | grep worker1
```

**Fix:** Check worker disk space, Python version, permissions

### Issue: Data locality poor

**Check:**
```bash
hdfs dfs -stat %r /gutenberg-input/*.txt
```

**Fix:** Increase replication factor:
```bash
hdfs dfs -setrep 3 /gutenberg-input/*.txt
```

---

## Performance Tuning

### For Large Datasets (100+ books)

**1. Increase parallelism:**
```bash
./run_inverted_index_mapreduce.sh 16
```

**2. Enable compression:**

Edit `run_inverted_index_mapreduce.sh`, add:
```bash
-D mapreduce.output.fileoutputformat.compress=true \
-D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec \
```

**3. Optimize splits:**
```bash
-D mapreduce.input.fileinputformat.split.maxsize=134217728 \  # 128MB
```

### Monitoring Performance

**Job history:**
```bash
mapred job -history /gutenberg-output
```

**Task timing:**
```bash
yarn application -status application_XXX_YYYY | grep -A 20 "Task Completion Times"
```

---

## Security Considerations

### HDFS Permissions

```bash
# Set appropriate permissions
hdfs dfs -chmod 755 /gutenberg-input
hdfs dfs -chmod 755 /gutenberg-output

# Restrict to user
hdfs dfs -chown -R yourusername:hadoop /gutenberg-input
```

### YARN Configuration

**Queue management:**
```bash
# Check available queues
yarn queue -status default

# Submit to specific queue
# Edit run script, add: -D mapreduce.job.queuename=yourqueue
```

---

## Backup and Recovery

### Backup Results

```bash
# Download from HDFS
hadoop fs -get /gutenberg-output ./backup/

# Or use distcp for large datasets
hadoop distcp /gutenberg-output /backup/gutenberg-output
```

### Re-run Failed Jobs

```bash
# Clean failed output
hdfs dfs -rm -r /gutenberg-output

# Re-run
./run_inverted_index_mapreduce.sh 2
```

---

## Scaling Up

### Adding More Books

1. Edit `gutenberg_metadata_2books.csv`
2. Add book entries
3. Re-run download: `python3 donwload_file.py`
4. Increase reducers: `./run_inverted_index_mapreduce.sh 8`

### Adding More Worker Nodes

1. Install Hadoop on new node
2. Configure as DataNode + NodeManager
3. Start services
4. Verify: `yarn node -list`
5. Increase reducer count accordingly

---

## Quick Reference Commands

### Cluster Management
```bash
# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Start YARN
$HADOOP_HOME/sbin/start-yarn.sh

# Check all Java processes
jps
```

### Job Management
```bash
# List applications
yarn application -list

# Kill application
yarn application -kill application_XXX_YYYY

# View logs
yarn logs -applicationId application_XXX_YYYY
```

### HDFS Operations
```bash
# List files
hdfs dfs -ls /gutenberg-input

# Check file size
hdfs dfs -du -h /gutenberg-input

# Remove directory
hdfs dfs -rm -r /gutenberg-output

# Copy from HDFS
hadoop fs -get /gutenberg-output ./local-dir
```

---

## Summary

### Simplified Multi-Node Setup

1. ✅ **Master:** Install pandas/requests (for download only)
2. ✅ **Workers:** Nothing! Python 3 is enough
3. ✅ **Run:** Just execute the MapReduce script
4. ✅ **YARN handles everything:** Script distribution, scheduling, collection

**No complex dependency management across nodes!**

### Key Advantages

- **Simple deployment:** No library sync across workers
- **Fast execution:** No external library loading in mappers
- **Easy maintenance:** Update only master node for download changes
- **Reliable:** Built-in Python modules always available

---

**Ready for production!** 🚀

For troubleshooting, see `troubleshooting.md`
