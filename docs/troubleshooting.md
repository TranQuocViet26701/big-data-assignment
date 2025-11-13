# Troubleshooting Guide

Common issues and solutions for the Inverted Index MapReduce project.

---

## Quick Diagnostics

### Check Everything is Running

```bash
# HDFS
hdfs dfsadmin -report

# YARN
yarn node -list

# Application
yarn application -list

# Logs
yarn logs -applicationId application_XXX_YYYY
```

---

## Common Issues

### 1. "No module named 'pandas'" (Master Node)

**Error:**
```
ModuleNotFoundError: No module named 'pandas'
```

**Cause:** Python dependencies not installed on master

**Solution:**
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

---

### 2. "Input directory does not exist"

**Error:**
```
[ERROR] Input directory does not exist: /gutenberg-input
```

**Cause:** HDFS input directory not created

**Solution:**
```bash
# Create directory
hdfs dfs -mkdir -p /gutenberg-input

# Verify
hdfs dfs -ls /

# Upload data
python3 donwload_file.py
```

---

### 3. "Output directory already exists"

**Error:**
```
Output directory hdfs://namenode:9000/gutenberg-output already exists
```

**Cause:** Previous job output not cleaned

**Solution:**
```bash
# Remove old output
hdfs dfs -rm -r /gutenberg-output

# Re-run job
./run_inverted_index_mapreduce.sh 2
```

---

### 4. MapReduce Job Fails Immediately

**Symptoms:**
- Job fails within seconds
- All map tasks fail

**Check logs:**
```bash
# Get application ID
yarn application -list | grep Inverted_Index

# View logs
yarn logs -applicationId application_XXX_YYYY | head -100
```

**Common causes:**

#### 4a. Mapper Script Error

**Solution:**
```bash
# Test mapper locally
echo "sample text for testing" | python3 inverted_index_mapper.py

# Should output: word → filename@count format
```

#### 4b. Python Not Found on Workers

**Check:**
```bash
ssh worker1 "python3 --version"
ssh worker2 "python3 --version"
```

**Solution:**
```bash
# Install Python 3 on workers
ssh worker1 "sudo apt install python3"  # Ubuntu/Debian
ssh worker1 "sudo yum install python3"  # CentOS/RHEL
```

#### 4c. Permission Issues

**Check:**
```bash
hdfs dfs -ls /gutenberg-input
```

**Solution:**
```bash
# Fix permissions
hdfs dfs -chmod 755 /gutenberg-input
hdfs dfs -chmod 644 /gutenberg-input/*
```

---

### 5. "Container killed by YARN"

**Error:**
```
Container [container_XXX] is running beyond physical memory limits
Current usage: 2.1 GB of 2 GB physical memory used
```

**Cause:** Insufficient memory allocated

**Solution:**

Edit `run_inverted_index_mapreduce.sh` (lines ~243-244):

```bash
# Increase memory limits
-D mapreduce.map.memory.mb=4096 \
-D mapreduce.reduce.memory.mb=4096 \
```

**Or reduce dataset size:**
```bash
# Process fewer books
# Edit gutenberg_metadata_2books.csv, remove some entries
```

---

### 6. Job Hangs / Very Slow

**Symptoms:**
- Map phase stuck at 99%
- Reducers not starting
- Tasks timing out

**Check resource availability:**
```bash
yarn top
```

**Possible causes:**

#### 6a. Worker Node Down

**Check:**
```bash
yarn node -list
```

**Solution:**
```bash
# Restart NodeManager on affected worker
ssh worker1
sudo systemctl restart hadoop-yarn-nodemanager
```

#### 6b. Data Skew

**Check task distribution:**
```bash
yarn application -status application_XXX_YYYY
```

**Solution:**
- Increase number of reducers
- Check input data is evenly distributed

#### 6c. Network Issues

**Check:**
```bash
# From master to workers
ping worker1
ping worker2
```

---

### 7. "Unrecognized option: -D"

**Error:**
```
ERROR streaming.StreamJob: Unrecognized option: -D
```

**Cause:** Generic options (`-D`) placed after streaming options

**Already fixed in current script!** If you see this, verify script is up-to-date.

**Manual fix:** Move all `-D` options before `-input` in hadoop command

---

### 8. HDFS Issues

#### 8a. "Cannot connect to HDFS"

**Check:**
```bash
# HDFS running?
jps | grep NameNode

# HDFS accessible?
hdfs dfs -ls /
```

**Solution:**
```bash
# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# Check status
hdfs dfsadmin -report
```

#### 8b. "No space left on device"

**Check:**
```bash
hdfs dfs -df -h
```

**Solution:**
```bash
# Clean old data
hdfs dfs -rm -r /tmp/*
hdfs dfs -rm -r /old-jobs/*

# Or increase HDFS capacity (add disks)
```

#### 8c. "Datanodes unavailable"

**Check:**
```bash
hdfs dfsadmin -report
```

**Solution:**
```bash
# Restart DataNodes on workers
ssh worker1 "sudo systemctl restart hadoop-hdfs-datanode"
ssh worker2 "sudo systemctl restart hadoop-hdfs-datanode"
```

---

### 9. YARN Issues

#### 9a. "ResourceManager not running"

**Check:**
```bash
jps | grep ResourceManager
```

**Solution:**
```bash
$HADOOP_HOME/sbin/start-yarn.sh
```

#### 9b. "No NodeManagers available"

**Check:**
```bash
yarn node -list
```

**Solution:**
```bash
# Start NodeManagers on workers
ssh worker1 "sudo systemctl start hadoop-yarn-nodemanager"
ssh worker2 "sudo systemctl start hadoop-yarn-nodemanager"
```

#### 9c. "Application rejected by queue"

**Check:**
```bash
yarn queue -status default
```

**Solution:**
- Reduce resource requirements
- Use different queue
- Wait for resources to free up

---

### 10. Download Script Issues

#### 10a. "Connection timeout" downloading books

**Cause:** Network issues or Gutenberg servers down

**Solution:**
```bash
# Retry
python3 donwload_file.py

# Or use different mirror
# Edit donwload_file.py, line 90: change mirror URL
```

#### 10b. "Book not found"

**Cause:** Invalid book ID in CSV

**Solution:**
```bash
# Verify book exists
curl -I http://www.gutenberg.org/ebooks/17748

# Or update CSV with valid book ID
```

---

## Debugging Techniques

### View Detailed Logs

**Application logs:**
```bash
yarn logs -applicationId application_XXX_YYYY > app.log
less app.log
```

**Specific container logs:**
```bash
yarn logs -applicationId application_XXX_YYYY -containerId container_XXX_YYY
```

**Filter for errors:**
```bash
yarn logs -applicationId application_XXX_YYYY | grep -i error
yarn logs -applicationId application_XXX_YYYY | grep -i exception
```

### Test Components Locally

**Test mapper:**
```bash
cat /path/to/sample.txt | python3 inverted_index_mapper.py
```

**Test reducer:**
```bash
# Create sample mapper output
echo -e "word1\tfile1@100\nword1\tfile2@200" | python3 inverted_index_reducer.py
```

**Test download script:**
```bash
python3 donwload_file.py
```

### Monitor Resources

**Real-time monitoring:**
```bash
# YARN resources
watch -n 2 'yarn node -list'

# HDFS usage
watch -n 5 'hdfs dfs -df -h'

# Running applications
watch -n 2 'yarn application -list'
```

**System resources on workers:**
```bash
ssh worker1 "top -b -n 1 | head -20"
ssh worker1 "df -h"
ssh worker1 "free -h"
```

---

## Performance Issues

### Job Taking Too Long

**Optimize reducers:**
```bash
# More reducers = more parallelism
./run_inverted_index_mapreduce.sh 8
```

**Increase resources:**
```bash
# Edit script, increase memory (lines ~243-244)
-D mapreduce.map.memory.mb=4096
```

**Enable compression:**
```bash
# Add to hadoop command in script
-D mapreduce.output.fileoutputformat.compress=true
```

### High Memory Usage

**Reduce per-task memory:**
```bash
# Edit script
-D mapreduce.map.memory.mb=1024
-D mapreduce.reduce.memory.mb=1024
```

**Process fewer files:**
```bash
# Split input into batches
# Process 10 books at a time instead of 100
```

---

## Getting Help

### Log Files to Check

1. **Application logs:** `yarn logs -applicationId application_XXX_YYYY`
2. **HDFS logs:** `$HADOOP_HOME/logs/hadoop-*-namenode-*.log`
3. **YARN logs:** `$HADOOP_HOME/logs/yarn-*-resourcemanager-*.log`
4. **System logs:** `/var/log/syslog` or `/var/log/messages`

### Information to Gather

When reporting issues:
- Hadoop version: `hadoop version`
- YARN status: `yarn node -list`
- HDFS status: `hdfs dfsadmin -report`
- Application ID and logs
- Error messages (complete stack traces)
- Cluster size and configuration

### Useful Commands

```bash
# Full cluster health check
hdfs dfsadmin -report
yarn node -list
jps  # On each node
df -h  # Disk space
free -h  # Memory

# Application details
yarn application -list
yarn application -status application_XXX_YYYY
yarn logs -applicationId application_XXX_YYYY

# HDFS operations
hdfs dfs -ls /
hdfs dfs -du -h /gutenberg-input
hdfs dfs -df -h
```

---

## Prevention Best Practices

### Before Running Job

- ✅ Check HDFS: `hdfs dfsadmin -report`
- ✅ Check YARN: `yarn node -list`
- ✅ Verify input exists: `hdfs dfs -ls /gutenberg-input`
- ✅ Clean old output: `hdfs dfs -rm -r /gutenberg-output`
- ✅ Test scripts locally first

### Regular Maintenance

- Clean old job outputs regularly
- Monitor disk space on HDFS
- Keep worker nodes synchronized
- Update Hadoop regularly
- Backup important results

---

## Still Stuck?

1. **Check logs thoroughly:** Most issues have clear error messages in logs
2. **Search error message:** Google the exact error + "Hadoop MapReduce"
3. **Hadoop documentation:** https://hadoop.apache.org/docs/stable/
4. **StackOverflow:** Tag questions with `hadoop`, `mapreduce`, `yarn`

---

**Most issues are configuration or resource-related. Logs are your friend!** 🔍
