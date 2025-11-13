# Quick Start Guide for ktdl9 User

## Direct Answer to Your SSH Question

**Q: From the master node, will it SSH to other workers using the ktdl9 user? Should I care about it?**

**A: NO, the MapReduce job script does NOT SSH to worker nodes. YARN handles everything automatically via its own protocol.**

---

## Three Scenarios for You

### Scenario 1: Dependencies Already Installed (Best Case)
```
✓ Python 3 + NLTK already on all workers (installed by admin)
✗ SSH NOT required
✓ Just run the job!

[ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2
```

### Scenario 2: You Need to Install Dependencies + You Have SSH + Sudo
```
✓ You have SSH access to workers as ktdl9
✓ You have sudo privileges on workers
✓ Use the automated installation script

Step 1: Edit install_dependencies_all_nodes.sh
        (Change WORKER_NODES array to your worker hostnames)

Step 2: Run installation
        [ktdl9@master]$ ./install_dependencies_all_nodes.sh

Step 3: Run the job
        [ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2
```

### Scenario 3: You Don't Have SSH/Sudo Access
```
✗ Cannot SSH to workers OR no sudo privileges
✓ Ask system administrator to install dependencies
✓ Then run the job (no SSH needed)

Email your admin with these instructions:
---------------------------------------
Please install the following on all worker nodes:

sudo yum install -y python3 python3-pip
sudo pip3 install nltk
sudo python3 -c "
import nltk
nltk.download('stopwords', download_dir='/root/nltk_data')
nltk.download('wordnet', download_dir='/root/nltk_data')
nltk.download('punkt', download_dir='/root/nltk_data')
nltk.download('omw-1.4', download_dir='/root/nltk_data')
"

After installation, run:
[ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2
```

---

## Quick Test: What's Your Situation?

Run these commands to find out:

```bash
# Test 1: Can you SSH to workers?
ssh worker1 "hostname"

# If successful → You have SSH access
# If "Permission denied" → You DON'T have SSH access (use Scenario 3)

# Test 2: Are dependencies already installed?
ssh worker1 "python3 -c 'import nltk; print(\"NLTK OK\")'"

# If you see "NLTK OK" → Dependencies installed (use Scenario 1)
# If you see "ModuleNotFoundError" → Need to install (use Scenario 2 or 3)
# If you can't SSH → Use Scenario 3

# Test 3: Can you submit YARN jobs?
yarn application -list

# If successful → You can run MapReduce jobs
# If failed → You need YARN permissions from admin
```

---

## How The Job Actually Runs (Without SSH)

```
Step 1: You run script on master node as ktdl9
        [ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2

Step 2: Script submits job to YARN ResourceManager
        (No SSH involved - uses YARN RPC protocol)

Step 3: YARN ResourceManager creates ApplicationMaster
        (No SSH involved - YARN internal communication)

Step 4: YARN allocates containers on worker nodes
        (No SSH involved - NodeManager handles this)

Step 5: Containers execute mapper.py and reducer.py
        (Scripts run locally on workers, no SSH)
        (Python/NLTK must be installed on workers!)

Step 6: Results written to HDFS
        (No SSH involved - HDFS protocol)

Step 7: You view results from master node
        [ktdl9@master]$ hdfs dfs -cat /gutenberg-output/part-*
        (No SSH involved - HDFS client)
```

**Conclusion: SSH is NEVER used by the job execution itself!**

---

## When SSH IS Used (Optional)

SSH is ONLY used for these manual administrative tasks:

1. **Installing dependencies** (if you do it yourself)
   ```bash
   ssh worker1 "sudo pip3 install nltk"
   ```

2. **Checking worker status** (for debugging)
   ```bash
   ssh worker1 "python3 --version"
   ```

3. **Viewing logs** (for troubleshooting)
   ```bash
   ssh worker1 "tail -f /var/log/hadoop-yarn/..."
   ```

But the MapReduce job itself? **No SSH required!**

---

## Recommended Workflow for ktdl9

### Option A: If Admin Can Help (Easiest)

```bash
1. Ask admin to install Python + NLTK on all workers
2. Wait for confirmation
3. Run: ./run_inverted_index_mapreduce.sh 2
4. Done!
```

### Option B: If You Have SSH + Sudo (Self-Service)

```bash
1. Edit install_dependencies_all_nodes.sh
   (Add your worker hostnames)
2. Run: ./install_dependencies_all_nodes.sh
3. Wait for installation to complete
4. Run: ./run_inverted_index_mapreduce.sh 2
5. Done!
```

### Option C: Dependencies Already There (Fastest)

```bash
1. Run: ./run_inverted_index_mapreduce.sh 2
2. If it works → Great!
3. If it fails with "ModuleNotFoundError: nltk" → Use Option A or B
```

---

## SSH Setup (Only If Needed for Option B)

If you choose Option B and don't have passwordless SSH:

```bash
# On master node as ktdl9:

# 1. Generate SSH key (if you don't have one)
ssh-keygen -t rsa -b 4096 -N ""

# 2. Copy to each worker (enter password when prompted)
ssh-copy-id ktdl9@worker1
ssh-copy-id ktdl9@worker2
ssh-copy-id ktdl9@worker3

# 3. Test passwordless access
ssh worker1 "hostname"
# Should work without password!

# 4. Now run the installation script
./install_dependencies_all_nodes.sh
```

---

## Files You Have

1. **run_inverted_index_mapreduce.sh**
   - Runs the MapReduce job
   - Execute on master node only
   - Does NOT SSH to workers

2. **install_dependencies_all_nodes.sh**
   - Installs Python + NLTK on all workers via SSH
   - Only needed if you're doing the installation yourself
   - Requires SSH access + sudo privileges

3. **SSH_SETUP_GUIDE.md**
   - Detailed guide on SSH setup
   - Read if you need passwordless SSH

4. **MULTI_NODE_DEPLOYMENT_GUIDE.md**
   - Comprehensive deployment guide
   - Full details on everything

5. **EXECUTION_FLOW_OVERVIEW.md**
   - Visual diagrams of the pipeline
   - See how data flows through the system

---

## Final Answer to Your Question

**"Should I care about SSH for the ktdl9 user?"**

**It depends:**

- **For RUNNING the MapReduce job:** NO, don't care about SSH. YARN handles it.
- **For INSTALLING dependencies:** YES, you need SSH if you're doing it yourself.
- **If admin installs dependencies:** NO, you don't need SSH at all.

**Recommendation:** Try running the job first. If it fails with NLTK errors, then worry about SSH for installation.

---

## Quick Commands Reference

```bash
# Check if dependencies installed (requires SSH)
ssh worker1 "python3 -c 'import nltk; print(\"OK\")'"

# Run the MapReduce job (NO SSH required)
./run_inverted_index_mapreduce.sh 2

# Monitor job (NO SSH required)
yarn application -list

# View results (NO SSH required)
hdfs dfs -cat /gutenberg-output/part-* | head -20

# Install dependencies on all workers (requires SSH + sudo)
./install_dependencies_all_nodes.sh
```

---

## Summary

The `run_inverted_index_mapreduce.sh` script:
- ✗ Does NOT use SSH
- ✓ Runs only on master node
- ✓ Submits job to YARN
- ✓ YARN distributes work automatically
- ✓ You view results from master node

SSH is only needed for manual installation of dependencies. Once dependencies are installed (by you or admin), you never need SSH for running jobs.

**Start here:**
```bash
[ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2
```

If it works → You're done!
If it fails → Check the error, then decide if you need to install dependencies.
