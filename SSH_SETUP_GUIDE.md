# SSH Setup Guide for Multi-Node Hadoop Cluster

## Understanding SSH in Hadoop Context

### ⚠️ CRITICAL CLARIFICATION

```
┌─────────────────────────────────────────────────────────────────┐
│ SSH is NOT used by YARN/Hadoop for MapReduce job execution!    │
│                                                                  │
│ YARN uses its own RPC protocol to communicate with workers.    │
│ When you run the MapReduce job, YARN automatically:            │
│   - Distributes your scripts to worker nodes                    │
│   - Launches containers via NodeManager                         │
│   - Collects results                                            │
│                                                                  │
│ You do NOT need to configure SSH for the job to run!          │
└─────────────────────────────────────────────────────────────────┘
```

### When DO You Need SSH?

```
┌───────────────────────────────────────────────────────────────┐
│ SSH is ONLY needed for:                                       │
│                                                                │
│ 1. Manual Administration Tasks                                │
│    - Installing Python/NLTK on worker nodes                   │
│    - Checking if dependencies are installed                   │
│    - Debugging issues on worker nodes                         │
│    - Viewing logs on worker nodes                             │
│                                                                │
│ 2. Hadoop Cluster Management (if you're the admin)           │
│    - Starting/stopping Hadoop daemons                         │
│    - Initial cluster setup                                    │
│    - Configuration changes                                    │
│                                                                │
│ 3. Using Helper Scripts (optional convenience)                │
│    - Automated dependency installation scripts                │
│    - Verification scripts that check all nodes                │
└───────────────────────────────────────────────────────────────┘
```

---

## Your Specific Case: ktdl9 User

### Scenario Analysis

You mentioned using the `ktdl9` user. Here's what you need to know:

```
┌─────────────────────────────────────────────────────────────────┐
│ SCENARIO 1: You ONLY want to run the MapReduce job             │
│                                                                  │
│ What you need:                                                  │
│   ✓ Run from master node as ktdl9 user                         │
│   ✓ HDFS access permissions                                    │
│   ✓ YARN access permissions                                    │
│   ✗ SSH to worker nodes NOT required                           │
│                                                                  │
│ The job will work WITHOUT SSH setup!                           │
│                                                                  │
│ Command:                                                        │
│   [ktdl9@master]$ ./run_inverted_index_mapreduce.sh           │
│                                                                  │
│ BUT: Dependencies (Python, NLTK) must already be installed     │
│      on all worker nodes by an administrator                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ SCENARIO 2: You need to install dependencies on workers        │
│                                                                  │
│ What you need:                                                  │
│   ✓ SSH access from master to all workers as ktdl9            │
│   ✓ ktdl9 user must have sudo privileges on workers           │
│   OR                                                            │
│   ✓ Ask system administrator to install dependencies          │
│                                                                  │
│ Options:                                                        │
│   A) Set up passwordless SSH (see below)                       │
│   B) Ask administrator to install for you                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Decision Tree: Do You Need SSH Setup?

```
START: Do you need to install Python/NLTK on worker nodes?
  │
  ├─ NO → Worker nodes already have Python 3 + NLTK installed
  │        └─> You DON'T need SSH setup
  │            └─> Just run: ./run_inverted_index_mapreduce.sh
  │
  └─ YES → You need to install dependencies
           │
           ├─ Can you ask system admin to install?
           │  └─ YES → Ask admin to install Python + NLTK on all workers
           │           └─> You DON'T need SSH setup
           │
           └─ NO → You must install dependencies yourself
                   │
                   ├─ Do you have sudo privileges on workers?
                   │  │
                   │  ├─ YES → Set up passwordless SSH (see below)
                   │  │        └─> Use automated installation scripts
                   │  │
                   │  └─ NO → You CANNOT install dependencies
                   │           └─> Must ask administrator for help
                   │
                   └─ Do you have SSH access from master to workers?
                      │
                      ├─ YES → Great! Proceed with setup below
                      │
                      └─ NO → Need to set up SSH keys (see below)
```

---

## SSH Setup for ktdl9 User (If Needed)

### Step 1: Check Current SSH Access

```bash
# Test if you can already SSH to workers as ktdl9
ssh ktdl9@worker1 "hostname"
ssh ktdl9@worker2 "hostname"

# If this works without asking for password, you're already set up!
# If it asks for password, continue to Step 2
```

### Step 2: Generate SSH Keys (If Not Already Done)

```bash
# On master node as ktdl9 user
cd ~
ls -la ~/.ssh/id_rsa

# If id_rsa doesn't exist, generate keys:
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

# This creates:
#   ~/.ssh/id_rsa       (private key - keep secret!)
#   ~/.ssh/id_rsa.pub   (public key - can share)
```

### Step 3: Copy Public Key to Worker Nodes

#### Option A: Using ssh-copy-id (Easiest)

```bash
# For each worker node (you'll need to enter password once)
ssh-copy-id ktdl9@worker1
ssh-copy-id ktdl9@worker2
ssh-copy-id ktdl9@worker3
# ... repeat for all workers
```

#### Option B: Manual Copy (If ssh-copy-id not available)

```bash
# Copy public key to each worker
cat ~/.ssh/id_rsa.pub | ssh ktdl9@worker1 "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys"
```

#### Option C: Ask Administrator

If you don't have the password, ask your system administrator to:
1. Add your public key (~/.ssh/id_rsa.pub) to /home/ktdl9/.ssh/authorized_keys on all worker nodes

```bash
# Give this file to admin:
cat ~/.ssh/id_rsa.pub

# Admin should run on each worker:
sudo su - ktdl9
mkdir -p ~/.ssh
echo "YOUR_PUBLIC_KEY_HERE" >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
exit
```

### Step 4: Verify Passwordless SSH

```bash
# Test without password
ssh ktdl9@worker1 "hostname && python3 --version"
ssh ktdl9@worker2 "hostname && python3 --version"

# If successful, you see output without password prompt!
```

---

## Alternative: Let Administrator Handle Dependencies

### Best Practice for Production Environments

```
┌─────────────────────────────────────────────────────────────────┐
│ RECOMMENDED APPROACH                                            │
│                                                                  │
│ In production/shared clusters, system administrators typically │
│ handle software installation. You should:                       │
│                                                                  │
│ 1. Request from your admin:                                    │
│    "Please install Python 3 and NLTK on all worker nodes:      │
│     - sudo yum install -y python3 python3-pip                  │
│     - sudo pip3 install nltk                                   │
│     - Download NLTK data to /root/nltk_data"                   │
│                                                                  │
│ 2. Provide them with this command to run on each worker:       │
│                                                                  │
│    sudo yum install -y python3 python3-pip                     │
│    sudo pip3 install nltk                                      │
│    sudo python3 -c "                                           │
│    import nltk                                                  │
│    nltk.download('stopwords', download_dir='/root/nltk_data')│
│    nltk.download('wordnet', download_dir='/root/nltk_data')  │
│    nltk.download('punkt', download_dir='/root/nltk_data')    │
│    nltk.download('omw-1.4', download_dir='/root/nltk_data')  │
│    "                                                           │
│                                                                  │
│ 3. Then you can run the job without needing SSH access!       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Running the Job Without SSH Access

### The Simple Approach

```bash
# Assume dependencies are already installed on all workers
# (by admin or previous setup)

# Step 1: Upload data to HDFS
[ktdl9@master]$ python3 donwload_file.py

# Step 2: Run the MapReduce job
[ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2

# That's it! YARN handles everything automatically.
# No SSH needed!
```

### What Happens Behind the Scenes

```
Master Node (ktdl9 user)
  |
  | 1. Submit job to YARN ResourceManager
  ↓
YARN ResourceManager
  |
  | 2. Create ApplicationMaster
  | 3. Request containers from NodeManagers
  ↓
Worker Nodes (Containers run as yarn user, not ktdl9!)
  |
  | 4. NodeManager launches containers
  | 5. Containers execute mapper.py and reducer.py
  | 6. Python processes need NLTK (must be installed!)
  ↓
Results written to HDFS
  |
  | 7. Output visible to ktdl9 via HDFS permissions
  ↓
Done!
```

**Important Note:** The actual map/reduce tasks run as the YARN user (typically `yarn` or `hadoop`), not as `ktdl9`. This is why dependencies must be system-wide (installed with sudo).

---

## Summary & Recommendations

### For Your ktdl9 User Setup

```
┌─────────────────────────────────────────────────────────────────┐
│ RECOMMENDED WORKFLOW                                            │
│                                                                  │
│ 1. Check if dependencies are already installed:                │
│    [ktdl9@master]$ ssh worker1 "python3 --version"            │
│    [ktdl9@master]$ ssh worker2 "python3 -c 'import nltk'"     │
│                                                                  │
│    If YES: Skip to step 4                                      │
│    If NO: Continue to step 2                                   │
│                                                                  │
│ 2. If you have SSH access and sudo:                           │
│    - Set up passwordless SSH (if needed)                       │
│    - Use the automated installation script (see below)         │
│                                                                  │
│ 3. If you DON'T have SSH/sudo access:                         │
│    - Send installation instructions to your admin              │
│    - Wait for admin to install dependencies                    │
│                                                                  │
│ 4. Run the job:                                                │
│    [ktdl9@master]$ ./run_inverted_index_mapreduce.sh 2        │
│                                                                  │
│ 5. Monitor and view results (no SSH needed):                  │
│    [ktdl9@master]$ yarn application -list                     │
│    [ktdl9@master]$ hdfs dfs -cat /gutenberg-output/part-*     │
└─────────────────────────────────────────────────────────────────┘
```

### Quick Test: Do You Have What You Need?

```bash
# Run this test from master node as ktdl9:

# Test 1: Can you submit YARN jobs?
yarn application -list
# If successful: ✓ You can run MapReduce jobs

# Test 2: Are dependencies installed on workers?
# Try running the job - if it fails with "ModuleNotFoundError: nltk"
# then dependencies need to be installed

# Test 3: Can you SSH to workers? (only needed for manual installs)
ssh worker1 "hostname"
# If successful: ✓ You can install dependencies yourself
# If failed: ✗ Need admin help
```

---

## Automated Installation Script (If You Have SSH + Sudo)

If you have SSH access and sudo privileges, I'll create an automated script in the next step.

---

## Conclusion

**Short Answer:** You DON'T need SSH for running the MapReduce job itself. YARN handles all inter-node communication.

**You ONLY need SSH if:**
- You need to manually install Python/NLTK on worker nodes
- You want to check worker node status manually
- You want to use automated installation scripts

**Recommended approach:**
1. Check if dependencies are already installed
2. If not, ask your admin to install them
3. Run the job from master node - no SSH configuration needed!

The `./run_inverted_index_mapreduce.sh` script runs ONLY on the master node and doesn't SSH to workers.
