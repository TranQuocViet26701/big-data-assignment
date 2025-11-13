# Troubleshooting: Sudo Password Issues

## The Error You're Seeing

```
[INFO] Downloading NLTK data...
sudo: a terminal is required to read the password; either use the -S option to read from standard input or configure an askpass helper
sudo: a password is required
[ERROR] Installation failed on hadoop-worker-1
```

## What This Means

The installation script tried to run `sudo` commands on `hadoop-worker-1`, but:
- SSH sessions don't have an interactive terminal for password prompts
- Your user (`ktdl9`) either:
  - Doesn't have passwordless sudo configured
  - Doesn't have sudo privileges at all

---

## ✅ SOLUTION: Use the No-Sudo Installation Script

I've created a new script that installs NLTK **in user space** without requiring sudo privileges!

### Step 1: Edit the Script

```bash
# Open the script
nano install_dependencies_no_sudo.sh

# Update the WORKER_NODES array with your actual worker hostnames:
WORKER_NODES=(
    "hadoop-worker-1"
    "hadoop-worker-2"
    # Add more workers as needed
)

# Save and exit (Ctrl+X, then Y, then Enter)
```

### Step 2: Run the No-Sudo Installation

```bash
# Make sure you're in the project directory
cd /path/to/big-data-assignment

# Run the installation script
./install_dependencies_no_sudo.sh
```

### Step 3: Verify Installation

The script will automatically verify that NLTK is working on all nodes.

### Step 4: Run Your MapReduce Job

```bash
./run_inverted_index_mapreduce.sh 2
```

---

## How the No-Sudo Solution Works

```
┌─────────────────────────────────────────────────────────────┐
│ Traditional Installation (requires sudo):                   │
│                                                              │
│ sudo pip3 install nltk                                      │
│ → Installs to: /usr/lib/python3/site-packages/            │
│ → Available to all users                                    │
│ → Requires root/sudo privileges                             │
│                                                              │
│ NLTK data at: /root/nltk_data                              │
│ → Requires sudo to write                                    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ User-Space Installation (NO sudo required):                 │
│                                                              │
│ pip3 install --user nltk                                    │
│ → Installs to: ~/.local/lib/python3/site-packages/         │
│ → Available only to your user (ktdl9)                       │
│ → No privileges required                                    │
│                                                              │
│ NLTK data at: ~/nltk_data                                  │
│ → Your home directory, no sudo needed                       │
└─────────────────────────────────────────────────────────────┘
```

**Important:** The mapper script has been updated to check both locations!

---

## Prerequisites for No-Sudo Installation

You only need:
1. ✓ SSH access to worker nodes
2. ✓ Python 3 installed (ask admin if missing)
3. ✓ pip3 available
4. ✗ NO sudo required!

---

## Alternative Solutions (If No-Sudo Doesn't Work)

### Option 1: Ask Admin to Install System-Wide

If you don't have Python 3 or pip3 installed, ask your admin to run this on all workers:

```bash
# Admin runs this on each worker node:
sudo yum install -y python3 python3-pip
sudo pip3 install nltk
sudo python3 << 'EOF'
import nltk
nltk.download('stopwords', download_dir='/root/nltk_data')
nltk.download('wordnet', download_dir='/root/nltk_data')
nltk.download('punkt', download_dir='/root/nltk_data')
nltk.download('omw-1.4', download_dir='/root/nltk_data')
EOF
```

### Option 2: Configure Passwordless Sudo (Advanced)

If you have sudo privileges but passwordless sudo is not configured:

```bash
# Ask your admin to add this to /etc/sudoers on all worker nodes:
# (Replace 'ktdl9' with your actual username)

ktdl9 ALL=(ALL) NOPASSWD: /usr/bin/pip3, /usr/bin/python3, /usr/bin/yum

# Then you can use the original install_dependencies_all_nodes.sh script
```

**Warning:** This is a security-sensitive change and requires admin approval.

### Option 3: Use Sudo with Password Prompt

If you have sudo access and want to enter your password:

```bash
# This requires entering password for EACH node
ssh hadoop-worker-1
sudo pip3 install nltk
python3 -c "
import nltk
nltk.download('stopwords', download_dir='/root/nltk_data')
nltk.download('wordnet', download_dir='/root/nltk_data')
nltk.download('punkt', download_dir='/root/nltk_data')
nltk.download('omw-1.4', download_dir='/root/nltk_data')
"
exit

# Repeat for worker-2, worker-3, etc.
```

---

## Comparison of Solutions

| Solution | Sudo Required? | Admin Help? | Complexity |
|----------|----------------|-------------|------------|
| **No-Sudo Script** (Recommended) | ❌ No | ❌ No | ⭐ Easy |
| Ask Admin to Install | ✅ Yes (admin) | ✅ Yes | ⭐ Easy (for you) |
| Passwordless Sudo | ✅ Yes | ✅ Yes (setup) | ⭐⭐ Medium |
| Manual SSH Installation | ✅ Yes | ❌ No | ⭐⭐⭐ Hard |

---

## Verifying Your Current Situation

Run these tests to understand what you have:

### Test 1: Do you have Python 3 on workers?

```bash
ssh hadoop-worker-1 "python3 --version"

# If successful → Python 3 is installed ✓
# If "command not found" → Ask admin to install Python 3
```

### Test 2: Do you have pip3 on workers?

```bash
ssh hadoop-worker-1 "pip3 --version"

# If successful → pip3 is available ✓
# If "command not found" → Ask admin to install pip3
```

### Test 3: Can you install without sudo?

```bash
ssh hadoop-worker-1 "pip3 install --user --upgrade pip"

# If successful → You can use no-sudo installation ✓
# If failed → Check error message
```

### Test 4: Do you have sudo access?

```bash
ssh hadoop-worker-1 "sudo -n true 2>&1"

# If no output → You have passwordless sudo ✓
# If "password is required" → You have sudo but need password
# If "not in sudoers" → You don't have sudo access
```

---

## What Changed in the Mapper Script

The `inverted_index_mapper.py` has been updated to support both installation types:

**Before:**
```python
import nltk
nltk.data.path.append("/root/nltk_data")  # Only system-wide
```

**After:**
```python
import nltk
import os
# Support both system-wide and user-space NLTK data
nltk.data.path.append("/root/nltk_data")  # System-wide installation
nltk.data.path.append(os.path.expanduser("~/nltk_data"))  # User-space installation
```

This means:
- If admin installed system-wide → Uses `/root/nltk_data`
- If you installed user-space → Uses `~/nltk_data`
- Both work automatically!

---

## Step-by-Step: Using No-Sudo Installation

```bash
# 1. Navigate to project directory
cd ~/big-data-assignment

# 2. Edit worker nodes list
nano install_dependencies_no_sudo.sh
# Update WORKER_NODES=("hadoop-worker-1" "hadoop-worker-2" ...)

# 3. Run installation
./install_dependencies_no_sudo.sh

# Expected output:
# ========================================
# User-Space Installation (No Sudo Required)
# ========================================
# [INFO] Current user: ktdl9
# [INFO] NLTK data will be installed in: /home/ktdl9/nltk_data
# ...
# [SUCCESS] All nodes ready for MapReduce job

# 4. Run your MapReduce job
./run_inverted_index_mapreduce.sh 2

# 5. Monitor progress
yarn application -list

# 6. View results
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

---

## Common Issues with No-Sudo Installation

### Issue: "pip3: command not found"

**Solution:** Ask admin to install pip3:
```bash
sudo yum install -y python3-pip  # CentOS/RHEL
# or
sudo apt-get install -y python3-pip  # Ubuntu
```

### Issue: "Python 3 not found"

**Solution:** Ask admin to install Python 3:
```bash
sudo yum install -y python3  # CentOS/RHEL
# or
sudo apt-get install -y python3  # Ubuntu
```

### Issue: "Permission denied" when writing to ~/nltk_data

**Solution:** Check home directory permissions:
```bash
ssh hadoop-worker-1 "ls -la ~ | grep nltk_data"
ssh hadoop-worker-1 "mkdir -p ~/nltk_data && echo 'OK'"
```

### Issue: MapReduce job still fails with "Resource stopwords not found"

**Solution:** Verify NLTK data was downloaded:
```bash
ssh hadoop-worker-1 "ls -la ~/nltk_data/corpora/"

# Should show:
# drwxr-xr-x stopwords
# drwxr-xr-x wordnet
```

---

## Quick Decision Tree

```
START: Can you SSH to workers?
  │
  ├─ NO → Set up SSH first (see SSH_SETUP_GUIDE.md)
  │
  └─ YES → Continue
      │
      ├─ Do workers have Python 3 + pip3?
      │  │
      │  ├─ YES → Use install_dependencies_no_sudo.sh ✓
      │  │
      │  └─ NO → Ask admin to install Python 3 + pip3
      │           Then use install_dependencies_no_sudo.sh ✓
      │
      └─ Do you have sudo access?
          │
          ├─ YES (passwordless) → Can use install_dependencies_all_nodes.sh
          │
          ├─ YES (with password) → Use manual installation or no-sudo script
          │
          └─ NO → Use install_dependencies_no_sudo.sh ✓
```

---

## Summary

**The Problem:** Original script needed sudo, but your SSH session can't prompt for passwords.

**The Solution:** Use `install_dependencies_no_sudo.sh` which:
- ✓ Installs NLTK in your home directory
- ✓ No sudo required
- ✓ Works with SSH
- ✓ Mapper script updated to find user-space NLTK data

**Next Step:**
```bash
./install_dependencies_no_sudo.sh
```

Then run your job:
```bash
./run_inverted_index_mapreduce.sh 2
```

---

## Need More Help?

1. Check if Python 3 is installed: `ssh hadoop-worker-1 "python3 --version"`
2. Check if pip3 is available: `ssh hadoop-worker-1 "pip3 --version"`
3. Try the no-sudo installation: `./install_dependencies_no_sudo.sh`
4. If still stuck, ask admin to install Python 3 + pip3 on all workers

The no-sudo approach is the recommended solution for users without sudo privileges!
