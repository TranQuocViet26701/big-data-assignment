# Python 3.12 "externally-managed-environment" - QUICK FIX

## Your Error:

```
error: externally-managed-environment
× This environment is externally managed
```

---

## ✅ SOLUTION (30 seconds):

### Step 1: Edit worker nodes

```bash
nano install_all_dependencies_ubuntu.sh
```

Update line ~19:
```bash
WORKER_NODES=(
    "hadoop-worker-1"
    "hadoop-worker-2"
    # Add your actual worker hostnames
)
```

Save and exit (Ctrl+X → Y → Enter)

### Step 2: Run installation

```bash
./install_all_dependencies_ubuntu.sh
```

### Step 3: Done!

```bash
# Download books
python3 donwload_file.py

# Run MapReduce
./run_inverted_index_mapreduce.sh 2

# View results
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

---

## What This Script Does:

1. ✅ Automatically adds `--break-system-packages` flag (fixes Python 3.12 error)
2. ✅ Installs **all libraries** on master (nltk, pandas, numpy, requests, etc.)
3. ✅ Installs **only NLTK** on workers (that's all MapReduce needs!)
4. ✅ Downloads NLTK data on all nodes
5. ✅ Verifies everything works
6. ✅ **No sudo required!**

---

## Alternative: Auto-Detecting Script

If you want one script that works on both old and new Python versions:

```bash
./install_all_dependencies.sh
```

This script automatically:
- Detects Python version
- Detects OS (Ubuntu vs CentOS)
- Adds `--break-system-packages` only if needed (Python 3.12+ on Debian/Ubuntu)
- Works on all systems!

---

## Manual One-Liner (If Scripts Don't Work):

```bash
# Master node
pip3 install --user --break-system-packages nltk pandas numpy requests beautifulsoup4 gutenberg colorama

# Each worker
ssh hadoop-worker-1 "pip3 install --user --break-system-packages nltk"
ssh hadoop-worker-2 "pip3 install --user --break-system-packages nltk"
```

---

## For More Details:

| Document | What It Covers |
|----------|----------------|
| **UBUNTU_PYTHON312_FIX.md** | Complete guide with 4 solution options, FAQ, troubleshooting |
| **TROUBLESHOOTING_SUDO.md** | All installation errors (PEP 668, sudo issues, etc.) |
| **PYTHON_DEPENDENCIES_GUIDE.md** | How to manage dependencies, add new libraries |
| **README_INSTALLATION.md** | Quick reference for installation |

---

## Why This Happens:

- Ubuntu 24.04+ / Debian 13+ use **Python 3.12**
- Python 3.12 implements **PEP 668** (security feature)
- Prevents pip from conflicting with system packages
- Solution: Use `--break-system-packages` flag (safe for Hadoop workers)

---

## Is --break-system-packages Safe?

**Yes, for Hadoop worker nodes!**

- Installs to `~/.local/` (your home directory), not system directories
- Doesn't affect system Python packages
- Worker nodes don't run critical Python services
- Hadoop uses Java, not Python, for core operations

See UBUNTU_PYTHON312_FIX.md for full safety discussion.

---

## Summary

**Problem:** Python 3.12 on Ubuntu blocks pip installations

**Solution:** Use `--break-system-packages` flag

**How:** Run `./install_all_dependencies_ubuntu.sh`

**Time:** 2-5 minutes for complete installation

**Result:** Ready to run MapReduce jobs!

---

**Need help?** See the comprehensive guides listed above or ask!
