# Python Dependencies Management Guide

## Overview

Your Inverted Index MapReduce project uses different Python libraries on different nodes:

```
┌──────────────────────────────────────────────────────────────┐
│ MASTER NODE (where you run scripts)                         │
│                                                              │
│ • nltk          - Text processing (MapReduce + testing)     │
│ • pandas        - Data processing (download script)         │
│ • numpy         - Numerical operations                       │
│ • requests      - HTTP requests                              │
│ • beautifulsoup4- HTML parsing                              │
│ • gutenberg     - Project Gutenberg API                     │
│ • colorama      - Colored terminal output                    │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ WORKER NODES (where MapReduce tasks run)                    │
│                                                              │
│ • nltk          - Text processing (mapper/reducer only!)    │
│                                                              │
│ Note: pandas, requests, etc. NOT needed on workers          │
└──────────────────────────────────────────────────────────────┘
```

---

## Quick Start: Install All Dependencies

### Option 1: Use the Complete Installation Script (Recommended)

```bash
# Edit worker nodes list
nano install_all_dependencies.sh
# Update WORKER_NODES=("hadoop-worker-1" "hadoop-worker-2" ...)

# Run installation
./install_all_dependencies.sh

# This will:
# - Install ALL libraries on master node
# - Install ONLY NLTK on worker nodes
# - Download NLTK data on all nodes
# - Verify everything works
```

### Option 2: Use requirements.txt

```bash
# On master node
pip3 install --user -r requirements.txt

# On each worker node
ssh hadoop-worker-1 "pip3 install --user nltk"
ssh hadoop-worker-2 "pip3 install --user nltk"
# etc.
```

---

## Installing Additional Libraries

### Adding a New Library (Example: pandas)

#### Option 1: On Master Node Only

If you need a library ONLY for scripts that run on the master (like the download script):

```bash
# Install on master node
pip3 install --user pandas

# That's it! No need to install on workers.
```

#### Option 2: On All Nodes (Master + Workers)

If you need a library in your mapper/reducer (runs on workers):

```bash
# Install on master
pip3 install --user pandas

# Install on all workers
for node in hadoop-worker-1 hadoop-worker-2; do
    echo "Installing pandas on $node..."
    ssh $node "pip3 install --user pandas"
done
```

#### Option 3: Update requirements.txt

```bash
# Add to requirements.txt
echo "pandas>=1.3.0" >> requirements.txt

# Install on master
pip3 install --user -r requirements.txt

# Install on workers (if needed for MapReduce)
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user -r requirements.txt"
done
```

---

## Understanding Where to Install

### Decision Tree: Where Should I Install This Library?

```
START: I need to install library X

Question 1: Where is the library used?
  │
  ├─ In download script (donwload_file.py)
  │  └─> Install on MASTER ONLY
  │      Example: pip3 install --user pandas
  │
  ├─ In mapper (inverted_index_mapper.py)
  │  └─> Install on ALL NODES (master + workers)
  │      Example: pip3 install --user nltk (on all nodes)
  │
  ├─ In reducer (inverted_index_reducer.py)
  │  └─> Install on ALL NODES (master + workers)
  │      Example: pip3 install --user pandas (on all nodes)
  │
  └─ For testing/development
     └─> Install on MASTER ONLY
         Example: pip3 install --user pytest
```

### Examples

| Library | Used In | Install Where? |
|---------|---------|----------------|
| nltk | Mapper | ✅ Master + Workers |
| pandas | Download script | ✅ Master only |
| requests | Download script | ✅ Master only |
| beautifulsoup4 | Download script | ✅ Master only |
| gutenberg | Download script | ✅ Master only |
| colorama | Download script | ✅ Master only |
| numpy | Download script | ✅ Master only |

---

## Common Scenarios

### Scenario 1: Add pandas to Mapper

If you want to use pandas in your mapper:

```bash
# 1. Update requirements.txt (optional but recommended)
echo "pandas>=1.3.0" >> requirements.txt

# 2. Install on master
pip3 install --user pandas

# 3. Install on ALL workers
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user pandas"
done

# 4. Verify installation
ssh hadoop-worker-1 "python3 -c 'import pandas; print(pandas.__version__)'"
```

### Scenario 2: Add scikit-learn for Machine Learning

```bash
# Master only (for data preprocessing)
pip3 install --user scikit-learn

# Verify
python3 -c "import sklearn; print(sklearn.__version__)"
```

### Scenario 3: Add scipy for Scientific Computing in MapReduce

```bash
# Need on all nodes (used in mapper)
pip3 install --user scipy

# Workers
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user scipy"
done
```

### Scenario 4: Multiple Libraries at Once

```bash
# Create custom requirements file
cat > my_requirements.txt << EOF
pandas>=1.3.0
numpy>=1.21.0
scipy>=1.7.0
scikit-learn>=1.0.0
EOF

# Install on master
pip3 install --user -r my_requirements.txt

# Install on workers (only if needed for MapReduce!)
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user -r my_requirements.txt"
done
```

---

## Troubleshooting

### Issue 1: "No module named 'pandas'" on Master

```bash
# Install pandas
pip3 install --user pandas

# Verify
python3 -c "import pandas; print('OK')"
```

### Issue 2: "No module named 'pandas'" During MapReduce

This means pandas is needed in mapper/reducer but not installed on workers:

```bash
# Install on all workers
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user pandas"
done

# Verify
ssh hadoop-worker-1 "python3 -c 'import pandas; print(\"OK\")'"
```

### Issue 3: Different Versions on Different Nodes

```bash
# Check versions
python3 -c "import pandas; print(pandas.__version__)"  # Master
ssh hadoop-worker-1 "python3 -c 'import pandas; print(pandas.__version__)'"

# Fix: Reinstall with specific version
pip3 install --user pandas==1.5.3
ssh hadoop-worker-1 "pip3 install --user pandas==1.5.3"
```

### Issue 4: Library Installed but Not Found

```bash
# Check if pip user directory is in PATH
python3 -c "import sys; print('\n'.join(sys.path))"

# Add to ~/.bashrc if needed
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Issue 5: Permission Denied

```bash
# Make sure using --user flag
pip3 install --user pandas  # Correct
pip3 install pandas         # Wrong (requires sudo)
```

---

## Best Practices

### 1. Always Use --user Flag

```bash
# ✓ Good (no sudo required)
pip3 install --user pandas

# ✗ Bad (requires sudo)
pip3 install pandas
```

### 2. Keep requirements.txt Updated

```bash
# After installing new library
pip3 freeze --user | grep -E "pandas|numpy|nltk" >> requirements.txt

# Or manually add
echo "pandas>=1.3.0" >> requirements.txt
```

### 3. Version Pinning

```bash
# Flexible (gets latest compatible)
pandas>=1.3.0

# Strict (exact version)
pandas==1.5.3

# Recommended for production
pandas>=1.5.0,<2.0.0
```

### 4. Test Before MapReduce

```bash
# Test mapper locally with sample data
echo "sample text" | python3 inverted_index_mapper.py

# If it works locally, it should work in MapReduce
# (assuming same libraries on workers)
```

---

## Managing NLTK Data

### Download Specific Datasets

```bash
# Interactive download
python3 -m nltk.downloader

# Programmatic download
python3 << EOF
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
EOF
```

### Check What's Installed

```bash
python3 << EOF
import nltk
print("NLTK Data Path:", nltk.data.path)

# List installed corpora
import os
nltk_data_dir = os.path.expanduser('~/nltk_data/corpora')
if os.path.exists(nltk_data_dir):
    print("Installed corpora:")
    for item in os.listdir(nltk_data_dir):
        print(f"  - {item}")
EOF
```

---

## Advanced: Creating Virtual Environments (Alternative)

If you want isolation (not recommended for Hadoop but good for local testing):

```bash
# Create virtual environment
python3 -m venv ~/myproject_env

# Activate
source ~/myproject_env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Deactivate
deactivate
```

**Note:** Virtual environments don't work well with Hadoop Streaming. Use `--user` installations instead.

---

## Automated Installation Helper Script

Create a custom installation helper:

```bash
cat > install_library.sh << 'EOF'
#!/bin/bash

LIBRARY=$1
WORKERS="hadoop-worker-1 hadoop-worker-2"

if [ -z "$LIBRARY" ]; then
    echo "Usage: ./install_library.sh <library_name>"
    exit 1
fi

echo "Installing $LIBRARY on master..."
pip3 install --user $LIBRARY

echo ""
read -p "Install on workers too? (yes/no): " confirm

if [ "$confirm" = "yes" ]; then
    for worker in $WORKERS; do
        echo "Installing on $worker..."
        ssh $worker "pip3 install --user $LIBRARY"
    done
fi

echo "Done!"
EOF

chmod +x install_library.sh

# Usage:
./install_library.sh pandas
```

---

## Quick Reference Commands

```bash
# Install library on master
pip3 install --user <library>

# Install library on all workers
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user <library>"
done

# List installed libraries
pip3 list --user

# Check library version
python3 -c "import <library>; print(<library>.__version__)"

# Uninstall library
pip3 uninstall <library>

# Upgrade library
pip3 install --user --upgrade <library>

# Install from requirements.txt
pip3 install --user -r requirements.txt

# Generate requirements.txt
pip3 freeze --user > requirements.txt
```

---

## Summary

### Current Project Dependencies

**requirements.txt** contains all project dependencies:
- nltk (required on ALL nodes)
- pandas (master only)
- numpy (master only)
- requests (master only)
- beautifulsoup4 (master only)
- gutenberg (master only)
- colorama (master only)

### To Install Everything

```bash
# Automated (recommended)
./install_all_dependencies.sh

# Manual
pip3 install --user -r requirements.txt  # On master
ssh hadoop-worker-1 "pip3 install --user nltk"  # On each worker
```

### To Add New Library

```bash
# If used in download script → Install on master only
pip3 install --user <library>

# If used in mapper/reducer → Install on all nodes
pip3 install --user <library>
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user <library>"
done
```

---

## Files You Have

1. **requirements.txt** - List of all Python dependencies
2. **install_all_dependencies.sh** - Automated installation script
3. **install_dependencies_no_sudo.sh** - NLTK-only installation
4. **install_dependencies_all_nodes.sh** - Original (requires sudo)

**Recommended:** Use `install_all_dependencies.sh` for complete setup!
