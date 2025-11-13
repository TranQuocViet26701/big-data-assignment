# Installation Quick Reference

## Your Project Dependencies

Your Inverted Index MapReduce project uses these Python libraries:

### On Master Node (all scripts)
- **nltk** - Text processing
- **pandas** - Data processing
- **numpy** - Numerical operations
- **requests** - HTTP requests
- **beautifulsoup4** - HTML parsing
- **gutenberg** - Project Gutenberg API
- **colorama** - Colored output

### On Worker Nodes (MapReduce only)
- **nltk** - Text processing (mapper/reducer)

---

## 🚀 Quick Start: Install Everything

### Step 1: Edit Worker Nodes

```bash
nano install_all_dependencies.sh
```

Update this section with your worker hostnames:
```bash
WORKER_NODES=(
    "hadoop-worker-1"
    "hadoop-worker-2"
    # Add more workers
)
```

### Step 2: Run Installation

```bash
./install_all_dependencies.sh
```

This will:
- ✅ Install ALL libraries on master node
- ✅ Install ONLY NLTK on worker nodes
- ✅ Download NLTK data on all nodes
- ✅ Verify everything works
- ✅ No sudo required!

### Step 3: Run Your Pipeline

```bash
# Download books
python3 donwload_file.py

# Run MapReduce
./run_inverted_index_mapreduce.sh 2

# View results
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

---

## 📦 Installation Scripts Available

| Script | Purpose | Sudo Required? | Installs |
|--------|---------|----------------|----------|
| **install_all_dependencies.sh** ⭐ | Complete setup | ❌ No | All libs on master, NLTK on workers |
| install_dependencies_no_sudo.sh | NLTK only | ❌ No | NLTK on all nodes |
| install_dependencies_all_nodes.sh | System-wide | ✅ Yes | Requires sudo |

**Recommended:** Use `install_all_dependencies.sh`

---

## 🔧 Installing Additional Libraries

### Example: Install pandas

#### If needed ONLY on master (for scripts):
```bash
pip3 install --user pandas
```

#### If needed on ALL nodes (for MapReduce):
```bash
# Master
pip3 install --user pandas

# Workers
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user pandas"
done
```

### Example: Install scikit-learn
```bash
# Add to requirements.txt
echo "scikit-learn>=1.0.0" >> requirements.txt

# Install on master
pip3 install --user scikit-learn

# If needed for MapReduce, install on workers too
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user scikit-learn"
done
```

---

## 📖 Documentation Files

| File | Purpose |
|------|---------|
| **requirements.txt** | List of all dependencies |
| **PYTHON_DEPENDENCIES_GUIDE.md** | Complete dependency management guide |
| **TROUBLESHOOTING_SUDO.md** | Fix sudo password issues |
| **SSH_SETUP_GUIDE.md** | SSH configuration help |
| **QUICK_START_ktdl9.md** | Quick start for ktdl9 user |
| **MULTI_NODE_DEPLOYMENT_GUIDE.md** | Full deployment guide |
| **EXECUTION_FLOW_OVERVIEW.md** | Pipeline flow diagrams |

---

## 🧪 Verification Commands

### Check if library is installed:
```bash
# On master
python3 -c "import pandas; print('pandas:', pandas.__version__)"

# On worker
ssh hadoop-worker-1 "python3 -c 'import nltk; print(\"NLTK OK\")'"
```

### List all installed libraries:
```bash
pip3 list --user
```

### Check NLTK data:
```bash
python3 -c "from nltk.corpus import stopwords; print(len(stopwords.words('english')), 'stopwords loaded')"
```

---

## ❓ Common Questions

### Q: Do I need sudo?
**A:** No! All scripts use `pip3 install --user` for user-space installation.

### Q: Do workers need pandas?
**A:** No! Workers only need NLTK (unless you use pandas in mapper/reducer).

### Q: Where are libraries installed?
**A:** `~/.local/lib/python3.x/site-packages/` (user space, no sudo needed)

### Q: Where is NLTK data?
**A:** `~/nltk_data/` (in your home directory)

### Q: How to add a new library?
**A:**
```bash
# Master only
pip3 install --user <library>

# All nodes (if needed for MapReduce)
for node in hadoop-worker-1 hadoop-worker-2; do
    ssh $node "pip3 install --user <library>"
done
```

---

## 🎯 Recommended Workflow

### First Time Setup:
```bash
1. Edit worker nodes: nano install_all_dependencies.sh
2. Run installation: ./install_all_dependencies.sh
3. Verify: python3 -c "import nltk, pandas, requests; print('OK')"
```

### Adding New Libraries:
```bash
1. Add to requirements.txt: echo "scipy>=1.7.0" >> requirements.txt
2. Install on master: pip3 install --user scipy
3. If needed for MapReduce:
   for node in hadoop-worker-1 hadoop-worker-2; do
       ssh $node "pip3 install --user scipy"
   done
```

### Running Jobs:
```bash
1. Download data: python3 donwload_file.py
2. Run MapReduce: ./run_inverted_index_mapreduce.sh 2
3. View results: hdfs dfs -cat /gutenberg-output/part-* | head
```

---

## 📞 Need Help?

- **Sudo issues?** → See `TROUBLESHOOTING_SUDO.md`
- **SSH issues?** → See `SSH_SETUP_GUIDE.md`
- **Dependency questions?** → See `PYTHON_DEPENDENCIES_GUIDE.md`
- **Full deployment?** → See `MULTI_NODE_DEPLOYMENT_GUIDE.md`
- **Quick start?** → See `QUICK_START_ktdl9.md`

---

## ✅ Final Checklist

Before running MapReduce job:

- [ ] Python 3 installed on all nodes
- [ ] pip3 available on all nodes
- [ ] Edited `install_all_dependencies.sh` with worker hostnames
- [ ] Ran `./install_all_dependencies.sh` successfully
- [ ] Verified NLTK works: `python3 -c "from nltk.corpus import stopwords"`
- [ ] HDFS has data: `hdfs dfs -ls /gutenberg-input`
- [ ] YARN is running: `yarn node -list`

Then run:
```bash
./run_inverted_index_mapreduce.sh 2
```

Good luck! 🎉
