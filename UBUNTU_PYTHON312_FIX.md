# Ubuntu/Debian Python 3.12+ Installation Guide
## Fixing "externally-managed-environment" Error (PEP 668)

---

## The Error You're Seeing

```
error: externally-managed-environment

× This environment is externally managed
╰─> To install Python packages system-wide, try apt install
    python3-xyz, where xyz is the package you are trying to
    install.

    If you wish to install a non-Debian-packaged Python package,
    create a virtual environment using python3 -m venv path/to/venv.
    Then use path/to/venv/bin/python and path/to/venv/bin/pip. Make
    sure you have python3-full installed.
```

---

## What This Means

Starting with **Python 3.12**, Debian and Ubuntu systems implement **PEP 668**, which prevents `pip` from installing packages that might conflict with system packages.

This is a **security feature** to protect your system Python environment.

---

## Solutions (4 Options)

### Option 1: Use --break-system-packages Flag ⚡ (Fastest)

**Pros:** Quick, works immediately, no sudo required
**Cons:** May conflict with system packages (rare), not recommended for production servers
**Best for:** Hadoop worker nodes, development systems, controlled environments

#### Quick Fix:
```bash
# Instead of: pip3 install --user pandas
# Use: pip3 install --user --break-system-packages pandas
```

#### Automated Script:
```bash
# We've already created this for you!
./install_all_dependencies_ubuntu.sh
```

The script automatically:
- Detects Python 3.12+
- Adds `--break-system-packages` flag
- Installs all dependencies on master
- Installs NLTK on workers

**Single command:**
```bash
./install_all_dependencies_ubuntu.sh
```

Or use the auto-detecting script:
```bash
./install_all_dependencies.sh  # Automatically detects and uses correct flags
```

---

### Option 2: Use System Packages via apt 🏛️ (Cleanest)

**Pros:** Clean, no conflicts, officially supported
**Cons:** Requires sudo, may have older versions, not all packages available
**Best for:** Production servers, officially managed systems

#### Installation:

```bash
# On master node
sudo apt update
sudo apt install -y \
    python3-nltk \
    python3-pandas \
    python3-numpy \
    python3-requests \
    python3-bs4 \
    python3-pip

# Download NLTK data
sudo python3 -c "
import nltk
nltk.download('stopwords', download_dir='/root/nltk_data')
nltk.download('wordnet', download_dir='/root/nltk_data')
nltk.download('punkt', download_dir='/root/nltk_data')
nltk.download('omw-1.4', download_dir='/root/nltk_data')
"

# On each worker node
ssh hadoop-worker-1 << 'EOF'
sudo apt update
sudo apt install -y python3-nltk
sudo python3 -c "
import nltk
nltk.download('stopwords', download_dir='/root/nltk_data')
nltk.download('wordnet', download_dir='/root/nltk_data')
nltk.download('punkt', download_dir='/root/nltk_data')
nltk.download('omw-1.4', download_dir='/root/nltk_data')
"
EOF
```

**Note:** Some packages may not be available via apt:
- `gutenberg` - Not in apt repositories (use Option 1 for this)
- `colorama` - Available as `python3-colorama`

**Update mapper to use system NLTK data:**
```bash
# Edit inverted_index_mapper.py
# Change: nltk.data.path.append(os.path.expanduser("~/nltk_data"))
# To: nltk.data.path.append("/usr/share/nltk_data")
```

---

### Option 3: Use pipx 🎁 (Modern Approach)

**Pros:** Isolated environments, modern Python practice
**Cons:** Complicates Hadoop Streaming (not recommended for this project)
**Best for:** Standalone CLI applications

```bash
# Install pipx
sudo apt install pipx
pipx ensurepath

# Install packages
pipx install pandas
pipx install nltk

# Download NLTK data
pipx run python -c "import nltk; nltk.download('stopwords')"
```

**Not recommended for MapReduce** because:
- Hadoop Streaming needs packages accessible via regular `python3`
- pipx creates isolated environments per application
- Extra complexity for minimal benefit in this use case

---

### Option 4: Virtual Environment 🐍 (Traditional)

**Pros:** Isolated from system, officially recommended
**Cons:** Complex with Hadoop Streaming, requires extra configuration
**Best for:** Local development/testing only

```bash
# Create virtual environment
python3 -m venv ~/myenv

# Activate
source ~/myenv/bin/python

# Install dependencies
pip install -r requirements.txt

# Download NLTK data
python -c "
import nltk
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('omw-1.4')
"
```

**For Hadoop Streaming**, you'd need to:
1. Package the venv with your job
2. Modify scripts to use venv Python
3. Add complexity to job submission

**Not recommended for this project** due to Hadoop Streaming complications.

---

## Comparison Table

| Option | Speed | Sudo? | Complexity | Hadoop Friendly? | Recommended? |
|--------|-------|-------|------------|------------------|--------------|
| **--break-system-packages** | ⚡⚡⚡ Fast | ❌ No | ⭐ Easy | ✅ Yes | ✅ **Best for Hadoop** |
| **apt install** | ⚡⚡ Medium | ✅ Yes | ⭐⭐ Medium | ✅ Yes | ✅ Best for production |
| **pipx** | ⚡ Slow | ✅ Yes (install) | ⭐⭐⭐ Complex | ❌ No | ❌ Not for MapReduce |
| **Virtual env** | ⚡⚡ Medium | ❌ No | ⭐⭐⭐⭐ Very Complex | ⚠️ Possible but hard | ❌ Not for Hadoop |

---

## Recommended Approach for Your Project

### If You Have Sudo Access:

**Best:** Option 2 (apt) for core libraries, Option 1 for extras
```bash
# Install most libraries via apt
sudo apt install python3-nltk python3-pandas python3-numpy python3-requests python3-bs4

# Install unavailable libraries with --break-system-packages
pip3 install --user --break-system-packages gutenberg colorama
```

### If You DON'T Have Sudo:

**Best:** Option 1 (--break-system-packages)
```bash
# Use our automated script
./install_all_dependencies_ubuntu.sh
```

---

## Step-by-Step: Using --break-system-packages (Recommended)

### Method A: Automated Script (Easiest)

```bash
# 1. Edit worker nodes
nano install_all_dependencies_ubuntu.sh
# Update WORKER_NODES=("hadoop-worker-1" "hadoop-worker-2" ...)

# 2. Run installation
./install_all_dependencies_ubuntu.sh

# 3. Verify
python3 -c "import nltk, pandas, numpy; print('All OK')"

# 4. Run your pipeline
python3 donwload_file.py
./run_inverted_index_mapreduce.sh 2
```

### Method B: Manual Installation

```bash
# Master node
pip3 install --user --break-system-packages nltk pandas numpy requests beautifulsoup4 gutenberg colorama

# Download NLTK data
python3 -c "
import nltk
import os
nltk_dir = os.path.expanduser('~/nltk_data')
os.makedirs(nltk_dir, exist_ok=True)
nltk.download('stopwords', download_dir=nltk_dir)
nltk.download('wordnet', download_dir=nltk_dir)
nltk.download('punkt', download_dir=nltk_dir)
nltk.download('omw-1.4', download_dir=nltk_dir)
"

# Each worker node
ssh hadoop-worker-1 << 'EOF'
pip3 install --user --break-system-packages nltk
python3 -c "
import nltk
import os
nltk_dir = os.path.expanduser('~/nltk_data')
os.makedirs(nltk_dir, exist_ok=True)
nltk.download('stopwords', download_dir=nltk_dir)
nltk.download('wordnet', download_dir=nltk_dir)
nltk.download('punkt', download_dir=nltk_dir)
nltk.download('omw-1.4', download_dir=nltk_dir)
"
EOF
```

---

## FAQ

### Q: Is --break-system-packages dangerous?

**A:** For Hadoop worker nodes, it's generally safe because:
- Worker nodes typically don't run many system Python services
- User-space installs (`--user` flag) don't affect system packages
- Packages installed in `~/.local` are isolated from `/usr/lib`
- Hadoop uses Java, not Python, for core services

**However:** On production servers running critical Python services, prefer apt packages.

### Q: Will this break my system?

**A:** Unlikely, especially with `--user` flag:
```
System packages:  /usr/lib/python3/dist-packages/  (untouched)
Your packages:    ~/.local/lib/python3/site-packages/  (isolated)
```

The `--user` flag installs to your home directory, not system directories.

### Q: What if I get package conflicts?

**A:** Remove conflicting package:
```bash
pip3 uninstall package_name
pip3 install --user --break-system-packages package_name
```

### Q: Can I make --break-system-packages default?

**A:** Yes, create `~/.config/pip/pip.conf`:
```ini
[global]
break-system-packages = true
```

But this is **not recommended** - better to be explicit per command.

### Q: Should I upgrade all my packages?

**A:** Be cautious with system package upgrades:
```bash
# DON'T do this on production:
# pip3 install --user --break-system-packages --upgrade pip setuptools wheel

# DO install specific versions:
pip3 install --user --break-system-packages pandas==1.5.3
```

---

## Verification Commands

### Check if Python is externally managed:
```bash
python3 -c "import sysconfig; print(sysconfig.get_path('purelib'))"
# If contains /usr/lib → externally managed
# If contains ~/.local → user space OK
```

### Check installed packages:
```bash
# System packages (via apt)
apt list --installed | grep python3-

# User packages (via pip)
pip3 list --user

# Both
pip3 list
```

### Test NLTK:
```bash
python3 << 'EOF'
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()
print(f"Loaded {len(stop_words)} stopwords")
print(f"Lemmatize 'running': {lemmatizer.lemmatize('running')}")
print("NLTK OK!")
EOF
```

---

## Troubleshooting

### Issue: Still getting externally-managed error

**Solution:** Make sure you're using BOTH flags:
```bash
# Wrong
pip3 install --user pandas

# Correct
pip3 install --user --break-system-packages pandas
```

### Issue: Package not found after installation

**Solution:** Add pip user directory to PATH:
```bash
export PATH="$HOME/.local/bin:$PATH"

# Make permanent
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Issue: Wrong version installed

**Solution:** Uninstall and reinstall with specific version:
```bash
pip3 uninstall pandas
pip3 install --user --break-system-packages pandas==1.5.3
```

### Issue: MapReduce tasks fail with ImportError

**Solution:** Verify packages on workers:
```bash
for node in hadoop-worker-1 hadoop-worker-2; do
    echo "Checking $node..."
    ssh $node "python3 -c 'import nltk; print(\"NLTK OK\")'"
done
```

---

## Files We Created for You

| File | Purpose |
|------|---------|
| **install_all_dependencies_ubuntu.sh** | Ubuntu-specific with --break-system-packages |
| **install_all_dependencies.sh** | Auto-detects OS and Python version |
| install_dependencies_no_sudo.sh | Older script (before Python 3.12 support) |
| requirements.txt | List of all dependencies |

**Recommended:** Use `install_all_dependencies_ubuntu.sh` or the auto-detecting `install_all_dependencies.sh`

---

## Summary

**For your Hadoop MapReduce project on Ubuntu with Python 3.12:**

✅ **Easiest:** Use `./install_all_dependencies_ubuntu.sh` (handles everything automatically)
✅ **Cleanest:** Use `sudo apt install` for available packages, `--break-system-packages` for others
❌ **Avoid:** Virtual environments and pipx (too complex for Hadoop Streaming)

**Quick start:**
```bash
# Edit worker list
nano install_all_dependencies_ubuntu.sh

# Run
./install_all_dependencies_ubuntu.sh

# Done!
python3 donwload_file.py
./run_inverted_index_mapreduce.sh 2
```

---

## Further Reading

- [PEP 668: Marking Python base environments as "externally managed"](https://peps.python.org/pep-0668/)
- [Debian Python Policy](https://www.debian.org/doc/packaging-manuals/python-policy/)
- [pip --break-system-packages documentation](https://pip.pypa.io/en/stable/cli/pip_install/)
