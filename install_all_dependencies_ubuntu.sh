#!/bin/bash

################################################################################
# Complete Dependency Installation for Ubuntu/Debian (Python 3.12+)
#
# This script handles the "externally-managed-environment" error in Python 3.12+
# by using the --break-system-packages flag.
#
# ⚠️ WARNING: Using --break-system-packages can potentially conflict with
# system packages. This is acceptable for Hadoop worker nodes but use caution.
#
# Alternative: Use system packages (sudo apt install python3-pandas etc.)
# See UBUNTU_PYTHON312_FIX.md for other options.
#
# Prerequisites:
#   - Ubuntu/Debian with Python 3.12+
#   - SSH access to all workers
#   - pip3 available
#   - NO sudo required for pip installs!
#
# Usage: ./install_all_dependencies_ubuntu.sh
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Configuration
CURRENT_USER=$(whoami)
USER_HOME="$HOME"
NLTK_DATA_DIR="$USER_HOME/nltk_data"

# PIP flags for Python 3.12+ externally-managed environment
PIP_FLAGS="--user --break-system-packages"

# ⚠️ EDIT THIS: Add your worker node hostnames
WORKER_NODES=(
    "hadoop-worker-1"
    "hadoop-worker-2"
    # "hadoop-worker-3"
    # Add more workers as needed
)

print_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

################################################################################
# Warning and Confirmation
################################################################################

print_header "Ubuntu/Debian Python 3.12+ Installation"

print_warning "This script uses --break-system-packages flag"
echo ""
echo -e "${MAGENTA}About --break-system-packages:${NC}"
echo "  • Bypasses Python 3.12's PEP 668 protection"
echo "  • Allows pip to install packages in user space"
echo "  • Generally safe for Hadoop worker nodes"
echo "  • May conflict with system Python packages (rare)"
echo ""
echo -e "${MAGENTA}Alternative (if you have sudo):${NC}"
echo "  sudo apt install python3-pandas python3-nltk python3-numpy"
echo "  (See UBUNTU_PYTHON312_FIX.md for details)"
echo ""

print_status "Current user: $CURRENT_USER"
print_status "Python version: $(python3 --version 2>&1 || echo 'Not found')"
print_status "pip flags: $PIP_FLAGS"
print_status "Worker nodes: ${#WORKER_NODES[@]}"
for node in "${WORKER_NODES[@]}"; do
    echo "  - $node"
done

echo ""
read -p "Continue with installation using --break-system-packages? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    print_warning "Installation cancelled"
    echo ""
    print_status "Alternative options:"
    echo "  1. Use system packages: See UBUNTU_PYTHON312_FIX.md"
    echo "  2. Use pipx: See UBUNTU_PYTHON312_FIX.md"
    echo "  3. Use virtual env: See UBUNTU_PYTHON312_FIX.md"
    exit 0
fi

################################################################################
# Test SSH Connectivity
################################################################################

print_header "Testing SSH Connectivity"

FAILED_NODES=()
for node in "${WORKER_NODES[@]}"; do
    print_status "Testing SSH to $node..."
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "$node" "echo 'SSH OK'" > /dev/null 2>&1; then
        print_success "SSH to $node: OK"
    else
        print_error "SSH to $node: FAILED"
        FAILED_NODES+=("$node")
    fi
done

if [ ${#FAILED_NODES[@]} -gt 0 ]; then
    print_error "Cannot SSH to the following nodes:"
    for node in "${FAILED_NODES[@]}"; do
        echo "  - $node"
    done
    exit 1
fi

print_success "All nodes are accessible"

################################################################################
# Install on Master Node
################################################################################

print_header "Installing Dependencies on Master Node"

# Check Python 3
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found"
    exit 1
fi
PYTHON_VERSION=$(python3 --version 2>&1)
print_success "Python: $PYTHON_VERSION"

# Check pip3
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 not found. Install with: sudo apt install python3-pip"
    exit 1
fi
print_success "pip3: $(pip3 --version | head -1)"

# Upgrade pip (with break-system-packages flag)
print_status "Upgrading pip..."
pip3 install $PIP_FLAGS --upgrade pip --quiet 2>&1 | grep -v "WARNING" || true

# Install all libraries from requirements.txt
print_status "Installing Python libraries..."
if [ -f "requirements.txt" ]; then
    print_status "Installing from requirements.txt..."

    # Read requirements and install each with proper flags
    while IFS= read -r line; do
        # Skip comments and empty lines
        [[ "$line" =~ ^#.*$ ]] && continue
        [[ -z "$line" ]] && continue

        # Extract package name (before >= or ==)
        package=$(echo "$line" | cut -d'>' -f1 | cut -d'=' -f1 | cut -d'<' -f1 | tr -d ' ')

        print_status "Installing $package..."
        pip3 install $PIP_FLAGS "$line" --quiet 2>&1 | grep -v "WARNING" || true
    done < requirements.txt

    print_success "All libraries installed from requirements.txt"
else
    print_warning "requirements.txt not found, installing individually..."

    declare -a packages=("nltk" "pandas" "numpy" "requests" "beautifulsoup4" "gutenberg" "colorama")

    for package in "${packages[@]}"; do
        print_status "Installing $package..."
        pip3 install $PIP_FLAGS "$package" --quiet 2>&1 | grep -v "WARNING" || true
    done

    print_success "All libraries installed"
fi

# Download NLTK data
print_status "Downloading NLTK data to $NLTK_DATA_DIR..."
python3 << EOF
import nltk
import os

nltk_data_dir = '$NLTK_DATA_DIR'
os.makedirs(nltk_data_dir, exist_ok=True)

print("Downloading stopwords...")
nltk.download('stopwords', download_dir=nltk_data_dir)
print("Downloading wordnet...")
nltk.download('wordnet', download_dir=nltk_data_dir)
print("Downloading punkt...")
nltk.download('punkt', download_dir=nltk_data_dir)
print("Downloading omw-1.4...")
nltk.download('omw-1.4', download_dir=nltk_data_dir)
print("NLTK data download complete!")
EOF

print_success "Master node setup complete"

################################################################################
# Install on Worker Nodes
################################################################################

print_header "Installing Dependencies on Worker Nodes"

print_status "Workers only need NLTK (not pandas, requests, etc.)"
print_status "Installing NLTK on all worker nodes..."

INSTALL_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
set -e

NODE_NAME=$(hostname)
USER_HOME="$HOME"
NLTK_DATA_DIR="$USER_HOME/nltk_data"
PIP_FLAGS="--user --break-system-packages"

echo "[INFO] Installing on $NODE_NAME (using --break-system-packages)..."

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python 3 not found"
    exit 1
fi
echo "[OK] Python: $(python3 --version 2>&1)"

# Check pip3
if ! command -v pip3 &> /dev/null; then
    echo "[ERROR] pip3 not found. Ask admin: sudo apt install python3-pip"
    exit 1
fi

# Upgrade pip
echo "[INFO] Upgrading pip..."
pip3 install $PIP_FLAGS --upgrade pip --quiet 2>&1 | grep -v "WARNING" || true

# Install NLTK only
echo "[INFO] Installing NLTK..."
pip3 install $PIP_FLAGS nltk --quiet 2>&1 | grep -v "WARNING" || true

# Download NLTK data
echo "[INFO] Downloading NLTK data..."
python3 << 'EOF'
import nltk
import os

nltk_data_dir = os.path.expanduser('~/nltk_data')
os.makedirs(nltk_data_dir, exist_ok=True)

nltk.download('stopwords', download_dir=nltk_data_dir, quiet=True)
nltk.download('wordnet', download_dir=nltk_data_dir, quiet=True)
nltk.download('punkt', download_dir=nltk_data_dir, quiet=True)
nltk.download('omw-1.4', download_dir=nltk_data_dir, quiet=True)
EOF

echo "[SUCCESS] Installation completed on $NODE_NAME"
EOFSCRIPT
)

FAILED_INSTALLS=()
for node in "${WORKER_NODES[@]}"; do
    print_status "Installing on $node..."
    if echo "$INSTALL_SCRIPT" | ssh "$node" "bash -s"; then
        print_success "$node: Complete"
    else
        print_error "$node: Failed"
        FAILED_INSTALLS+=("$node")
    fi
    echo ""
done

if [ ${#FAILED_INSTALLS[@]} -gt 0 ]; then
    print_error "Installation failed on:"
    for node in "${FAILED_INSTALLS[@]}"; do
        echo "  - $node"
    done
    exit 1
fi

################################################################################
# Verify Installation
################################################################################

print_header "Verifying Installation"

# Verify master node
print_status "Verifying master node..."
python3 << 'EOF'
import sys

print("[INFO] Checking installed libraries...")

required_libs = {
    'nltk': 'NLTK',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'requests': 'requests',
    'bs4': 'BeautifulSoup4',
    'gutenberg': 'gutenberg',
    'colorama': 'colorama'
}

missing = []
for module, name in required_libs.items():
    try:
        __import__(module)
        print(f"  ✓ {name}")
    except ImportError:
        print(f"  ✗ {name} (missing)")
        missing.append(name)

if missing:
    print(f"\n[ERROR] Missing libraries: {', '.join(missing)}")
    sys.exit(1)
else:
    print("\n[SUCCESS] All libraries installed on master node")
EOF

# Verify NLTK functionality on master
python3 << 'EOF'
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

try:
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    test_word = lemmatizer.lemmatize('running')
    print("[SUCCESS] NLTK working correctly on master")
except Exception as e:
    print(f"[ERROR] NLTK not working: {e}")
    exit(1)
EOF

# Verify worker nodes
VERIFY_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
NODE_NAME=$(hostname)

python3 << 'EOF'
import sys

try:
    import nltk
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer

    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()

    print(f"[SUCCESS] Node ready")
except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit(1)
EOF
EOFSCRIPT
)

FAILED_VERIFICATIONS=()
for node in "${WORKER_NODES[@]}"; do
    print_status "Verifying $node..."
    if echo "$VERIFY_SCRIPT" | ssh "$node" "bash -s"; then
        print_success "$node: OK"
    else
        print_error "$node: FAILED"
        FAILED_VERIFICATIONS+=("$node")
    fi
done

if [ ${#FAILED_VERIFICATIONS[@]} -gt 0 ]; then
    print_error "Verification failed on:"
    for node in "${FAILED_VERIFICATIONS[@]}"; do
        echo "  - $node"
    done
    exit 1
fi

################################################################################
# Summary
################################################################################

print_header "Installation Complete!"

print_success "All nodes ready for MapReduce job"
echo ""
print_status "Installation Summary:"
echo ""
echo "  Master Node:"
echo "    ✓ Python $(python3 --version 2>&1 | cut -d' ' -f2)"
echo "    ✓ NLTK, pandas, numpy, requests, beautifulsoup4, gutenberg, colorama"
echo "    ✓ Installed with: pip3 $PIP_FLAGS"
echo ""
echo "  Worker Nodes (${#WORKER_NODES[@]}):"
for node in "${WORKER_NODES[@]}"; do
    echo "    ✓ $node: Python 3 + NLTK"
done
echo ""
print_status "Next Steps:"
echo "  1. Download data:  python3 donwload_file.py"
echo "  2. Run MapReduce:  ./run_inverted_index_mapreduce.sh 2"
echo "  3. View results:   hdfs dfs -cat /gutenberg-output/part-* | head"
echo ""
print_status "Installation locations:"
echo "  Libraries: ~/.local/lib/python3.x/site-packages/"
echo "  NLTK data: ~/nltk_data/"
echo ""
print_warning "Note: Used --break-system-packages flag (Python 3.12+ requirement)"
print_status "See UBUNTU_PYTHON312_FIX.md for alternative installation methods"
echo ""

exit 0
