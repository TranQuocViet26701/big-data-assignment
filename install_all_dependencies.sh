#!/bin/bash

################################################################################
# Complete Dependency Installation (User-Space, No Sudo Required)
#
# This script installs ALL Python dependencies for the Inverted Index project:
#   - On MASTER: All libraries (NLTK, pandas, requests, etc.)
#   - On WORKERS: Only NLTK (required for MapReduce tasks)
#
# Prerequisites:
#   - SSH access to all workers
#   - Python 3 installed on all nodes
#   - pip3 available on all nodes
#   - NO sudo required!
#
# Usage: ./install_all_dependencies.sh
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
CURRENT_USER=$(whoami)
USER_HOME="$HOME"
NLTK_DATA_DIR="$USER_HOME/nltk_data"

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
# Configuration
################################################################################

print_header "Complete Dependency Installation"

print_status "Current user: $CURRENT_USER"
print_status "Installation type: User-space (no sudo required)"
print_status "NLTK data location: $NLTK_DATA_DIR"
print_status ""
print_status "Dependencies to install:"
echo "  On MASTER node:"
echo "    • nltk (text processing for MapReduce)"
echo "    • pandas (data processing for download script)"
echo "    • numpy (numerical operations)"
echo "    • requests (HTTP requests)"
echo "    • beautifulsoup4 (HTML parsing)"
echo "    • gutenberg (Project Gutenberg API)"
echo "    • colorama (colored terminal output)"
echo ""
echo "  On WORKER nodes:"
echo "    • nltk (required for mapper/reducer execution)"
echo ""
print_status "Worker nodes (${#WORKER_NODES[@]}):"
for node in "${WORKER_NODES[@]}"; do
    echo "  - $node"
done

echo ""
read -p "Continue with installation? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    print_warning "Installation cancelled"
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
    print_warning "Please set up passwordless SSH first"
    exit 1
fi

print_success "All nodes are accessible"

################################################################################
# Install on Master Node
################################################################################

print_header "Installing Dependencies on Master Node"

# Check Python 3
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found. Please ask admin to install Python 3."
    exit 1
fi
print_success "Python 3: $(python3 --version)"

# Check pip3
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 not found. Please ask admin to install python3-pip."
    exit 1
fi
print_success "pip3: $(pip3 --version | head -1)"

# Upgrade pip in user space
print_status "Upgrading pip..."
pip3 install --user --upgrade pip --quiet

# Install all libraries from requirements.txt
print_status "Installing Python libraries from requirements.txt..."
if [ -f "requirements.txt" ]; then
    pip3 install --user -r requirements.txt
    print_success "All libraries installed from requirements.txt"
else
    print_warning "requirements.txt not found, installing individually..."

    print_status "Installing nltk..."
    pip3 install --user nltk

    print_status "Installing pandas..."
    pip3 install --user pandas

    print_status "Installing numpy..."
    pip3 install --user numpy

    print_status "Installing requests..."
    pip3 install --user requests

    print_status "Installing beautifulsoup4..."
    pip3 install --user beautifulsoup4

    print_status "Installing gutenberg..."
    pip3 install --user gutenberg

    print_status "Installing colorama..."
    pip3 install --user colorama

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

print_warning "Workers only need NLTK (not pandas, requests, etc.)"
print_status "Installing NLTK on all worker nodes..."

INSTALL_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
set -e

NODE_NAME=$(hostname)
USER_HOME="$HOME"
NLTK_DATA_DIR="$USER_HOME/nltk_data"

echo "[INFO] Installing on $NODE_NAME..."

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python 3 not found. Please ask admin to install."
    exit 1
fi
echo "[OK] Python 3: $(python3 --version)"

# Check pip3
if ! command -v pip3 &> /dev/null; then
    echo "[ERROR] pip3 not found. Please ask admin to install."
    exit 1
fi

# Upgrade pip
echo "[INFO] Upgrading pip..."
pip3 install --user --upgrade pip --quiet

# Install NLTK only
echo "[INFO] Installing NLTK..."
pip3 install --user nltk --quiet

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

# Verify NLTK data on master
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

    print(f"[SUCCESS] {sys.argv[0] if len(sys.argv) > 0 else 'Node'} ready")
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
echo "    ✓ Python 3 + pip3"
echo "    ✓ NLTK (with data)"
echo "    ✓ pandas, numpy"
echo "    ✓ requests, beautifulsoup4, gutenberg"
echo "    ✓ colorama"
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
print_status "All libraries installed in user space at:"
echo "  ~/.local/lib/python3.x/site-packages/"
echo ""
print_status "NLTK data installed at:"
echo "  ~/nltk_data/"
echo ""

exit 0
