#!/bin/bash

################################################################################
# Automated Dependency Installation for All Worker Nodes
#
# This script installs Python 3 and NLTK on all worker nodes in your cluster.
# Run this ONCE from the master node before executing the MapReduce job.
#
# Prerequisites:
#   - SSH access from master to all workers (passwordless recommended)
#   - Sudo privileges on worker nodes
#   - Edit WORKER_NODES array below with your actual worker hostnames
#
# Usage: ./install_dependencies_all_nodes.sh
################################################################################

set -e  # Exit on any error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CURRENT_USER=$(whoami)
NLTK_DATA_DIR="/root/nltk_data"

# ⚠️ EDIT THIS: Add your worker node hostnames
WORKER_NODES=(
    "worker1"
    "worker2"
    # "worker3"
    # "worker4"
    # Add more workers as needed
)

# Print functions
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
# Step 1: Verify Configuration
################################################################################

print_header "Step 1: Verify Configuration"

if [ ${#WORKER_NODES[@]} -eq 0 ]; then
    print_error "No worker nodes configured!"
    print_status "Please edit this script and add your worker node hostnames to WORKER_NODES array"
    exit 1
fi

print_status "Current user: $CURRENT_USER"
print_status "Number of worker nodes: ${#WORKER_NODES[@]}"
print_status "Worker nodes:"
for node in "${WORKER_NODES[@]}"; do
    echo "  - $node"
done

echo ""
read -p "Is this configuration correct? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    print_warning "Installation cancelled by user"
    exit 0
fi

################################################################################
# Step 2: Test SSH Connectivity
################################################################################

print_header "Step 2: Test SSH Connectivity"

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
    print_status "Please set up passwordless SSH first:"
    echo "  ssh-keygen -t rsa -b 4096 -N ''"
    echo "  ssh-copy-id $CURRENT_USER@worker1"
    echo "  ssh-copy-id $CURRENT_USER@worker2"
    exit 1
fi

print_success "All nodes are accessible via SSH"

################################################################################
# Step 3: Install Dependencies on Master Node
################################################################################

print_header "Step 3: Install Dependencies on Master Node"

print_status "Installing Python 3 and NLTK on master node..."

# Detect package manager
if command -v yum &> /dev/null; then
    PKG_MANAGER="yum"
    INSTALL_CMD="sudo yum install -y"
elif command -v apt-get &> /dev/null; then
    PKG_MANAGER="apt-get"
    INSTALL_CMD="sudo apt-get install -y"
else
    print_error "Unsupported package manager. Please install manually."
    exit 1
fi

print_status "Detected package manager: $PKG_MANAGER"

# Install Python 3
if command -v python3 &> /dev/null; then
    print_success "Python 3 already installed: $(python3 --version)"
else
    print_status "Installing Python 3..."
    $INSTALL_CMD python3 python3-pip
    print_success "Python 3 installed"
fi

# Install NLTK
if python3 -c "import nltk" 2>/dev/null; then
    print_success "NLTK already installed"
else
    print_status "Installing NLTK..."
    sudo pip3 install nltk
    print_success "NLTK installed"
fi

# Download NLTK data on master
print_status "Downloading NLTK data on master node..."
sudo python3 << 'EOF'
import nltk
import os
import sys

try:
    nltk_data_dir = '/root/nltk_data'
    os.makedirs(nltk_data_dir, exist_ok=True)

    print("Downloading stopwords...")
    nltk.download('stopwords', download_dir=nltk_data_dir, quiet=False)

    print("Downloading wordnet...")
    nltk.download('wordnet', download_dir=nltk_data_dir, quiet=False)

    print("Downloading punkt...")
    nltk.download('punkt', download_dir=nltk_data_dir, quiet=False)

    print("Downloading omw-1.4...")
    nltk.download('omw-1.4', download_dir=nltk_data_dir, quiet=False)

    print("NLTK data download completed!")
    sys.exit(0)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    print_success "NLTK data downloaded on master node"
else
    print_error "Failed to download NLTK data on master node"
    exit 1
fi

################################################################################
# Step 4: Install Dependencies on Worker Nodes
################################################################################

print_header "Step 4: Install Dependencies on Worker Nodes"

# Create installation script
INSTALL_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
set -e

NODE_NAME=$(hostname)
echo "[INFO] Installing on $NODE_NAME..."

# Detect package manager
if command -v yum &> /dev/null; then
    PKG_MANAGER="yum"
elif command -v apt-get &> /dev/null; then
    PKG_MANAGER="apt-get"
else
    echo "[ERROR] Unsupported package manager"
    exit 1
fi

# Install Python 3
if ! command -v python3 &> /dev/null; then
    echo "[INFO] Installing Python 3..."
    if [ "$PKG_MANAGER" = "yum" ]; then
        sudo yum install -y python3 python3-pip
    else
        sudo apt-get update
        sudo apt-get install -y python3 python3-pip
    fi
else
    echo "[OK] Python 3 already installed"
fi

# Install NLTK
if ! python3 -c "import nltk" 2>/dev/null; then
    echo "[INFO] Installing NLTK..."
    sudo pip3 install nltk
else
    echo "[OK] NLTK already installed"
fi

# Download NLTK data
echo "[INFO] Downloading NLTK data..."
sudo python3 << 'EOF'
import nltk
import os
import sys

try:
    nltk_data_dir = '/root/nltk_data'
    os.makedirs(nltk_data_dir, exist_ok=True)

    nltk.download('stopwords', download_dir=nltk_data_dir, quiet=True)
    nltk.download('wordnet', download_dir=nltk_data_dir, quiet=True)
    nltk.download('punkt', download_dir=nltk_data_dir, quiet=True)
    nltk.download('omw-1.4', download_dir=nltk_data_dir, quiet=True)

    print("NLTK data download completed!")
    sys.exit(0)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
EOF

echo "[SUCCESS] Installation completed on $NODE_NAME"
EOFSCRIPT
)

# Install on each worker node
FAILED_INSTALLS=()
for node in "${WORKER_NODES[@]}"; do
    print_status "Installing dependencies on $node..."

    if echo "$INSTALL_SCRIPT" | ssh "$node" "bash -s"; then
        print_success "Installation completed on $node"
    else
        print_error "Installation failed on $node"
        FAILED_INSTALLS+=("$node")
    fi
    echo ""
done

if [ ${#FAILED_INSTALLS[@]} -gt 0 ]; then
    print_error "Installation failed on the following nodes:"
    for node in "${FAILED_INSTALLS[@]}"; do
        echo "  - $node"
    done
    exit 1
fi

################################################################################
# Step 5: Verify Installation
################################################################################

print_header "Step 5: Verify Installation on All Nodes"

# Create verification script
VERIFY_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
NODE_NAME=$(hostname)

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python 3 not found on $NODE_NAME"
    exit 1
fi

# Check NLTK
if ! python3 -c "import nltk" 2>/dev/null; then
    echo "[ERROR] NLTK not found on $NODE_NAME"
    exit 1
fi

# Check NLTK data
python3 << 'EOF'
import os
import sys

nltk_data_dir = '/root/nltk_data'
required = ['stopwords', 'wordnet', 'punkt', 'omw-1.4']
missing = []

for item in required:
    corpus_path = os.path.join(nltk_data_dir, 'corpora', item)
    tokenizer_path = os.path.join(nltk_data_dir, 'tokenizers', item)
    if not os.path.exists(corpus_path) and not os.path.exists(tokenizer_path):
        missing.append(item)

if missing:
    print(f"[ERROR] Missing NLTK data: {missing}")
    sys.exit(1)
EOF

if [ $? -ne 0 ]; then
    exit 1
fi

# Test NLTK functionality
python3 << 'EOF'
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import sys

try:
    stop_words = set(stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    test_word = lemmatizer.lemmatize('running')
    if len(stop_words) > 0 and test_word == 'running':
        print("[OK] NLTK working correctly")
    else:
        print("[ERROR] NLTK not working properly")
        sys.exit(1)
except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    echo "[SUCCESS] $NODE_NAME is ready"
else
    echo "[ERROR] $NODE_NAME verification failed"
    exit 1
fi
EOFSCRIPT
)

# Verify master node
print_status "Verifying master node..."
if echo "$VERIFY_SCRIPT" | bash; then
    print_success "Master node: OK"
else
    print_error "Master node: FAILED"
fi
echo ""

# Verify all worker nodes
FAILED_VERIFICATIONS=()
for node in "${WORKER_NODES[@]}"; do
    print_status "Verifying $node..."

    if echo "$VERIFY_SCRIPT" | ssh "$node" "bash -s" 2>&1; then
        print_success "$node: OK"
    else
        print_error "$node: FAILED"
        FAILED_VERIFICATIONS+=("$node")
    fi
done

echo ""

if [ ${#FAILED_VERIFICATIONS[@]} -gt 0 ]; then
    print_error "Verification failed on the following nodes:"
    for node in "${FAILED_VERIFICATIONS[@]}"; do
        echo "  - $node"
    done
    exit 1
fi

################################################################################
# Summary
################################################################################

print_header "Installation Complete!"

print_success "All nodes are ready for MapReduce job execution"
echo ""
print_status "Summary:"
echo "  ✓ Master node: Ready"
for node in "${WORKER_NODES[@]}"; do
    echo "  ✓ $node: Ready"
done
echo ""
print_status "You can now run the MapReduce job:"
echo "  ./run_inverted_index_mapreduce.sh [NUM_REDUCERS]"
echo ""
print_status "Example:"
echo "  ./run_inverted_index_mapreduce.sh 2"
echo ""

exit 0
