#!/bin/bash

################################################################################
# Dependency Installation WITHOUT SUDO (User-Space Installation)
#
# This script installs NLTK in user space without requiring sudo privileges.
# It's designed for users who don't have sudo access on worker nodes.
#
# Prerequisites:
#   - SSH access from master to all workers (passwordless recommended)
#   - Python 3 must already be installed (ask admin if not)
#   - pip3 must be available
#
# Usage: ./install_dependencies_no_sudo.sh
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
# Verify Configuration
################################################################################

print_header "User-Space Installation (No Sudo Required)"

print_status "Current user: $CURRENT_USER"
print_status "NLTK data will be installed in: $NLTK_DATA_DIR"
print_status "Number of worker nodes: ${#WORKER_NODES[@]}"
print_status "Worker nodes:"
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
    exit 1
fi

################################################################################
# Install on Master Node
################################################################################

print_header "Installing on Master Node"

# Check Python 3
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found. Please ask admin to install Python 3."
    exit 1
fi
print_success "Python 3: $(python3 --version)"

# Install NLTK in user space
print_status "Installing NLTK in user space (no sudo required)..."
if python3 -c "import nltk" 2>/dev/null; then
    print_success "NLTK already installed"
else
    pip3 install --user nltk
    print_success "NLTK installed"
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
print("Done!")
EOF

print_success "Master node setup complete"

################################################################################
# Install on Worker Nodes
################################################################################

print_header "Installing on Worker Nodes"

INSTALL_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
set -e

NODE_NAME=$(hostname)
USER_HOME="$HOME"
NLTK_DATA_DIR="$USER_HOME/nltk_data"

echo "[INFO] Installing on $NODE_NAME (no sudo)..."

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python 3 not found. Please ask admin to install."
    exit 1
fi
echo "[OK] Python 3 found: $(python3 --version)"

# Install NLTK in user space
if ! python3 -c "import nltk" 2>/dev/null; then
    echo "[INFO] Installing NLTK with pip3 --user..."
    pip3 install --user nltk
    echo "[OK] NLTK installed"
else
    echo "[OK] NLTK already installed"
fi

# Download NLTK data
echo "[INFO] Downloading NLTK data to $NLTK_DATA_DIR..."
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
        print_success "$node: Installation complete"
    else
        print_error "$node: Installation failed"
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

VERIFY_SCRIPT=$(cat << 'EOFSCRIPT'
#!/bin/bash
NODE_NAME=$(hostname)

if ! python3 -c "import nltk; from nltk.corpus import stopwords; print('[OK] NLTK working')" 2>/dev/null; then
    echo "[ERROR] NLTK not working on $NODE_NAME"
    exit 1
fi

echo "[SUCCESS] $NODE_NAME is ready"
EOFSCRIPT
)

# Verify master
print_status "Verifying master node..."
if echo "$VERIFY_SCRIPT" | bash; then
    print_success "Master node: OK"
fi

# Verify workers
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
# Update MapReduce Scripts
################################################################################

print_header "Updating MapReduce Scripts"

print_status "Updating mapper script to use user NLTK data..."

# Check if mapper exists
if [ ! -f "inverted_index_mapper.py" ]; then
    print_warning "inverted_index_mapper.py not found in current directory"
    print_status "Please update the NLTK data path manually in your mapper script:"
    echo "  Change: nltk.data.path.append('/root/nltk_data')"
    echo "  To:     nltk.data.path.append(os.path.expanduser('~/nltk_data'))"
else
    # Backup original
    cp inverted_index_mapper.py inverted_index_mapper.py.backup

    # Update NLTK data path
    sed -i.tmp "s|nltk.data.path.append(\"/root/nltk_data\")|import os\nnltk.data.path.append(os.path.expanduser('~/nltk_data'))|g" inverted_index_mapper.py
    rm -f inverted_index_mapper.py.tmp

    print_success "Mapper script updated (backup saved as inverted_index_mapper.py.backup)"
fi

################################################################################
# Summary
################################################################################

print_header "Installation Complete!"

print_success "All nodes ready for MapReduce job"
echo ""
print_status "Summary:"
echo "  ✓ NLTK installed in user space (no sudo required)"
echo "  ✓ NLTK data location: ~/nltk_data"
echo "  ✓ Master node: Ready"
for node in "${WORKER_NODES[@]}"; do
    echo "  ✓ $node: Ready"
done
echo ""
print_status "Next steps:"
echo "  1. Upload data: python3 donwload_file.py"
echo "  2. Run job: ./run_inverted_index_mapreduce.sh 2"
echo ""

exit 0
