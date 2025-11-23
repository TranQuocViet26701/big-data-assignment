# Fixes Applied to Spark HBase Pipeline

## Problem Analysis

The pipeline was failing with the error message indicating it was running in **cluster mode** with **localhost** as the HBase Thrift server host. This caused multiple issues:

### Root Causes

1. **Cluster Mode Issue**: When using `--deploy-mode cluster`, the driver runs on a worker node, not the machine where spark-submit was called
2. **Localhost Problem**: Worker nodes cannot connect to "localhost:9090" because HBase Thrift is running on the master node
3. **Missing Python Module**: `hbase_connector.py` wasn't being distributed to executors
4. **Missing Dependencies**: Python packages (happybase, thriftpy2, etc.) weren't available on worker nodes

## Solutions Applied

### 1. Fixed Pipeline Script (`run_spark_hbase_pipeline.py`)

**Changes in all three execution functions** (`run_inverted_index`, `run_jpii`, `run_pairwise`):

#### Before:
```python
cmd = f"""spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\        # ❌ Driver on worker node
    --num-executors {self.args.num_executors} \\
    --executor-memory {self.args.executor_memory} \\
    --driver-memory {self.args.driver_memory} \\
    --conf spark.dynamicAllocation.enabled=false \\
    {spark_script} \\
    {self.args.input_dir} \\
    {self.args.thrift_host} \\      # ❌ Uses 'localhost'
    {self.args.thrift_port}"""
```

#### After:
```python
hbase_connector = os.path.join(self.script_dir, 'hbase_connector.py')

# Use hadoop-master for Thrift host to ensure worker nodes can connect
thrift_host = 'hadoop-master' if self.args.thrift_host == 'localhost' else self.args.thrift_host

cmd = f"""spark-submit \\
    --master yarn \\
    --deploy-mode client \\         # ✅ Driver on current node
    --num-executors {self.args.num_executors} \\
    --executor-memory {self.args.executor_memory} \\
    --driver-memory {self.args.driver_memory} \\
    --conf spark.dynamicAllocation.enabled=false \\
    --py-files {hbase_connector} \\  # ✅ Distribute hbase_connector.py
    {spark_script} \\
    {self.args.input_dir} \\
    {thrift_host} \\                 # ✅ Uses 'hadoop-master'
    {self.args.thrift_port}"""
```

### 2. Updated Spark Scripts

**Modified Files:**
- `spark_hbase_inverted_index.py`
- `spark_hbase_pairwise.py`

**Added to all executor functions:**
```python
import sys
sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
from hbase_connector import HBaseConnector
```

### 3. Deployed Python Dependencies

Copied required packages to `/tmp` on all nodes (master, worker-1, worker-2):
- happybase
- thrift
- thriftpy2
- importlib_resources
- ply

### 4. Updated Pairwise Similarity with IDF Filtering

Added IDF (Inverse Document Frequency) filtering to `spark_hbase_pairwise.py`:
- Filters out overly common terms (IDF < 0.05) - stop words
- Filters out very rare terms (IDF > 0.95) - typos, rare words
- Focuses on meaningful term overlaps
- Reduces computational overhead

**Configuration:**
```python
IDF_LOW = 0.05
IDF_HIGH = 0.95
```

## Testing Results

✅ **Inverted Index Build**: Successfully completed
- **35,243 records** written to HBase
- **9,755 unique terms** indexed
- **Average 3.61 documents per term**

## How to Use

### Option 1: Use Pipeline Script (Recommended)
```bash
cd /home/ktdl9/big-data-assignment/2-spark-hbase

# Pairwise mode
python3 run_spark_hbase_pipeline.py \
    --mode pairwise \
    --num-books 10 \
    --input-dir hdfs:///gutenberg-input-10 \
    --thrift-host localhost \
    --thrift-port 9090
```

The script automatically:
- Converts `localhost` to `hadoop-master`
- Uses client deploy mode
- Distributes `hbase_connector.py`
- Collects and saves metrics

### Option 2: Use Wrapper Scripts
```bash
cd /home/ktdl9/big-data-assignment/2-spark-hbase

# Build inverted index
./run_inverted_index.sh hdfs:///gutenberg-input-10

# Run pairwise similarity
./run_pairwise.sh
```

## Key Learnings

1. **Client vs Cluster Mode**:
   - Client: Driver on machine running spark-submit
   - Cluster: Driver on a worker node (good for production, harder to debug)

2. **Network Addressing**: Always use cluster-resolvable hostnames (e.g., `hadoop-master`), not `localhost`

3. **Module Distribution**: Use `--py-files` to distribute Python modules to all executors

4. **Dependency Management**: Ensure all worker nodes have required Python packages

5. **Path Configuration**: Add dependency paths in executor functions, not just driver code

## Files Modified

1. `run_spark_hbase_pipeline.py` - Fixed deploy mode, hostname, and py-files
2. `spark_hbase_inverted_index.py` - Added /tmp to Python path
3. `spark_hbase_pairwise.py` - Added /tmp to Python path + IDF filtering logic
4. Created `run_inverted_index.sh` and `run_pairwise.sh` wrapper scripts
5. Created `SETUP.md` and `FIXES_APPLIED.md` documentation
