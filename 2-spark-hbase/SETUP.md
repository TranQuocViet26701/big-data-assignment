# Spark HBase Setup Instructions

## Problem: Missing Python Dependencies on Worker Nodes

The worker nodes need `happybase` and its dependencies (`thriftpy2`, `thrift`, `importlib_resources`, `ply`) to connect to HBase Thrift server.

## Solutions

### Option 1: Install on All Nodes (Recommended for Production)

```bash
# On each worker node (hadoop-worker-1, hadoop-worker-2):
ssh hadoop-worker-1 "sudo apt-get update && sudo apt-get install -y python3-pip"
ssh hadoop-worker-1 "pip3 install --user happybase"

ssh hadoop-worker-2 "sudo apt-get update && sudo apt-get install -y python3-pip"
ssh hadoop-worker-2 "pip3 install --user happybase"
```

### Option 2: Manual Copy to /tmp (Quick Fix - Current Solution)

```bash
# Copy dependencies to worker nodes /tmp directory
for worker in hadoop-worker-1 hadoop-worker-2; do
    scp -r ~/.local/lib/python3.10/site-packages/happybase $worker:/tmp/
    scp -r ~/.local/lib/python3.10/site-packages/thrift $worker:/tmp/
    scp -r ~/.local/lib/python3.10/site-packages/thriftpy2 $worker:/tmp/
    scp -r ~/.local/lib/python3.10/site-packages/importlib_resources* $worker:/tmp/
    scp -r ~/.local/lib/python3.10/site-packages/ply $worker:/tmp/
done
```

The scripts already add `/tmp` to Python path with `sys.path.insert(0, '/tmp')`.

### Option 3: Use Conda/Virtual Environment with --archives (Best for Large Clusters)

Create a conda environment and distribute it as an archive - not implemented yet.

## Running the Jobs

### Inverted Index
```bash
cd /home/ktdl9/big-data-assignment/2-spark-hbase
./run_inverted_index.sh hdfs:///gutenberg-input-10
```

### Pairwise Similarity (with IDF Filtering)
```bash
cd /home/ktdl9/big-data-assignment/2-spark-hbase
./run_pairwise.sh
```

## Key Configuration Points

1. **Thrift Host**: Must use `hadoop-master` (not `localhost`) so worker nodes can connect
2. **Deploy Mode**: Using `client` mode (driver on current node, executors on workers)
3. **--py-files**: Distributes `hbase_connector.py` to all executors
4. **sys.path.insert(0, '/tmp')**: Added in all executor functions to find dependencies

## IDF Filtering

The pairwise similarity script now includes IDF (Inverse Document Frequency) filtering to:
- Remove overly common terms (IDF < 0.05) - stop words, common terms
- Remove very rare terms (IDF > 0.95) - typos, rare words
- Focus on meaningful term overlaps for better similarity computation
- Reduce computational overhead

Configuration in `spark_hbase_pairwise.py`:
```python
IDF_LOW = 0.05
IDF_HIGH = 0.95
```
