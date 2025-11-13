# Inverted Index MapReduce - Execution Flow Overview

## Quick Start Guide

### Prerequisites Setup (One-Time on All Nodes)

```
┌─────────────────────────────────────────────────────────────────┐
│ MASTER NODE + ALL WORKER NODES                                  │
│                                                                  │
│ 1. Install Python 3                                             │
│    sudo yum install -y python3 python3-pip                      │
│                                                                  │
│ 2. Install NLTK                                                 │
│    sudo pip3 install nltk                                       │
│                                                                  │
│ 3. Download NLTK Data                                           │
│    sudo python3 -c "                                            │
│    import nltk                                                  │
│    nltk.download('stopwords', download_dir='/root/nltk_data')  │
│    nltk.download('wordnet', download_dir='/root/nltk_data')    │
│    nltk.download('punkt', download_dir='/root/nltk_data')      │
│    nltk.download('omw-1.4', download_dir='/root/nltk_data')    │
│    "                                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Complete Pipeline Flow

```
╔═══════════════════════════════════════════════════════════════════╗
║                    PHASE 1: DATA PREPARATION                      ║
╚═══════════════════════════════════════════════════════════════════╝

    Local Machine
    ┌──────────────────────┐
    │ python3              │
    │ donwload_file.py     │──┐
    │                      │  │
    │ - Reads CSV metadata │  │
    │ - Downloads books    │  │
    │ - Cleans text        │  │
    └──────────────────────┘  │
                              │ Upload via HDFS API
                              ↓
                       ┌─────────────┐
                       │    HDFS     │
                       │ /gutenberg- │
                       │   input/    │
                       │             │
                       │ • book1.txt │
                       │ • book2.txt │
                       │ • book3.txt │
                       └─────────────┘

╔═══════════════════════════════════════════════════════════════════╗
║              PHASE 2: MAPREDUCE JOB SUBMISSION                    ║
╚═══════════════════════════════════════════════════════════════════╝

    Master Node
    ┌──────────────────────────────────┐
    │ ./run_inverted_index_           │
    │       mapreduce.sh [reducers]    │
    └──────────────┬───────────────────┘
                   │
                   ↓
    ┌──────────────────────────────────┐
    │ 1. Validate Environment          │
    │    ✓ Check HADOOP_HOME           │
    │    ✓ Find streaming jar          │
    │    ✓ Verify scripts exist        │
    │    ✓ Test HDFS connection        │
    │                                   │
    │ 2. Verify NLTK Setup             │
    │    ✓ Check /root/nltk_data       │
    │                                   │
    │ 3. Prepare HDFS                  │
    │    ✓ Verify input exists         │
    │    ✓ Count documents             │
    │    ✓ Clean output dir            │
    │                                   │
    │ 4. Submit to YARN                │
    └──────────────┬───────────────────┘
                   │
                   ↓
    ┌──────────────────────────────────┐
    │  YARN ResourceManager            │
    │  - Creates ApplicationMaster     │
    │  - Allocates containers          │
    │  - Distributes tasks             │
    └──────────────┬───────────────────┘
                   │
                   ↓

╔═══════════════════════════════════════════════════════════════════╗
║                    PHASE 3: MAP PHASE                             ║
╚═══════════════════════════════════════════════════════════════════╝

Worker Node 1            Worker Node 2            Worker Node 3
┌────────────────┐      ┌────────────────┐       ┌────────────────┐
│ Container 1    │      │ Container 3    │       │ Container 5    │
│                │      │                │       │                │
│ Input:         │      │ Input:         │       │ Input:         │
│   book1.txt    │      │   book2.txt    │       │   book3.txt    │
│                │      │                │       │                │
│ Mapper Process:│      │ Mapper Process:│       │ Mapper Process:│
│ ┌────────────┐ │      │ ┌────────────┐ │       │ ┌────────────┐ │
│ │Load NLTK   │ │      │ │Load NLTK   │ │       │ │Load NLTK   │ │
│ │stopwords   │ │      │ │stopwords   │ │       │ │stopwords   │ │
│ └─────┬──────┘ │      │ └─────┬──────┘ │       │ └─────┬──────┘ │
│       ↓        │      │       ↓        │       │       ↓        │
│ ┌────────────┐ │      │ ┌────────────┐ │       │ ┌────────────┐ │
│ │Read text   │ │      │ │Read text   │ │       │ │Read text   │ │
│ │from STDIN  │ │      │ │from STDIN  │ │       │ │from STDIN  │ │
│ └─────┬──────┘ │      │ └─────┬──────┘ │       │ └─────┬──────┘ │
│       ↓        │      │       ↓        │       │       ↓        │
│ ┌────────────┐ │      │ ┌────────────┐ │       │ ┌────────────┐ │
│ │Lowercase   │ │      │ │Lowercase   │ │       │ │Lowercase   │ │
│ │Remove punct│ │      │ │Remove punct│ │       │ │Remove punct│ │
│ │Remove stops│ │      │ │Remove stops│ │       │ │Remove stops│ │
│ │Lemmatize   │ │      │ │Lemmatize   │ │       │ │Lemmatize   │ │
│ └─────┬──────┘ │      │ └─────┬──────┘ │       │ └─────┬──────┘ │
│       ↓        │      │       ↓        │       │       ↓        │
│ Output:        │      │ Output:        │       │ Output:        │
│ elephant→      │      │ elephant→      │       │ lion→          │
│   book1@5000   │      │   book2@4500   │       │   book3@3200   │
│ lion→          │      │ giraffe→       │       │ tiger→         │
│   book1@5000   │      │   book2@4500   │       │   book3@3200   │
│ zebra→         │      │ zebra→         │       │ elephant→      │
│   book1@5000   │      │   book2@4500   │       │   book3@3200   │
└────────┬───────┘      └────────┬───────┘       └────────┬───────┘
         │                       │                        │
         └───────────────────────┼────────────────────────┘
                                 ↓
                        Local Disk Buffer

╔═══════════════════════════════════════════════════════════════════╗
║              PHASE 4: SHUFFLE & SORT (Automatic)                  ║
╚═══════════════════════════════════════════════════════════════════╝

    YARN performs automatic shuffle & sort by key

    ┌──────────────────────────────────────────────────┐
    │ Partition by key hash:                           │
    │                                                   │
    │ elephant → [book1@5000, book2@4500, book3@3200] │
    │ giraffe  → [book2@4500]                         │
    │ lion     → [book1@5000, book3@3200]             │
    │ tiger    → [book3@3200]                         │
    │ zebra    → [book1@5000, book2@4500]             │
    │                                                   │
    │ Split into N partitions (N = number of reducers) │
    └───────────────────┬──────────────────────────────┘
                        │
         ┌──────────────┼──────────────┐
         ↓              ↓              ↓
    To Reducer 1   To Reducer 2   To Reducer 3

╔═══════════════════════════════════════════════════════════════════╗
║                   PHASE 5: REDUCE PHASE                           ║
╚═══════════════════════════════════════════════════════════════════╝

Worker Node 1            Worker Node 2
┌────────────────────┐   ┌────────────────────┐
│ Container 10       │   │ Container 11       │
│                    │   │                    │
│ Reducer 1 Process: │   │ Reducer 2 Process: │
│                    │   │                    │
│ Input (sorted):    │   │ Input (sorted):    │
│ elephant →         │   │ lion →             │
│   [book1@5000,     │   │   [book1@5000,     │
│    book2@4500,     │   │    book3@3200]     │
│    book3@3200]     │   │                    │
│                    │   │ tiger →            │
│ giraffe →          │   │   [book3@3200]     │
│   [book2@4500]     │   │                    │
│                    │   │ zebra →            │
│ ┌────────────────┐ │   │   [book1@5000,     │
│ │ For each word: │ │   │    book2@4500]     │
│ │ 1. Group docs  │ │   │                    │
│ │ 2. Filter:     │ │   │ ┌────────────────┐ │
│ │    Skip if in  │ │   │ │ For each word: │ │
│ │    ALL docs    │ │   │ │ 1. Group docs  │ │
│ │ 3. Sort by     │ │   │ │ 2. Filter      │ │
│ │    count DESC  │ │   │ │ 3. Sort        │ │
│ └────────┬───────┘ │   │ └────────┬───────┘ │
│          ↓         │   │          ↓         │
│ Output:            │   │ Output:            │
│ elephant→book1@    │   │ lion→book1@5000→   │
│   5000→book2@4500→ │   │   book3@3200       │
│   book3@3200       │   │                    │
│ giraffe→book2@4500 │   │ tiger→book3@3200   │
│                    │   │                    │
│ (Skip 'zebra' -    │   │ zebra→book1@5000→  │
│  filtered out)     │   │   book2@4500       │
└──────────┬─────────┘   └──────────┬─────────┘
           │                        │
           └────────────┬───────────┘
                        ↓
                  Write to HDFS

╔═══════════════════════════════════════════════════════════════════╗
║                  PHASE 6: OUTPUT TO HDFS                          ║
╚═══════════════════════════════════════════════════════════════════╝

                    ┌─────────────────┐
                    │      HDFS       │
                    │  /gutenberg-    │
                    │    output/      │
                    │                 │
                    │ • part-00000    │
                    │ • part-00001    │
                    │ • _SUCCESS      │
                    └─────────────────┘
                            │
                            ↓
    ┌──────────────────────────────────────────────┐
    │ Inverted Index Format:                       │
    │                                               │
    │ word<TAB>doc1@count<TAB>doc2@count<TAB>...  │
    │                                               │
    │ Examples:                                     │
    │ elephant<TAB>book1@5000<TAB>book2@4500<TAB>  │
    │         book3@3200                           │
    │ giraffe<TAB>book2@4500                       │
    │ lion<TAB>book1@5000<TAB>book3@3200          │
    │ tiger<TAB>book3@3200                        │
    │ zebra<TAB>book1@5000<TAB>book2@4500         │
    └──────────────────────────────────────────────┘

╔═══════════════════════════════════════════════════════════════════╗
║                  PHASE 7: VIEW RESULTS                            ║
╚═══════════════════════════════════════════════════════════════════╝

    Script automatically displays:
    ┌──────────────────────────────────────┐
    │ • Output file list with sizes        │
    │ • First 20 lines of results          │
    │ • Commands for further analysis      │
    └──────────────────────────────────────┘

    Manual commands:
    ┌──────────────────────────────────────────────────────────┐
    │ # View all results                                       │
    │ hadoop fs -cat /gutenberg-output/part-* | less          │
    │                                                           │
    │ # Download to local                                      │
    │ hadoop fs -get /gutenberg-output ./results              │
    │                                                           │
    │ # Count unique words                                     │
    │ hadoop fs -cat /gutenberg-output/part-* | wc -l         │
    │                                                           │
    │ # Search for specific word                               │
    │ hadoop fs -cat /gutenberg-output/part-* | grep "^lion"  │
    └──────────────────────────────────────────────────────────┘
```

---

## Critical Success Factors

### ✓ MUST HAVE on ALL Worker Nodes

```
┌─────────────────────────────────────────────────────────┐
│ Every worker node MUST have:                            │
│                                                          │
│ 1. Python 3 installed                                   │
│    Location: /usr/bin/python3                           │
│                                                          │
│ 2. NLTK library installed                               │
│    Install: sudo pip3 install nltk                      │
│                                                          │
│ 3. NLTK data downloaded                                 │
│    Location: /root/nltk_data/                           │
│    Required: stopwords, wordnet, punkt, omw-1.4         │
│                                                          │
│ WHY? Because Hadoop Streaming executes your Python      │
│ scripts on the worker nodes where data blocks reside.   │
│ If any dependency is missing on any node, tasks will    │
│ fail when they are scheduled on that node.              │
└─────────────────────────────────────────────────────────┘
```

---

## Data Flow Timeline

```
T=0     : Submit job via run_inverted_index_mapreduce.sh
T=0-5s  : Script validates environment, checks HDFS
T=5s    : Job submitted to YARN ResourceManager
T=5-10s : YARN creates ApplicationMaster, requests containers
T=10s   : Map tasks start on worker nodes
T=10-30s: Mappers read files, process text (NLTK operations)
T=30s   : Map phase completes, shuffle begins
T=30-40s: YARN shuffles and sorts intermediate data
T=40s   : Reduce tasks start
T=40-50s: Reducers process grouped keys, filter, sort
T=50s   : Reduce phase completes
T=50-55s: Final output written to HDFS
T=55s   : _SUCCESS file created, job complete
T=55s   : Script displays results summary
```

*Note: Times are approximate for small dataset (2-5 books). Larger datasets will take proportionally longer.*

---

## Resource Allocation Example

### 3-Node Cluster (1 Master + 2 Workers)

```
┌─────────────────────────────────────────────────────────────┐
│ Master Node                                                  │
│ • NameNode (HDFS metadata)                                  │
│ • ResourceManager (YARN scheduler)                          │
│ • Does NOT execute map/reduce tasks                         │
│ Resources: 4 CPU, 8GB RAM                                   │
└─────────────────────────────────────────────────────────────┘

┌──────────────────────────┐  ┌──────────────────────────────┐
│ Worker Node 1            │  │ Worker Node 2                │
│ • DataNode               │  │ • DataNode                   │
│ • NodeManager            │  │ • NodeManager                │
│                          │  │                              │
│ Available:               │  │ Available:                   │
│   2 CPU cores            │  │   2 CPU cores                │
│   4GB RAM                │  │   4GB RAM                    │
│                          │  │                              │
│ Can run:                 │  │ Can run:                     │
│   2 Map tasks (2GB)      │  │   2 Map tasks (2GB)          │
│   1 Reduce task (2GB)    │  │   1 Reduce task (2GB)        │
└──────────────────────────┘  └──────────────────────────────┘

Job Configuration:
• Total mappers: 4 (one per input file, or as split)
• Total reducers: 2 (one per worker node)
• Each mapper gets: ~2GB RAM
• Each reducer gets: ~2GB RAM
```

---

## Monitoring Commands

### Before Job Execution

```bash
# Verify all nodes are healthy
yarn node -list

# Check HDFS health
hdfs dfsadmin -report

# Verify input data
hdfs dfs -ls /gutenberg-input
hdfs dfs -count /gutenberg-input

# Test Python/NLTK on all workers
for node in worker1 worker2; do
    echo "Testing $node..."
    ssh $node "python3 -c 'import nltk; from nltk.corpus import stopwords; print(\"OK\")'"
done
```

### During Job Execution

```bash
# Watch running applications
watch -n 2 'yarn application -list'

# Get application ID
APP_ID=$(yarn application -list | grep "Inverted_Index" | awk '{print $1}')

# Monitor job progress
yarn application -status $APP_ID

# View live logs
yarn logs -applicationId $APP_ID -log_files stdout

# Check resource usage
yarn top
```

### After Job Completion

```bash
# Verify success
hdfs dfs -test -e /gutenberg-output/_SUCCESS && echo "SUCCESS" || echo "FAILED"

# View output
hdfs dfs -ls /gutenberg-output
hdfs dfs -cat /gutenberg-output/part-* | head -20

# Get statistics
hdfs dfs -cat /gutenberg-output/part-* | wc -l  # Count unique words

# Download results
hadoop fs -get /gutenberg-output ./inverted_index_results
```

---

## Troubleshooting Quick Reference

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Task fails with "ModuleNotFoundError: nltk" | NLTK not installed on worker | SSH to worker, run: `sudo pip3 install nltk` |
| Task fails with "Resource stopwords not found" | NLTK data missing on worker | SSH to worker, download NLTK data |
| "Output directory already exists" | Previous run didn't clean up | Run: `hdfs dfs -rm -r /gutenberg-output` |
| "Application failed... insufficient resources" | Not enough memory/cores | Reduce reducer count or adjust memory |
| All tasks fail immediately | Python3 not found | Install Python3 on all workers |
| Job hangs at map phase | Data skew or slow node | Check `yarn node -list`, investigate slow node |
| Empty output directory | Reducers filtered all words | Check total_map_tasks parameter |

---

## Performance Tips

### Optimal Configuration

| Cluster Size | Recommended Reducers | Memory per Task |
|--------------|---------------------|-----------------|
| 2 workers    | 2 reducers          | 2GB             |
| 4 workers    | 4 reducers          | 2-4GB           |
| 8 workers    | 8-12 reducers       | 4GB             |
| 16+ workers  | 16-24 reducers      | 4-8GB           |

### Run Commands

```bash
# Small cluster (2-4 workers)
./run_inverted_index_mapreduce.sh 2

# Medium cluster (5-8 workers)
./run_inverted_index_mapreduce.sh 8

# Large cluster (10+ workers)
./run_inverted_index_mapreduce.sh 16
```

---

## Success Checklist

Before running the job, ensure:

- [ ] Python 3 installed on ALL worker nodes
- [ ] NLTK installed on ALL worker nodes (`pip3 install nltk`)
- [ ] NLTK data at `/root/nltk_data` on ALL worker nodes
- [ ] Verified with: `ssh worker1 "python3 -c 'from nltk.corpus import stopwords'"`
- [ ] HDFS has data at `/gutenberg-input`
- [ ] YARN ResourceManager is running
- [ ] All NodeManagers are healthy (`yarn node -list`)
- [ ] Sufficient HDFS space available
- [ ] Scripts are executable (`chmod +x *.py *.sh`)

Then run:
```bash
./run_inverted_index_mapreduce.sh [NUM_REDUCERS]
```

Monitor with:
```bash
yarn application -list
```

View results:
```bash
hdfs dfs -cat /gutenberg-output/part-* | head -20
```

---

**End of Execution Flow Overview**
