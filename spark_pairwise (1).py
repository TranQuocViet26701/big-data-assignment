from pyspark.sql import SparkSession
from itertools import combinations
import sys
import time

# =============================
MAX_DOC_PER_TERM = 50  # loại bỏ term quá phổ biến
NUM_PARTITIONS = 4     # tùy số core/executor
# =============================

def gen_pairs_from_list(docs):
    """docs: list of strings 'doc@w'"""
    docs = list(set(docs))
    if len(docs) < 2 or len(docs) > MAX_DOC_PER_TERM:
        return []
    out = []
    for a, b in combinations(docs, 2):
        d1, w1 = a.split("@")
        d2, w2 = b.split("@")
        w1, w2 = int(w1), int(w2)
        if d1 <= d2:
            key = f"{d1}-{d2}@{w1}@{w2}"
        else:
            key = f"{d2}-{d1}@{w2}@{w1}"
        out.append((key, 1))
    return out

def compute_jaccard(pair_key, inter_count):
    try:
        pair_part, wA, wB = pair_key.split("@")
        wA, wB = int(wA), int(wB)
        union = wA + wB - inter_count
        score = inter_count / union if union != 0 else 0.0
        return f"{pair_part}\t{inter_count}\t{wA}\t{wB}\t{score:.8f}"
    except Exception:
        return None

def mapper_line(line):
    parts = line.strip().split("\t")
    if len(parts) < 2:
        return []
    term = parts[0]
    docs = [p for p in parts[1:] if "@" in p]
    return [(term, docs)]

if __name__=="__main__":
    if len(sys.argv)!=3:
        print("Usage: spark-submit spark_pairwise.py <phase1_input> <output_dir>")
        sys.exit(1)

    phase1_input = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("SparkPairwiseJaccard").getOrCreate()
    sc = spark.sparkContext
    start_total = time.time()

    rdd = sc.textFile(phase1_input, minPartitions=NUM_PARTITIONS)

    # ------------------ Combine docs per term ------------------
    term_docs_rdd = rdd.flatMap(mapper_line)
    def create_combiner(v): return v
    def merge_value(c, v): c.extend(v); return c
    def merge_combiners(c1, c2): c1.extend(c2); return c1
    combined = term_docs_rdd.combineByKey(create_combiner, 
                                          merge_value, 
                                          merge_combiners, 
                                          numPartitions=NUM_PARTITIONS).cache()

    # ------------------ Generate pairs ------------------
    pair_rdd = combined.flatMap(lambda kv: gen_pairs_from_list(kv[1]))
    reduced = pair_rdd.reduceByKey(lambda a,b: a+b)
   
    # ------------------ Compute Jaccard ------------------
    jaccard_rdd = reduced.map(lambda kv: compute_jaccard(kv[0], kv[1]))
                        .filter(lambda x: x is not None)

    # ------------------ Write output ------------------
    start = time.time()
    jaccard_rdd.coalesce(4).saveAsTextFile(output_path)
   
    sc.stop()
