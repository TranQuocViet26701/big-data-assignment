from pyspark.sql import SparkSession
from itertools import combinations
import sys
import math

# =============================
NUM_PARTITIONS = 4         # depends on number of cores/executors
IDF_LOW = 0.05
IDF_HIGH = 0.95
# =============================


# ----------------------------------------------------------
# Generate pairwise keys from a list of "doc@w" strings
# ----------------------------------------------------------
def gen_pairs_from_list(docs):
    docs = list(set(docs))  # deduplicate within term
    n = len(docs)

    out = []
    append = out.append

    for a, b in combinations(docs, 2):
        d1, w1 = a.split("@")
        d2, w2 = b.split("@")
        w1, w2 = int(w1), int(w2)

        # canonical ordering
        if d1 <= d2:
            key = f"{d1}-{d2}@{w1}@{w2}"
        else:
            key = f"{d2}-{d1}@{w2}@{w1}"

        append((key, 1))
    return out


# ----------------------------------------------------------
# Compute Jaccard score for a pair
# ----------------------------------------------------------
def compute_jaccard(pair_key, inter_count):
    try:
        pair_part, wA, wB = pair_key.split("@")
        wA, wB = int(wA), int(wB)

        union = wA + wB - inter_count
        score = inter_count / union if union > 0 else 0.0

        return f"{pair_part}\t{inter_count}\t{wA}\t{wB}\t{score}"
    except Exception:
        return None


# ----------------------------------------------------------
# Parse one line of term → doc list
# ----------------------------------------------------------
def mapper_line(line):
    parts = line.strip().split("\t")
    if len(parts) < 2:
        return []
    term = parts[0]
    docs = [p for p in parts[1:] if "@" in p]
    return [(term, docs)]


# ----------------------------------------------------------
# Partition-level pair generator (faster than flatMap)
# ----------------------------------------------------------
def partition_gen(iterator):
    """
    Input: iterator of (term, [url@w,...])
    Output: (pair_key, 1)
    """
    for term, docs in iterator:
        for pair in gen_pairs_from_list(docs):
            yield pair

def compute_term_idf(term_docs_rdd, total_docs):
    """
    term_docs_rdd: RDD of (term, [doc@w,...])
    total_docs: total number of documents (N)
    
    Returns: RDD of (term, idf)
    """
    term_df_rdd = term_docs_rdd.mapValues(lambda docs: len(docs))
    term_idf_rdd = term_df_rdd.mapValues(lambda df: math.log(total_docs / df) / math.log(total_docs))
    return term_idf_rdd

def filter_terms_by_idf(term_docs_rdd, term_idf_rdd, idf_low=0.05, idf_high=0.95):
    """
    Join term_docs with IDF values and filter by thresholds
    Returns: filtered (term, docs) RDD
    """
    filtered_rdd = term_docs_rdd.join(term_idf_rdd)\
                                .filter(lambda kv: idf_low <= kv[1][1] <= idf_high)\
                                .map(lambda kv: (kv[0], kv[1][0]))
    return filtered_rdd


# ==========================================================
#                       MAIN
# ==========================================================
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit spark_pairwise_idf.py <phase1_input> <output_dir>")
        sys.exit(1)

    phase1_input = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("SparkPairwiseJaccardIDF").getOrCreate()
    sc = spark.sparkContext

    # ------------------ Load input ------------------
    rdd = sc.textFile(phase1_input, minPartitions=NUM_PARTITIONS)

    # ------------------ Map Phase: group docs by term ------------------
    term_docs_rdd = rdd.flatMap(mapper_line)
    def create_combiner(v): return v
    def merge_value(c, v): c.extend(v); return c
    def merge_combiners(c1, c2): c1.extend(c2); return c

    combined = term_docs_rdd.combineByKey(
        create_combiner,
        merge_value,
        merge_combiners,
        numPartitions=NUM_PARTITIONS
    ).cache()  # cached because reused

    # ------------------ Filter overly common terms by IDF ------------------
    # ------------------ Compute IDF and filter ------------------
    total_docs = term_docs_rdd.flatMap(lambda kv: kv[1]).distinct().count()
    term_idf_rdd = compute_term_idf(term_docs_rdd, total_docs)
    filtered_rdd = filter_terms_by_idf(term_docs_rdd, term_idf_rdd, IDF_LOW, IDF_HIGH).cache()

    # ------------------ Combine docs per term ------------------
    def create_combiner(v): return v
    def merge_value(c, v): c.extend(v); return c
    def merge_combiners(c1, c2): c1.extend(c2); return c1

    combined = filtered_rdd.combineByKey(
        create_combiner,
        merge_value,
        merge_combiners,
        numPartitions=NUM_PARTITIONS
    ).cache()

    # ------------------ Pairwise generation ------------------
    pair_rdd = combined.mapPartitions(partition_gen)

    # ------------------ Reduce Phase: sum intersections ------------------
    reduced = pair_rdd.reduceByKey(lambda a, b: a + b)

    # ------------------ Compute Jaccard score ------------------
    jaccard_rdd = reduced.map(lambda kv: compute_jaccard(kv[0], kv[1])) \
                         .filter(lambda x: x is not None)

    # ------------------ Write output ------------------
    jaccard_rdd.saveAsTextFile(output_path)

    sc.stop()
