#!/usr/bin/env python3
"""
Spark HBase Pairwise Similarity

Reads inverted index from HBase and computes all-pairs document similarity
using Pairwise algorithm (complete Jaccard similarity matrix).
Writes results to HBase similarity_scores table.

Usage:
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --num-executors 4 \
        spark_hbase_pairwise.py \
        <thrift_host> <thrift_port>
"""

from pyspark import SparkConf, SparkContext
import sys
import os
import math

# =============================
NUM_PARTITIONS = 4         # depends on number of cores/executors
IDF_LOW = 0.05
IDF_HIGH = 0.95
# =============================


def read_term_docs_batch(terms, thrift_host, thrift_port):
    """
    Read term-document mappings from HBase for a batch of terms.

    This runs on driver to collect all term-doc data for IDF computation.

    Returns:
        List of (term, {doc: count, ...}) tuples
    """
    import sys
    sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)
    term_docs = []

    try:
        for term in terms:
            docs = connector.read_inverted_index_term(term)
            if docs:
                term_docs.append((term, docs))
        return term_docs
    finally:
        connector.close()


def read_terms_partition(term_batch, thrift_host, thrift_port):
    """
    Read terms from HBase and generate document pairs for pairwise similarity.

    This runs on Spark executors - each partition processes a batch of terms.
    """
    import sys
    sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)
    pairs = []

    try:
        for term in term_batch:
            # Read documents for this term from HBase
            docs = connector.read_inverted_index_term(term)

            if not docs or len(docs) < 2:
                continue

            # Generate all pairs for this term
            doc_list = list(docs.items())  # [(doc1, w1), (doc2, w2), ...]

            for i in range(len(doc_list)):
                for j in range(i + 1, len(doc_list)):
                    doc1, w1 = doc_list[i]
                    doc2, w2 = doc_list[j]

                    # Create canonical key (lexicographic ordering)
                    if doc1 <= doc2:
                        key = f"{doc1}-{doc2}@{w1}@{w2}"
                    else:
                        key = f"{doc2}-{doc1}@{w2}@{w1}"

                    pairs.append((key, 1))

        return pairs

    finally:
        connector.close()


def compute_term_idf(term_docs_list, total_docs):
    """
    Compute IDF for each term.

    Args:
        term_docs_list: List of (term, {doc: count, ...}) tuples
        total_docs: Total number of unique documents

    Returns:
        Dictionary mapping term -> IDF value
    """
    term_idf = {}
    for term, docs in term_docs_list:
        df = len(docs)  # document frequency
        idf = math.log(total_docs / df) / math.log(total_docs)
        term_idf[term] = idf
    return term_idf


def filter_terms_by_idf(term_docs_list, term_idf, idf_low=0.05, idf_high=0.95):
    """
    Filter terms based on IDF thresholds.

    Args:
        term_docs_list: List of (term, {doc: count, ...}) tuples
        term_idf: Dictionary mapping term -> IDF value
        idf_low: Minimum IDF threshold
        idf_high: Maximum IDF threshold

    Returns:
        Filtered list of (term, docs) tuples
    """
    filtered = []
    for term, docs in term_docs_list:
        idf = term_idf.get(term, 0)
        if idf_low <= idf <= idf_high:
            filtered.append((term, docs))
    return filtered


def compute_jaccard(pair_key_count):
    """
    Compute Jaccard similarity for a document pair.

    Args:
        pair_key_count: Tuple of ((pair_key, total_count))

    Returns:
        Dictionary with similarity data
    """
    pair_key, total_count = pair_key_count

    try:
        # Parse: "doc1-doc2@w1@w2"
        pair_part, w1, w2 = pair_key.split('@')
        w1, w2 = int(w1), int(w2)

        # Compute Jaccard similarity
        intersection = total_count
        union = w1 + w2 - intersection
        similarity = intersection / union if union > 0 else 0.0

        return {
            'doc_pair': pair_part,
            'similarity': similarity,
            'match_count': intersection,
            'w1': w1,
            'w2': w2
        }

    except Exception as e:
        print(f"[ERROR] Failed to compute Jaccard for {pair_key}: {e}", file=sys.stderr)
        return None


def write_similarity_partition(partition, mode, thrift_host, thrift_port):
    """
    Write similarity results to HBase.

    This runs on Spark executors - each partition writes its data.
    """
    import sys
    sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)
    batch = []
    total_written = 0

    try:
        for record in partition:
            if record is None:
                continue

            batch.append(record)

            # Write in batches of 1000
            if len(batch) >= 1000:
                connector.batch_write_similarity(batch, mode)
                total_written += len(batch)
                batch = []

        # Write remaining
        if batch:
            connector.batch_write_similarity(batch, mode)
            total_written += len(batch)

        yield total_written

    except Exception as e:
        print(f"[ERROR] Failed to write similarity: {e}", file=sys.stderr)
        yield 0

    finally:
        connector.close()


def main():
    if len(sys.argv) != 3:
        print("Usage: spark_hbase_pairwise.py <thrift_host> <thrift_port>")
        print("\nExample:")
        print("  spark-submit spark_hbase_pairwise.py localhost 9090")
        sys.exit(1)

    thrift_host = sys.argv[1]
    thrift_port = int(sys.argv[2])

    print(f"\n[INFO] Starting Spark HBase Pairwise Similarity with IDF Filtering")
    print(f"[INFO] HBase Thrift: {thrift_host}:{thrift_port}")
    print(f"[INFO] Mode: Pairwise (all-pairs)")
    print(f"[INFO] IDF thresholds: {IDF_LOW} - {IDF_HIGH}\n")

    # Initialize Spark
    conf = SparkConf().setAppName("Spark-HBase-Pairwise")
    sc = SparkContext(conf=conf)

    try:
        # Get all terms from HBase inverted index
        print(f"[INFO] Reading terms from HBase...")

        import sys as driver_sys
        driver_sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
        from hbase_connector import HBaseConnector

        connector = HBaseConnector(host=thrift_host, port=thrift_port)
        all_terms = connector.get_all_terms()
        connector.close()

        print(f"[INFO] Total terms in index: {len(all_terms)}")

        if not all_terms:
            print("[WARNING] No terms found in index!")
            sys.exit(0)

        # Read all term-document mappings for IDF computation
        print(f"[INFO] Reading term-document mappings for IDF computation...")
        term_docs_list = read_term_docs_batch(all_terms, thrift_host, thrift_port)

        print(f"[INFO] Computing document count and IDF values...")
        # Compute total unique documents
        all_docs = set()
        for _, docs in term_docs_list:
            all_docs.update(docs.keys())
        total_docs = len(all_docs)
        print(f"[INFO] Total unique documents: {total_docs}")

        # Compute IDF for each term
        term_idf = compute_term_idf(term_docs_list, total_docs)

        # Filter terms by IDF thresholds
        print(f"[INFO] Filtering terms by IDF (threshold: {IDF_LOW} - {IDF_HIGH})...")
        filtered_term_docs = filter_terms_by_idf(term_docs_list, term_idf, IDF_LOW, IDF_HIGH)
        filtered_terms = [term for term, _ in filtered_term_docs]

        print(f"[INFO] Terms after IDF filtering: {len(filtered_terms)} (filtered out {len(all_terms) - len(filtered_terms)})")

        if not filtered_terms:
            print("[WARNING] No terms passed IDF filtering!")
            sys.exit(0)

        # Distribute filtered terms across Spark workers
        num_slices = max(len(filtered_terms) // 10, sc.defaultParallelism * 4)
        terms_rdd = sc.parallelize(filtered_terms, numSlices=num_slices)

        print(f"[INFO] Processing terms in {num_slices} partitions...")

        # Read from HBase and generate pairs
        pairs_rdd = terms_rdd.mapPartitions(
            lambda terms: read_terms_partition(
                list(terms),
                thrift_host,
                thrift_port
            )
        )

        # Reduce by key to count intersections
        similarity_counts = pairs_rdd.reduceByKey(lambda a, b: a + b)

        # Compute Jaccard similarity
        similarity_results = similarity_counts.map(
            compute_jaccard
        ).filter(lambda x: x is not None)

        # Count results before writing
        num_pairs = similarity_results.count()
        print(f"[INFO] Document pairs found: {num_pairs}")

        if num_pairs == 0:
            print("[WARNING] No similar documents found!")
            sys.exit(0)

        # Write to HBase
        print(f"[INFO] Writing similarity results to HBase...")

        write_counts = similarity_results.mapPartitions(
            lambda partition: write_similarity_partition(
                partition, 'pairwise', thrift_host, thrift_port
            )
        )

        total_written = write_counts.sum()

        print(f"\n[SUCCESS] Pairwise similarity computation completed!")
        print(f"[INFO] Total similarity records written: {total_written}")
        print(f"[INFO] Document pairs computed: {num_pairs}")

        # Show top results (read from HBase)
        print(f"\n[INFO] Top 10 most similar document pairs:")
        from hbase_connector import HBaseConnector
        connector = HBaseConnector(host=thrift_host, port=thrift_port)
        top_results = connector.query_similarity(mode='pairwise', limit=10)

        for i, result in enumerate(top_results, 1):
            print(f"  {i}. {result['doc_pair']}: {result['similarity']:.4f}")

        connector.close()

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        sc.stop()


if __name__ == "__main__":
    main()
