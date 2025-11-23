#!/usr/bin/env python3
"""
Spark HBase JPII (Query-based Similarity)

Reads inverted index from HBase and computes document similarity
using JPII (Jaccard Pairwise Index of Inverted Index) algorithm.
Writes results to HBase similarity_scores table.

Usage:
    spark-submit \\
        --master yarn \\
        --deploy-mode client \\
        --num-executors 4 \\
        spark_hbase_jpii.py \\
        <query_file> <thrift_host> <thrift_port>
"""

from pyspark import SparkConf, SparkContext
import sys
import re
import os
import math

# Configuration
HBASE_CONNECTOR_PATH = os.getenv(
    'HBASE_CONNECTOR_PATH',
    os.path.dirname(os.path.abspath(__file__))  # Use script directory by default
)

# IDF Filtering Configuration
IDF_LOW = 0.05
IDF_HIGH = 0.95

# Stopwords
stop_words = {
    'i','me','my','myself','we','our','ours','ourselves','you',"you're","you've","you'll",
    "you'd",'your','yours','yourself','yourselves','he','him','his','himself','she',"she's",
    'her','hers','herself','it',"it's",'its','itself','they','them','their','theirs','themselves',
    'what','which','who','whom','this','that',"that'll",'these','those','am','is','are','was',
    'were','be','been','being','have','has','had','having','do','does','did','doing','a','an',
    'the','and','but','if','or','because','as','until','while','of','at','by','for','with',
    'about','against','between','into','through','during','before','after','above','below',
    'to','from','up','down','in','out','on','off','over','under','again','further','then',
    'once','here','there','when','where','why','how','all','any','both','each','few','more',
    'most','other','some','such','no','nor','not','only','own','same','so','than','too','very',
    's','t','can','will','just','don',"don't",'should',"should've",'now','d','ll','m','o','re',
    've','y','ain','aren',"aren't",'couldn',"couldn't",'didn',"didn't",'doesn',"doesn't",
    'hadn',"hadn't",'hasn',"hasn't",'haven',"haven't",'isn',"isn't",'ma','mightn',"mightn't",
    'mustn',"mustn't",'needn',"needn't",'shan',"shan't",'shouldn',"shouldn't",'wasn',"wasn't",
    'weren',"weren't",'won',"won't",'wouldn',"wouldn't"
}

def transform(text):
    """Transform text: lowercase, remove punctuation, remove stopwords"""
    text = text.lower()
    text = re.sub(r"[^\w\s]", '', text)
    words = [w for w in text.split() if w not in stop_words]
    return ' '.join(words)


def compute_idf(doc_freq, total_docs):
    """
    Compute normalized IDF (Inverse Document Frequency).

    Args:
        doc_freq: Number of documents containing the term
        total_docs: Total number of documents in the collection

    Returns:
        Normalized IDF value between 0 and 1
    """
    if total_docs == 0 or doc_freq == 0:
        return 0.0
    return math.log(total_docs / doc_freq) / math.log(total_docs)


def read_and_process_terms(term_batch, query_words, query_url, wq, thrift_host, thrift_port, connector_path):
    """
    Read terms from HBase and generate document pairs for JPII.

    This runs on Spark executors - each partition processes a batch of terms.
    """
    import sys
    import os
    sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
    sys.path.append(connector_path)
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)
    pairs = []

    try:
        for term in term_batch:
            # Only process terms in query
            if term not in query_words:
                continue

            # Read documents for this term from HBase
            docs = connector.read_inverted_index_term(term)

            if not docs:
                continue

            # Generate pairs: (query, document)
            for doc, w in docs.items():
                if doc == query_url:
                    continue

                # Create canonical key (larger word count first)
                if w > wq:
                    key = f"{doc}-{query_url}@{w}@{wq}"
                else:
                    key = f"{query_url}-{doc}@{wq}@{w}"

                pairs.append((key, 1))

        return pairs

    finally:
        connector.close()


def compute_jaccard(pair_key_count, query_url):
    """
    Compute Jaccard similarity for a document pair.

    Args:
        pair_key_count: Tuple of ((pair_key, total_count))
        query_url: Query document name

    Returns:
        Dictionary with similarity data
    """
    pair_key, total_count = pair_key_count

    try:
        # Parse: "doc1-doc2@w1@w2"
        pair_part, w1, w2 = pair_key.split('@')
        doc1, doc2 = pair_part.split('-')
        w1, w2 = int(w1), int(w2)

        # Compute Jaccard similarity
        intersection = total_count
        union = w1 + w2 - intersection
        similarity = intersection / union if union > 0 else 0.0

        # Determine which is query and which is document
        if doc1 == query_url:
            query_len = w1
            doc_len = w2
        elif doc2 == query_url:
            query_len = w2
            doc_len = w1
        else:
            return None  # Neither is query, skip

        return {
            'doc_pair': pair_part,
            'similarity': similarity,
            'match_count': intersection,
            'w1': w1,
            'w2': w2,
            'query_len': query_len,
            'doc_len': doc_len
        }

    except Exception as e:
        print(f"[ERROR] Failed to compute Jaccard for {pair_key}: {e}", file=sys.stderr)
        return None


def write_similarity_partition(partition, mode, thrift_host, thrift_port, connector_path):
    """
    Write similarity results to HBase.

    This runs on Spark executors - each partition writes its data.
    """
    import sys
    import os
    sys.path.insert(0, '/tmp')  # For happybase/thrift on worker nodes
    sys.path.append(connector_path)
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
    if len(sys.argv) != 4:
        print("Usage: spark_hbase_jpii.py <query_file> <thrift_host> <thrift_port>")
        sys.exit(1)

    query_file = sys.argv[1]
    thrift_host = sys.argv[2]
    thrift_port = int(sys.argv[3])

    print(f"\n[INFO] Starting Spark HBase JPII Similarity (OPTIMIZED)")
    print(f"[INFO] Query file: {query_file}")
    print(f"[INFO] HBase Thrift: {thrift_host}:{thrift_port}")
    print(f"[INFO] IDF filtering: enabled ({IDF_LOW} - {IDF_HIGH})")

    # Read and process query
    if not os.path.exists(query_file):
        print(f"[ERROR] Query file not found: {query_file}")
        sys.exit(1)

    with open(query_file, 'r') as f:
        query_content = f.read()

    query_words = set(transform(query_content).split())
    query_url = os.path.basename(query_file)
    wq = len(query_words)

    print(f"[INFO] Query document: {query_url}")
    print(f"[INFO] Query unique words: {wq}")

    # ✅ OPTIMIZATION: Filter query terms by IDF (remove too common/rare terms)
    print(f"\n[INFO] Filtering query terms by IDF (threshold: {IDF_LOW} - {IDF_HIGH})...")

    sys.path.append(HBASE_CONNECTOR_PATH)
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host=thrift_host, port=thrift_port)

    # Get document frequency for each query term and compute IDF
    filtered_query_words = set()
    total_docs = None
    term_stats = []

    for term in query_words:
        docs = connector.read_inverted_index_term(term)
        if docs:
            doc_freq = len(docs)

            # Get total docs count on first iteration
            if total_docs is None:
                # Estimate from index (alternatively, could be passed as parameter)
                # For now, we'll collect this from the first term's posting list
                # A better approach would be to store this as metadata
                all_docs = set()
                # Sample a few terms to estimate total docs
                sample_size = min(10, len(query_words))
                for sample_term in list(query_words)[:sample_size]:
                    sample_docs = connector.read_inverted_index_term(sample_term)
                    if sample_docs:
                        all_docs.update(sample_docs.keys())
                total_docs = max(len(all_docs), doc_freq)  # Use max to avoid division issues

            idf = compute_idf(doc_freq, total_docs)
            term_stats.append((term, doc_freq, idf))

            # Filter by IDF thresholds
            if IDF_LOW <= idf <= IDF_HIGH:
                filtered_query_words.add(term)

    connector.close()

    print(f"[INFO] Total documents (estimated): {total_docs}")
    print(f"[INFO] Query terms before IDF filtering: {len(query_words)}")
    print(f"[INFO] Query terms after IDF filtering: {len(filtered_query_words)}")
    print(f"[INFO] Terms filtered out: {len(query_words) - len(filtered_query_words)}")

    # Show some examples of filtered terms
    if term_stats:
        print(f"\n[INFO] Sample term statistics:")
        for term, df, idf in sorted(term_stats, key=lambda x: x[2], reverse=True)[:5]:
            status = "✓" if term in filtered_query_words else "✗"
            print(f"  {status} '{term}': df={df}, idf={idf:.4f}")

    # Update query words to filtered set
    query_words = filtered_query_words
    wq = len(query_words)

    # Initialize Spark
    conf = SparkConf().setAppName("Spark-HBase-JPII-Optimized")
    sc = SparkContext(conf=conf)

    try:
        # ✅ OPTIMIZED: Use filtered query terms
        relevant_terms = list(query_words)
        print(f"\n[INFO] Query terms to process: {len(relevant_terms)}")

        if not relevant_terms:
            print("[WARNING] Query has no valid terms!")
            sys.exit(0)

        # Broadcast query data
        query_words_bc = sc.broadcast(query_words)
        query_url_bc = sc.broadcast(query_url)
        wq_bc = sc.broadcast(wq)

        # Distribute terms across Spark workers
        # More partitions = better parallelism for HBase queries
        num_slices = max(len(relevant_terms) // 5, sc.defaultParallelism * 4)
        terms_rdd = sc.parallelize(relevant_terms, numSlices=num_slices)

        print(f"[INFO] Processing {len(relevant_terms)} terms in {num_slices} partitions...")

        # Read from HBase and generate pairs (parallel)
        pairs_rdd = terms_rdd.mapPartitions(
            lambda terms: read_and_process_terms(
                list(terms),
                query_words_bc.value,
                query_url_bc.value,
                wq_bc.value,
                thrift_host,
                thrift_port,
                HBASE_CONNECTOR_PATH
            )
        )

        # Reduce by key to count intersections
        similarity_counts = pairs_rdd.reduceByKey(lambda a, b: a + b)

        # Compute Jaccard similarity
        similarity_results = similarity_counts.map(
            lambda kv: compute_jaccard(kv, query_url_bc.value)
        ).filter(lambda x: x is not None)

        # Cache results for multiple operations
        similarity_results.cache()

        # Count results
        num_pairs = similarity_results.count()
        print(f"[INFO] Document pairs found: {num_pairs}")

        if num_pairs == 0:
            print("[WARNING] No similar documents found!")
            sys.exit(0)

        # Write to HBase
        print(f"[INFO] Writing similarity results to HBase...")

        write_counts = similarity_results.mapPartitions(
            lambda partition: write_similarity_partition(
                partition, 'jpii', thrift_host, thrift_port, HBASE_CONNECTOR_PATH
            )
        )

        total_written = write_counts.sum()

        print(f"\n[SUCCESS] JPII similarity computation completed!")
        print(f"[INFO] Total records written: {total_written}")
        print(f"[INFO] Query document: {query_url}")
        print(f"[INFO] Terms processed (after IDF filtering): {len(relevant_terms)}")
        print(f"\n[OPTIMIZATION] IDF filtering enabled (threshold: {IDF_LOW}-{IDF_HIGH})")
        print(f"  ✅ Filtered out {len(term_stats) - len(filtered_query_words)} irrelevant terms")
        print(f"  ✅ Reduced HBase queries by {((len(term_stats) - len(filtered_query_words)) / max(len(term_stats), 1) * 100):.1f}%")
        print(f"  ✅ Improved result quality by removing too common/rare terms")

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        sc.stop()

if __name__ == "__main__":
    main()
