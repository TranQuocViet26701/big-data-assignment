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


def read_and_process_terms(term_batch, query_words, query_url, wq, thrift_host, thrift_port):
    """
    Read terms from HBase and generate document pairs for JPII.

    This runs on Spark executors - each partition processes a batch of terms.
    """
    import sys
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
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


def write_similarity_partition(partition, mode, thrift_host, thrift_port):
    """
    Write similarity results to HBase.

    This runs on Spark executors - each partition writes its data.
    """
    import sys
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
    if len(sys.argv) != 4:
        print("Usage: spark_hbase_jpii.py <query_file> <thrift_host> <thrift_port>")
        print("\nExample:")
        print("  spark-submit spark_hbase_jpii.py my_query.txt localhost 9090")
        sys.exit(1)

    query_file = sys.argv[1]
    thrift_host = sys.argv[2]
    thrift_port = int(sys.argv[3])

    print(f"\n[INFO] Starting Spark HBase JPII Similarity")
    print(f"[INFO] Query file: {query_file}")
    print(f"[INFO] HBase Thrift: {thrift_host}:{thrift_port}")
    print(f"[INFO] Mode: JPII (query-based)\n")

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
    print(f"[INFO] Query terms: {len(query_words)}")

    # Initialize Spark
    conf = SparkConf().setAppName("Spark-HBase-JPII")
    sc = SparkContext(conf=conf)

    try:
        # Get all terms from HBase inverted index
        # We need to do this from driver to get the full term list
        print(f"[INFO] Reading terms from HBase...")

        import sys as driver_sys
        driver_sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')
        from hbase_connector import HBaseConnector

        connector = HBaseConnector(host=thrift_host, port=thrift_port)
        all_terms = connector.get_all_terms()
        connector.close()

        print(f"[INFO] Total terms in index: {len(all_terms)}")

        # Filter to only query terms (optimization)
        relevant_terms = [t for t in all_terms if t in query_words]
        print(f"[INFO] Relevant terms (in query): {len(relevant_terms)}")

        if not relevant_terms:
            print("[WARNING] No query terms found in index!")
            sys.exit(0)

        # Broadcast query data
        query_words_bc = sc.broadcast(query_words)
        query_url_bc = sc.broadcast(query_url)
        wq_bc = sc.broadcast(wq)

        # Distribute terms across Spark workers
        num_slices = max(len(relevant_terms) // 10, sc.defaultParallelism * 4)
        terms_rdd = sc.parallelize(relevant_terms, numSlices=num_slices)

        print(f"[INFO] Processing terms in {num_slices} partitions...")

        # Read from HBase and generate pairs
        pairs_rdd = terms_rdd.mapPartitions(
            lambda terms: read_and_process_terms(
                list(terms),
                query_words_bc.value,
                query_url_bc.value,
                wq_bc.value,
                thrift_host,
                thrift_port
            )
        )

        # Reduce by key to count intersections
        similarity_counts = pairs_rdd.reduceByKey(lambda a, b: a + b)

        # Compute Jaccard similarity
        similarity_results = similarity_counts.map(
            lambda kv: compute_jaccard(kv, query_url_bc.value)
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
                partition, 'jpii', thrift_host, thrift_port
            )
        )

        total_written = write_counts.sum()

        print(f"\n[SUCCESS] JPII similarity computation completed!")
        print(f"[INFO] Total similarity records written: {total_written}")
        print(f"[INFO] Query document: {query_url}")
        print(f"[INFO] Similar documents found: {num_pairs}")

        # Show top results (read from HBase)
        print(f"\n[INFO] Top 10 similar documents:")
        from hbase_connector import HBaseConnector
        connector = HBaseConnector(host=thrift_host, port=thrift_port)
        top_results = connector.query_similarity(mode='jpii', document=query_url.replace('.txt', ''), limit=10)

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
