#!/usr/bin/env python3
"""
Spark HBase Inverted Index Builder

Builds inverted index from documents in HDFS and writes directly to HBase
using HappyBase client via Thrift server.

Usage:
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --num-executors 4 \\
        --executor-memory 4G \\
        spark_hbase_inverted_index.py \\
        <hdfs_input_dir> <thrift_host> <thrift_port>

Example:
    spark-submit spark_hbase_inverted_index.py \\
        hdfs:///gutenberg-input-100 localhost 9090
"""

from pyspark import SparkConf, SparkContext
import sys
import re
import os

# Stopwords (same as HDFS version)
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


def mapper_phase1(file_with_content):
    """
    Map phase: Extract (term, document@word_count) pairs from each document.

    Args:
        file_with_content: Tuple of (filename, content)

    Yields:
        (term, "filename@word_count") tuples
    """
    filename, content = file_with_content
    fname = os.path.basename(filename)

    document_words = set()

    # Process each line
    for line in content.split("\n"):
        processed = transform(line)
        document_words.update(processed.split())

    W = len(document_words)

    # Emit (term, filename@word_count)
    for w in document_words:
        yield (w, f"{fname}@{W}")


def reducer_phase1(term, values, total_docs):
    """
    Reduce phase: Aggregate documents for each term and filter.

    Args:
        term: The word/term
        values: List of "filename@word_count" strings
        total_docs: Total number of documents

    Returns:
        List of (term, document, word_count) tuples for HBase writing
        None if term should be filtered out
    """
    # Parse and deduplicate
    parsed = []
    seen = set()

    for entry in values:
        if entry in seen:
            continue
        seen.add(entry)

        if '@' not in entry:
            continue

        fname, w_str = entry.rsplit('@', 1)

        try:
            w_int = int(w_str)
        except:
            continue

        parsed.append((fname, w_int))

    # Filter: Skip terms in only 1 document or ALL documents
    if len(parsed) == 1 or len(parsed) == total_docs:
        return []

    # Sort by word count (descending)
    parsed.sort(key=lambda x: x[1], reverse=True)

    # Return list of (term, document, word_count) for HBase
    result = []
    for fname, w in parsed:
        result.append((term, fname, w))

    return result


def write_partition_to_hbase(partition, thrift_host, thrift_port):
    """
    Write a partition of data to HBase using HappyBase.

    This function is called once per partition on each executor.
    It creates its own HBase connection for the partition.

    Args:
        partition: Iterator of (term, document, word_count) tuples
        thrift_host: HBase Thrift server hostname
        thrift_port: HBase Thrift server port

    Yields:
        Number of records written (for counting)
    """
    import sys
    sys.path.append('/home/ktdl9/big-data-assignment/2-spark-hbase')

    from hbase_connector import HBaseConnector

    connector = None
    batch_data = []
    total_written = 0

    try:
        # Create HBase connection for this partition
        connector = HBaseConnector(host=thrift_host, port=thrift_port)

        # Batch write records
        for record in partition:
            term, doc, count = record
            batch_data.append((term, doc, count))

            # Write in batches of 1000
            if len(batch_data) >= 1000:
                connector.batch_write_inverted_index(batch_data)
                total_written += len(batch_data)
                batch_data = []

        # Write remaining records
        if batch_data:
            connector.batch_write_inverted_index(batch_data)
            total_written += len(batch_data)

        yield total_written

    except Exception as e:
        print(f"[ERROR] Failed to write partition to HBase: {e}", file=sys.stderr)
        yield 0

    finally:
        if connector:
            connector.close()


def main():
    if len(sys.argv) != 4:
        print("Usage: spark_hbase_inverted_index.py <input_dir> <thrift_host> <thrift_port>")
        print("\nExample:")
        print("  spark-submit spark_hbase_inverted_index.py hdfs:///gutenberg-input-100 localhost 9090")
        sys.exit(1)

    input_dir = sys.argv[1]
    thrift_host = sys.argv[2]
    thrift_port = int(sys.argv[3])

    print(f"\n[INFO] Starting Spark HBase Inverted Index Builder")
    print(f"[INFO] Input: {input_dir}")
    print(f"[INFO] HBase Thrift: {thrift_host}:{thrift_port}")
    print(f"[INFO] Target HBase table: inverted_index\n")

    # Initialize Spark
    conf = SparkConf().setAppName("Spark-HBase-InvertedIndex")
    sc = SparkContext(conf=conf)

    try:
        # Read documents from HDFS
        rdd = sc.wholeTextFiles(input_dir).cache()

        total_docs = rdd.count()
        print(f"[INFO] Total documents: {total_docs}")

        total_docs_bc = sc.broadcast(total_docs)

        # Map: Extract terms and document metadata
        mapped = rdd.flatMap(mapper_phase1)

        # Group by term
        grouped = mapped.groupByKey().mapValues(list)

        # Reduce: Aggregate and filter
        reduced = grouped.flatMap(
            lambda kv: reducer_phase1(kv[0], kv[1], total_docs_bc.value)
        )

        # Count unique terms
        num_terms = reduced.map(lambda x: x[0]).distinct().count()
        print(f"[INFO] Unique terms (after filtering): {num_terms}")

        # Write to HBase using mapPartitions
        print(f"[INFO] Writing to HBase table: inverted_index")

        write_counts = reduced.mapPartitions(
            lambda partition: write_partition_to_hbase(partition, thrift_host, thrift_port)
        )

        total_written = write_counts.sum()

        print(f"\n[SUCCESS] Inverted index built successfully!")
        print(f"[INFO] Total records written to HBase: {total_written}")
        print(f"[INFO] Unique terms: {num_terms}")
        print(f"[INFO] Average documents per term: {total_written / num_terms if num_terms > 0 else 0:.2f}")

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        sc.stop()


if __name__ == "__main__":
    main()
