import sys
from collections import namedtuple
from pyspark import SparkConf, SparkContext

# Constants
TOP_K = 5

# Define structure for results
Result = namedtuple('Result', [
    'ebook_name', 'intersection_count', 'query_count',
    'ebook_count', 'jaccard_score', 'overlap_score', 'f1_score'
])

def calculate_overlap_score(intersection, query_count, ebook_count):
    """Calculate the Overlap Coefficient: |A ∩ Q| / min(|A|, |Q|)."""
    min_count = min(query_count, ebook_count)
    if min_count == 0:
        return 0.0
    return intersection / min_count

def calculate_f1_score(intersection, query_count, ebook_count):
    """Calculate the F1-Score from intersection, query count, and ebook count."""
    recall = intersection / query_count if query_count > 0 else 0.0
    precision = intersection / ebook_count if ebook_count > 0 else 0.0
    if precision + recall == 0:
        return 0.0
    return 2 * (precision * recall) / (precision + recall)

def parse_line(line, mode="jpii"):
    """
    Parse one line of input into a Result object.
    Expected format: Pair \t Intersection \t QueryCount \t EbookCount \t JaccardScore
    Example:
      00010.txt-my_query.txt   4991   9063   14570   0.2677...
      my_query.txt-00001.txt   7604   9063    8796   0.7414...
    """
    parts = line.strip().split('\t')
    if len(parts) < 5:
        return None
    try:
        intersection = int(parts[1])
        query_count = int(parts[2])
        ebook_count = int(parts[3])
        jaccard = float(parts[4])
    except ValueError:
        return None

    pair = parts[0]
    if mode == "jpii":
        if pair.startswith("000"):   # ebook on the left
            ebook_name = pair.split('-')[0]
        else:                        # ebook on the right
            ebook_name = pair.split('-')[1]
    else:
        ebook_name = pair

    # Filter: intersection must be >= query_count / 10
    if intersection < query_count / 10.0:
        return None

    overlap = calculate_overlap_score(intersection, query_count, ebook_count)
    f1 = calculate_f1_score(intersection, query_count, ebook_count)

    return Result(
        ebook_name=ebook_name,
        intersection_count=intersection,
        query_count=query_count,
        ebook_count=ebook_count,
        jaccard_score=jaccard,
        overlap_score=overlap,
        f1_score=f1
    )

def rank_candidates(candidates):
    """Rank candidates by Jaccard, F1, Overlap, and Intersection count."""
    return sorted(
        candidates,
        key=lambda x: (
            round(x.jaccard_score, 2),
            round(x.f1_score, 3),
            x.overlap_score,
            x.intersection_count
        ),
        reverse=True
    )[:TOP_K]

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: ranking_search.py <input_path> <output_path> [mode]", file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) >= 4 else "jpii"

    conf = SparkConf().setAppName("RankingSearch")
    sc = SparkContext(conf=conf)

    # Read input from HDFS
    lines = sc.textFile(input_path)

    # Parse each line into Result objects
    results = lines.map(lambda line: parse_line(line, mode)) \
                   .filter(lambda x: x is not None)

    candidates = results.collect()

    if not candidates:
        print("No valid candidates found.", file=sys.stderr)
        sc.stop()
        sys.exit(0)

    # Rank top K
    top_results = rank_candidates(candidates)

    # Prepare output lines
    output_lines = [
        f"{rank}\t{r.ebook_name}\t{r.jaccard_score:.2f}\t{r.f1_score:.3f}\t{r.overlap_score:.3f}\t{r.intersection_count}"
        for rank, r in enumerate(top_results, start=1)
    ]

    # Remove output path if it already exists
    try:
        hconf = sc._jsc.hadoopConfiguration()
        from py4j.java_gateway import java_import
        java_import(sc._jvm, "org.apache.hadoop.fs.Path")
        java_import(sc._jvm, "org.apache.hadoop.fs.FileSystem")
        fs = sc._jvm.FileSystem.get(hconf)
        path = sc._jvm.Path(output_path)
        if fs.exists(path):
            fs.delete(path, True)
    except Exception as e:
        print(f"Warning: could not remove existing output path: {e}", file=sys.stderr)

    # Save results to HDFS
    sc.parallelize(output_lines, 1).saveAsTextFile(output_path)

    sc.stop()