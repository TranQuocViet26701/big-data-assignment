from pyspark import SparkConf, SparkContext
import sys
import re
import os

# --------------------------
# Load stopwords (same as your mapper)
# --------------------------
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
    text = text.lower()
    text = re.sub(r"[^\w\s]", '', text)
    # remove stop words
    words = [w for w in text.split() if w not in stop_words]
    
    return ' '.join(words)

def mapper_phase2(term_row, bc_query_words, bc_query_url):
    """
    Input row: "term\turl1@w1\turl2@w2..."
    Output: [(pair_key, 1), ...]
    same logic as Hadoop mapper
    """

    line = term_row.strip()
    parts = line.split('\t')
    if len(parts) < 2:
        return []

    term = parts[0]
    urls = parts[1:]

    query_words = bc_query_words.value
    query_url = bc_query_url.value

    if term not in query_words:
        return []

    # weight of query = number of unique words
    wq = len(query_words)

    output = []
    for item in urls:
        url, w = item.split('@')
        if url != query_url:
            w, wq_int = int(w), int(wq)

            if w > wq_int:
                key = f"{url}-{query_url}@{w}@{wq_int}"
            else:
                key = f"{query_url}-{url}@{wq_int}@{w}"

            output.append((key, 1))

    return output

# ============================
# Phase 2 Reducer logic
# ============================

def reducer_jaccard(pair_key, counts):
    """
    Input: pair_key = "urlA-urlB@WA@WB"
           counts = list of 1's
    Output: (pair_key, sim)
    """
    total = sum(counts)

    pair, wA, wB = pair_key.split('@')
    wA, wB = int(wA), int(wB)

    sim = total / (wA + wB - total)

    return f"{pair}\t{sim}"

# ============================
# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: spark_phase2_jpii.py <phase1_output> <query_file> <output_dir>")
        sys.exit(1)

    inverted_index_output = sys.argv[1]
    query_file_path = sys.argv[2]
    output_dir = sys.argv[3]

    conf = SparkConf().setAppName("Spark-JPII")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile(inverted_index_output)

    # ---- LOAD QUERY FILE ----
    with open(query_file_path, "r") as f:
        query_raw = f.read()

    query_words = set(transform(query_raw).split())
    query_url = os.path.basename(query_file_path)

    # Broadcast to workers
    bc_query_words = sc.broadcast(query_words)
    bc_query_url = sc.broadcast(query_url)

    # MAP
    mapped = rdd.flatMap(
        lambda line: mapper_phase2(line, bc_query_words, bc_query_url)
    )

    # REDUCE
    reduced = (
        mapped
        .groupByKey()
        .map(lambda kv: reducer_jaccard(kv[0], kv[1]))
    )
    reduced = reduced.sortBy(lambda line: line.split("\t")[0])

    reduced.saveAsTextFile(output_dir)

    sc.stop()
