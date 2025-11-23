from pyspark import SparkConf, SparkContext
import sys
import re
import os

# --------------------------
# Load stopwords (same as invert_index_mapper)
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

# --------------------------
# Phase 1 Mapper
# emit: (term, "URL@W")
# --------------------------
def mapper_phase1(file_with_content):
    filename, content = file_with_content
    fname = os.path.basename(filename)

    document_words = set()

    # giống MapReduce: split theo dòng → transform từng dòng
    for line in content.split("\n"):
        processed = transform(line)
        document_words.update(processed.split())

    W = len(document_words)

    for w in document_words:
        yield (w, f"{fname}@{W}")


# --------------------------
# PHASE 1 Reducer
# (term, list(URL@W))
# --------------------------
def reducer_phase1(term, values, total_docs):

    # ---- (1) Parse URL@W thành tuple như MapReduce ----
    parsed = []
    seen = set()   # ---- (3) Remove duplicates ----

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

    # ---- Common-term filter giống MR/Spark ----
    if len(parsed) == 1 or len(parsed) == total_docs:
        return None

    # ---- (2) Sort theo W giảm dần (MR logic) ----
    parsed.sort(key=lambda x: x[1], reverse=True)

    # build posting list
    postings = [f"{fname}@{w}" for fname, w in parsed]

    return f"{term}\t" + "\t".join(postings)

# ====================================================
# MAIN
# ====================================================
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark_phase1_inverted_index.py <input_dir> <output_file>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2]

    conf = SparkConf().setAppName("Spark-InvertedIndex")
    sc = SparkContext(conf=conf)

    # Read all files from folder
    rdd = sc.wholeTextFiles(input_dir).cache()

    total_docs = rdd.count()
    total_docs_bc = sc.broadcast(total_docs)

    # Apply mapper
    mapped = rdd.flatMap(mapper_phase1).sortByKey()

    # Group by term
    grouped = mapped.groupByKey().mapValues(list)

    # Apply reducer logic
    reduced = grouped.map(lambda kv: reducer_phase1(kv[0], kv[1], total_docs_bc.value)) \
                     .filter(lambda x: x is not None)
    # Sort theo term (key) giống MapReduce
    reduced = reduced.sortBy(lambda line: line.split("\t")[0])

    # Save result as a single file
    reduced.coalesce(1).saveAsTextFile(output_file)

    sc.stop()