#!/usr/bin/python
import sys
import re
import os

""" MAP:
Input:
- A customized inverted index [term_k, [URL_i@W_i]ord]
Output:
- A list of candidate pairs [URL_ij@W_i@W_j, 1]

High level of what the first mapper will do:
MAP:    for each term, elements in input_data:
            get the information of given query object,
            and derive its URLs, total term
            for the rest object sharing the same term:
                extract URL, total term
                emit candidate pair

"""
stop_words = {'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"}
def transform(content):
    # lowercase
    content = content.lower()
    # remove punctualtions
    content = re.sub(r'[^\w\s]', '', content)
    # remove stop words
    words = [w for w in content.split() if w not in stop_words]
    
    return ' '.join(words)

def read_input(file, separator='\t'):
    # get term_k\tURL_1@W_1\tURL_2@W_2
    for line in file:
        term, urls  = line.split(separator, 1)
        yield term, urls.split(separator)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    # get URL of query
    # file_url = os.getenv('mapreduce_map_input_file')
    # file_url = file_url if file_url else "random_filename"
    # get filename containing content of query from hadoop environment
    QUERY_FILE_NAME = os.getenv('q_from_user')

    try:
        with open(QUERY_FILE_NAME, 'r') as f:
            raw_query = f.read()
    except FileNotFoundError:
        print(f"[ERROR] Query file '{QUERY_FILE}' not found.", file=sys.stderr)
        return

    query_words = set(transform(raw_query if raw_query else '').split())
    query_url = os.path.basename(QUERY_FILE_NAME)

    for term, elements in data:
        if term not in query_words:
            continue

        urlq, wq = query_url, len(query_words)
        for element in elements:
            url, w = element.split('@')
            if url != urlq:
                if int(w) > int(wq):
                    print("{}-{}@{}@{}\t{}".format(url, urlq, int(w), int(wq), 1))
                else:
                    print("{}-{}@{}@{}\t{}".format(urlq, url, int(wq), int(w), 1))

if __name__ == "__main__":
    main()
