#!/usr/bin/python
import sys

"""
PAIRWISE MAPPER
---------------
Input  (from inverted index):
    term<TAB>url1@w1<TAB>url2@w2 ...

Output (one pair per shared term):
    urlA-urlB@wA@wB    1

"""

def read_input(file, sep="\t"):
    # get term_k\tURL_1@W_1\tURL_2@W_2
    for line in file:
        term, urls  = line.split(sep, 1)
        yield term, urls.split(sep)

def main(sep="\t"):
    data = read_input(sys.stdin)

    for term, docs in data:
        n = len(docs)
        for i in range(n):
            url1, w1 = docs[i].split('@')
            for j in range(i+1, n):
                url2, w2 = docs[j].split('@')

                # canonical key chỉ dựa vào alphabet doc id
                if url1 <= url2:
                    print("{}-{}@{}@{}\t{}".format(url1, url2, int(w1), int(w2), 1))
                else:
                    print("{}-{}@{}@{}\t{}".format(url2, url1, int(w2), int(w1), 1))


if __name__ == "__main__":
    main()
