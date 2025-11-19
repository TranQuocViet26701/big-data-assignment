#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
import sys
import os

""" REDUCE:
Input:
- A list of candidate pairs [URL_i-URL_j@W_i@W_j, 1]
Output:
- Final similarity scores [URL_i@URL_j@, SIM(D_i, D_j)]

High level of what the first mapper will do:
REDUCE: for each pair, group when group by pair:
            for each pair, value in group:
                val = val + value
            w_i, w_j = ExtractW(pair)
            sim = val / (w_i + w_j - val)
            Emit pair sim
"""
def read_mapper_2_output(file, separator='\t'):
    # get URL_i-URL_j@W_i@W_j\t1
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_2_output(sys.stdin, separator=separator)
    # query url same as mapper
    query_url = os.getenv('q_from_user')
    if query_url:
        query_url = os.path.basename(query_url)

    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            url_ij, w_i, w_j = current_word.split('@')
            url_i, url_j = url_ij.split('-')
            w_i = int(w_i)
            w_j = int(w_j)
            #sim = float(total_count) / (int(w_i) + int(w_j) - total_count)
      
            # intersection size = total_count
            match_count = total_count
            if query_url and (url_i == query_url or url_j == query_url):
                query_len = w_i if url_i == query_url else w_j
                doc_len   = w_j if url_i == query_url else w_i
                sim = match_count / (query_len + doc_len - match_count)
            else:
                # pairwise mode, tính Jaccard cho tất cả cặp
                sim = match_count / (w_i + w_j - match_count)
          
            print(f"{url_ij}{separator}{match_count}{separator}{w_i}{separator}{w_j}{separator}{sim}")
        except ValueError:
            pass

if __name__ == "__main__":
    main()
