#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
import sys

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
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            url_ij, w_i, w_j = current_word.split('@')
            sim = float(total_count) / (int(w_i) + int(w_j) - total_count)
            print("{}{}{}".format(url_ij, separator, sim))
        except ValueError:
            pass

if __name__ == "__main__":
    main()
