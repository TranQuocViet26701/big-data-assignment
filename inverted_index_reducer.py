#!/usr/bin/python3

from itertools import groupby
from operator import itemgetter
import sys
import os

"""
High level of what the first reducer will do
REDUCE1:    [term_k, URL_i@W_i] --> [term_k, [URL_i@W_i] ordered]
            for each term, group when group by term:
                if group length != total number of documents
                    and group length != 1
                    then emit term, sorted group
"""

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t', second_sep='@'):
    data = read_mapper_output(sys.stdin, separator=separator)
    total_map = os.getenv('total_map_tasks')
    try:
        total_map = int(total_map.strip())
    except:
        total_map = 0
    for current_word, group in groupby(data, itemgetter(0)):
        uacs = [uc for _, uc in group]
        uacs = [(url, int(count)) for url, count in read_mapper_output(uacs, second_sep)]
        # lonely word or common word
        if len(uacs) == 1 or len(uacs) == total_map:
            continue
        sorted_uacs = ["{}{}{}".format(url, second_sep, count) \
                       for url, count in sorted(uacs, key=itemgetter(1), reverse=True)]
        print("{}{}{}".format(current_word, separator, separator.join(sorted_uacs)))

if __name__ == "__main__":
    main()
