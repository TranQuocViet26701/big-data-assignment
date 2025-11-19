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

MAX_DOC_PER_TERM = 50   # bạn muốn bao nhiêu thì chỉnh ở đây

def read_input(file, sep="\t"):
    for line in file:
        parts = line.rstrip("\n").split(sep)
        # parts = [term, url1@w1, url2@w2, ...]
        yield parts[1:]   

def main(sep="\t"):
    for docs in read_input(sys.stdin, sep):
        n = len(docs)

        # skip if out of threshold
        if n > MAX_DOC_PER_TERM:
            continue

        # parse docs only one time
        parsed = []
        append = parsed.append
        for d in docs:
            url, w = d.split("@", 1)
            append((url, w))

        # pairwise generate
        for i in range(n):
            url1, w1 = parsed[i]
            for j in range(i + 1, n):
                url2, w2 = parsed[j]

                # canonical ordering
                if url1 <= url2:
                    out = url1 + '-' + url2 + '@' + w1 + '@' + w2 + '\t1\n'
                else:
                    out = url2 + '-' + url1 + '@' + w2 + '@' + w1 + '\t1\n'

                sys.stdout.write(out)

if __name__ == "__main__":
    main()
