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
    for line in file:
        parts = line.rstrip("\n").split(sep)
        yield parts[1:]  # chỉ lấy danh sách url@w

def main(sep="\t"):
    for docs in read_input(sys.stdin, sep):
        n = len(docs)

        # parse url@w → (url, w)
        parsed = []
        append_parsed = parsed.append
        for d in docs:
            url, w = d.split("@", 1)
            append_parsed((url, w))

        # build output lines before print
        out_lines = []
        append_out = out_lines.append

        for i in range(n):
            url1, w1 = parsed[i]
            for j in range(i+1, n):
                url2, w2 = parsed[j]

                if url1 <= url2:
                    append_out(f"{url1}-{url2}@{w1}@{w2}\t1")
                else:
                    append_out(f"{url2}-{url1}@{w2}@{w1}\t1")

        # print 1 lần duy nhất
        if out_lines:
            print("\n".join(out_lines))

if __name__ == "__main__":
    main()

