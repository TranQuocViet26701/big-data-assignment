#!/usr/bin/python3

import sys
import re
import os

"""
High level of what the first mapper will do
MAP1:   [D_i]   -->     [term_k, URL_i@W_i]
        for each line in D_i
            extract term_k
        get number of terms of a document W_i
        get urls of documents
        emit term_k, URL_i@W_i
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

def read_input(file):
    file = file.read()
    for line in file.split('\n'):
        yield transform(line)

def main(separator='\t', second_sep='@'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    file_url = os.getenv('mapreduce_map_input_file')
    file_url = file_url if file_url else "random_filename"
    if '/' in file_url:
        file_url = file_url.split('/')[-1]
    document_words = set()
    for line in data:
        document_words.update(line.split())

    for word in document_words:
        print(f"{word}{separator}{file_url}{second_sep}{len(document_words)}")

if __name__ == "__main__":
    main()
