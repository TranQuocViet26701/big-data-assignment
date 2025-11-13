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

import nltk
import os
# Support both system-wide and user-space NLTK data
nltk.data.path.append("/root/nltk_data")  # System-wide installation
nltk.data.path.append(os.path.expanduser("~/nltk_data"))  # User-space installation
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def transform(content):
    # lowercase
    content = content.lower()
    # remove punctualtions
    content = re.sub(r'[^\w\s]', '', content)
    # remove stop words
    words = [w for w in content.split() if w not in stop_words]
    # lemmatization
    lemmatized = [lemmatizer.lemmatize(w) for w in words]
    return ' '.join(lemmatized)

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
