#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
from colorama import init, Fore, Style

init(autoreset=True)  # Automatically reset after each print

# os.system('apt install libdb5.3-dev')
# os.system('pip install gutenberg')
# os.system('pip install requests')

import pandas as pd
import requests
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen
from gutenberg.acquire import load_etext
from gutenberg.cleanup import strip_headers

# only removes funny tokens for English texts
def remove_funny_tokens(text):
    tokens = text.split()
    sample = ' '.join(' '.join(tokens).replace('xe2x80x9c', ' ').replace('xe2x80x9d', ' ')\
                                      .replace('xe2x80x94', ' ').replace('xe2x80x99', "'")\
                                      .replace('xe2x80x98', "'").split())
    return sample

# clean newlines, carriage returns and tabs
def clean_text(text):
    cleaned_listed_text = []
    listed_text = list(text)

    for iter in range(len(listed_text) - 1):
        if (listed_text[iter] == '\\' and listed_text[iter + 1] == 'n') or \
            (listed_text[iter] == 'n' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\' and listed_text[iter + 1] == 'r' or \
            (listed_text[iter] == 'r' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\' and listed_text[iter + 1] == 't' or \
            (listed_text[iter] == 't' and listed_text[iter - 1] == '\\'):
            continue
        elif listed_text[iter] == '\\':
            continue
        else:
            cleaned_listed_text.append(listed_text[iter])

    cleaned_text = ''.join([str(char) for char in cleaned_listed_text])
    cleaned_text = remove_funny_tokens(cleaned_text)

    return ''.join(cleaned_text)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Download Project Gutenberg books and upload to HDFS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Download 100 books to /gutenberg-input-100
  python3 donwload_file.py --num-books 100 --hdfs-dir /gutenberg-input-100

  # Download 200 books using custom CSV
  python3 donwload_file.py --csv custom_metadata.csv --num-books 200 --hdfs-dir /gutenberg-input-200

  # Download all books from CSV
  python3 donwload_file.py --csv gutenberg_metadata.csv --hdfs-dir /gutenberg-input-all
        '''
    )

    parser.add_argument(
        '--csv',
        type=str,
        default='gutenberg_metadata.csv',
        help='Path to CSV file containing book metadata (default: gutenberg_metadata.csv)'
    )

    parser.add_argument(
        '--num-books',
        type=int,
        default=None,
        help='Number of books to process from the CSV file (default: all books in CSV)'
    )

    parser.add_argument(
        '--hdfs-dir',
        type=str,
        default='/gutenberg-input',
        help='HDFS directory path for uploading books (default: /gutenberg-input)'
    )

    parser.add_argument(
        '--start-row',
        type=int,
        default=0,
        help='Starting row index in CSV file (default: 0)'
    )

    return parser.parse_args()

# Parse command-line arguments
args = parse_arguments()

# Read CSV file
print(Fore.CYAN + f"[INFO] Reading metadata from: {args.csv}")
try:
    df_metadata = pd.read_csv(args.csv)
except FileNotFoundError:
    print(Fore.RED + f"[ERROR] CSV file not found: {args.csv}")
    sys.exit(1)

# Limit number of books if specified
if args.num_books is not None:
    end_row = args.start_row + args.num_books
    df_metadata = df_metadata.iloc[args.start_row:end_row]
    print(Fore.CYAN + f"[INFO] Processing {len(df_metadata)} books (rows {args.start_row} to {end_row-1})")
else:
    if args.start_row > 0:
        df_metadata = df_metadata.iloc[args.start_row:]
    print(Fore.CYAN + f"[INFO] Processing all {len(df_metadata)} books from CSV")

print(Fore.CYAN + f"[INFO] Target HDFS directory: {args.hdfs_dir}")
print(Fore.CYAN + "=" * 70)

data = {'Author': None, 'Title': None, 'Link': None, 'ID': None, 'Bookshelf': None, 'Text': None}

rm_files = []

for key, row in df_metadata.iterrows():
    if data['Author'] == None:
        data['Author'] = [row['Author']]
    else:
        data['Author'].append(row['Author'])
    
    if data['Title'] == None:
        data['Title'] = [row['Title']]
    else:
        data['Title'].append(row['Title'])
    
    if data['Link'] == None:
        data['Link'] = [row['Link']]
    else:
        data['Link'].append(row['Link'])
    
    book_id = int(row['Link'].split('/')[-1])

    if data['ID'] == None:
        data['ID'] = [book_id]
    else:
        data['ID'].append(book_id)
    
    if data['Bookshelf'] == None:
        data['Bookshelf'] = [row['Bookshelf']]
    else:
        data['Bookshelf'].append(row['Bookshelf'])

    text = np.nan
    try:
        text = strip_headers(load_etext(etextno=book_id, 
                                        mirror='http://www.mirrorservice.org/sites/ftp.ibiblio.org/pub/docs/books/gutenberg/')).strip()
        text = ' '.join(' '.join(' '.join(text.split('\n')).split('\t')).split('\r'))
        text = ' '.join(text.split())
        text = clean_text(str(text))
    except:
        try: 
            page = requests.get(row['Link'])
            soup = BeautifulSoup(page.content, 'html.parser')
            text_link = 'http://www.gutenberg.org' + soup.find_all("a", string="Plain Text UTF-8")[0]['href']
            http_response_object = urlopen(text_link)

            text = strip_headers(str(http_response_object.read()))
            text = ' '.join(' '.join(' '.join(text.split('\n')).split('\t')).split('\r'))
            text = ' '.join(text.split())
            text = clean_text(str(text))
        except:
            print("Couldn't acquire text for " + row['Title'] + ' with ID ' + str(book_id) + '. Link: ' + row['Link'])


    local_file = f"{str(key + 1).zfill(5)}.txt"
    with open(local_file, "w") as f:
        f.write(' '.join(text.split(' ')))

    print(Fore.GREEN + f"[LOCAL] File {local_file} saved successfully! ({row['Title']})")

    # Construct the command
    print(Fore.GREEN + f"[LOCAL] Uploading {local_file} to HDFS path {args.hdfs_dir}...")
    cmd = ['hdfs', 'dfs', '-put', '-f', local_file, args.hdfs_dir]

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check result
    if result.returncode == 0:
        print(Fore.GREEN + f"[HDFS] File {local_file} successfully uploaded to {args.hdfs_dir}")
    else:
        print(Fore.RED + f"[HDFS] Error uploading file {local_file}: {result.stderr}")

    rm_files.append(local_file)
    print(Fore.LIGHTBLUE_EX + "===============================")
    
for rm_file in rm_files:
    os.remove(rm_file)
