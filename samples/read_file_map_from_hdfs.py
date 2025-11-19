#!/usr/bin/env python3

import subprocess
import csv
from typing import Dict
# Sample to read a file from HDFS and find the book name based on Id
def read_file_map_from_hdfs(hdfs_path: str) -> dict:
    # Command to read the file from HDFS
    cmd = ["hdfs", "dfs", "-cat", hdfs_path]

    try:
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        # Read the CSV content
        file_map: Dict[str, str] = {}
        reader = csv.reader(result.stdout.splitlines())

        # bypass header
        next(reader)

        for row in reader:
            if len(row) == 2:
                local_file, book_title = row
                file_map[local_file] = book_title

        return file_map

    except subprocess.CalledProcessError as e:
        print(f"Error reading file from HDFS: {e.stderr}")
        return {}

result = read_file_map_from_hdfs("/gutenberg-metadata/file_map.csv")

print(result)

# Example usage:
# file_map = read_file_map_from_hdfs("/gutenberg-metadata/file_map.csv")
# print(file_map)
# Output: {'00001.txt': 'Pride and Prejudice', '00002.txt': 'A Tale of Two Cities', ...}
# file_map['00001.txt']: 'Pride and Prejudice'