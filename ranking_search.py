import sys
import os
from collections import namedtuple

# Constants and Thresholds
TOP_K = 5
INTERSECTION_THRESHOLD = 3 # Minimum shared term count required to pass Layer 1

# Define structure for results
Result = namedtuple('Result', ['ebook_name', 'intersection_count', 'query_count', 'ebook_count', 'jaccard_score', 'overlap_score'])

def calculate_overlap_score(intersection, query_count, ebook_count):
    """Calculates the Overlap Coefficient: |A ∩ Q| / min(|A|, |Q|)"""
    min_count = min(query_count, ebook_count)
    if min_count == 0:
        return 0.0
    return intersection / min_count

def load_and_process_files(input_folder):
    """
    Scans files, loads data, and applies Layer 1 (Raw Intersection Count filter).
    Adjusted to handle tab-separated input format.
    """
    
    all_candidates = []
    
    try:
        files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.startswith('part-')]
    except FileNotFoundError:
        print(f"Error: Input directory '{input_folder}' not found.", file=sys.stderr)
        return []

    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # New Line format: Pair\tIntersection\tQueryCount\tEbookCount\tJaccardScore
                    parts = line.strip().split('\t')
                    
                    if len(parts) < 5: # Need 5 parts now
                        continue

                    # Data Extraction (Adjusted to new tab-separated format)
                    pair = parts[0]
                    ebook_name = pair.split('-')[0] 
                    intersection = int(parts[1])
                    
                    # New format: Query count and Ebook count are in separate columns (parts[2], parts[3])
                    query_count = int(parts[2])
                    ebook_count = int(parts[3])
                    
                    jaccard = float(parts[4])
                    
                    # Layer 1 Filter
                    if intersection < query_count/2:
                        continue
                        
                    # Calculate Overlap for Layer 3 Tie-breaker
                    overlap = calculate_overlap_score(intersection, query_count, ebook_count)
                    
                    all_candidates.append(
                        Result(
                            ebook_name=ebook_name,
                            intersection_count=intersection,
                            query_count=query_count,
                            ebook_count=ebook_count,
                            jaccard_score=jaccard,
                            overlap_score=overlap
                        )
                    )

                except ValueError as e:
                    print(f"Skipping format error line in {file_path}: {line.strip()} - Error: {e}", file=sys.stderr)
                except IndexError as e:
                    print(f"Skipping structure error line in {file_path}: {line.strip()} - Error: {e}", file=sys.stderr)
                    
    return all_candidates


def rank_candidates(candidates):
    """Applies Layer 2 and Layer 3 (Primary Ranking and Tie-breaking)."""
    
    # Sorting criteria (in descending order):
    # 1. Jaccard Score (Layer 2 - Main Rank), rounded to 4 decimal places.
    # 2. Overlap Score (Layer 3 - Tie-breaker 1).
    # 3. Intersection Count (Layer 3 - Tie-breaker 2).
    
    final_ranking = sorted(
        candidates,
        key=lambda x: (
            # 1. Primary Sort Key: Jaccard Score (Rounded for tie check)
            round(x.jaccard_score, 4), 
            # 2. Tie-breaker 1: Overlap Score
            x.overlap_score, 
            # 3. Tie-breaker 2: Raw Intersection Count
            x.intersection_count 
        ),
        reverse=True # All criteria are sorted in descending order
    )
    
    return final_ranking[:TOP_K]


def print_results(top_results):
    """Prints the final results, showing only the ebook name, Jaccard, Overlap, and Rank."""
    print("\n## 🏆 Top Ebooks (Three-Layer Jaccard Ranking) 🏆")
    print("----------------------------------------------------------")
    print("{:<20} | {:<10} | {:<10} | {:<5}".format(
        "Ebook Name", "Jaccard", "Overlap", "Rank"
    ))
    print("----------------------------------------------------------")
    
    rank = 1
    for result in top_results:
        print("{:<20} | {:<10.4f} | {:<10.4f} | {:<5}".format(
            result.ebook_name,
            result.jaccard_score,
            result.overlap_score,
            rank
        ))
        rank += 1
    print("----------------------------------------------------------")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python ranker.py <input_folder_path>", file=sys.stderr)
        sys.exit(1)
        
    input_path = sys.argv[1]
    
    # 1. Layer 1 (Intersection Count Filtering) + Data Loading
    candidate_list = load_and_process_files(input_path)
    
    if not candidate_list:
        print("No valid candidates found after Layer 1 filter.", file=sys.stderr)
        sys.exit(0)
        
    # 2. Layer 2 & 3: Primary Ranking and Tie-breaking
    top_results = rank_candidates(candidate_list)
    
    # 3. Print results
    print_results(top_results)