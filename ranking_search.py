import sys
import os
from collections import namedtuple

# Constants and Thresholds
TOP_K = 5
# Note: Layer 1 filter logic is now customizable based on the user's requirement: intersection >= query_count / 2

# Define structure for results (Includes f1_score)
Result = namedtuple('Result', ['ebook_name', 'intersection_count', 'query_count', 'ebook_count', 'jaccard_score', 'overlap_score', 'f1_score'])

def calculate_overlap_score(intersection, query_count, ebook_count):
    """Calculates the Overlap Coefficient: |A ∩ Q| / min(|A|, |Q|)"""
    min_count = min(query_count, ebook_count)
    if min_count == 0:
        return 0.0
    return intersection / min_count

def calculate_f1_score(intersection, query_count, ebook_count):
    """Calculates the F1-Score from intersection, query count, and ebook count."""
    
    # Recall = |A ∩ Q| / |Q|
    recall = intersection / query_count if query_count > 0 else 0.0
    
    # Precision = |A ∩ Q| / |A|
    precision = intersection / ebook_count if ebook_count > 0 else 0.0
    
    if precision + recall == 0:
        return 0.0
    
    # F1 = 2 * (Precision * Recall) / (Precision + Recall)
    return 2 * (precision * recall) / (precision + recall)

def load_and_process_files(input_folder):
    """
    Scans files, loads data, and applies Layer 1 (Custom Intersection Count filter).
    """
    
    all_candidates = []
    
    try:
        # Filter files starting with 'part-' (Hadoop Reducer output files)
        files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.startswith('part-')]
    except FileNotFoundError:
        print(f"Error: Input directory '{input_folder}' not found.", file=sys.stderr)
        return []

    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # Line format: Pair\tIntersection\tQueryCount\tEbookCount\tJaccardScore
                    parts = line.strip().split('\t')
                    
                    if len(parts) < 5: 
                        continue

                    # Data Extraction
                    pair = parts[0]
                    ebook_name = pair.split('-')[0] 
                    intersection = int(parts[1])
                    query_count = int(parts[2])
                    ebook_count = int(parts[3])
                    jaccard = float(parts[4])
                    
                    # Layer 1 Filter (Custom Logic: Intersection must be >= QueryCount/2)
                    if intersection < query_count/3:
                        continue
                        
                    # Calculate Overlap and F1 Score for Layer 3 Tie-breakers
                    overlap = calculate_overlap_score(intersection, query_count, ebook_count)
                    f1 = calculate_f1_score(intersection, query_count, ebook_count)
                    
                    all_candidates.append(
                        Result(
                            ebook_name=ebook_name,
                            intersection_count=intersection,
                            query_count=query_count,
                            ebook_count=ebook_count,
                            jaccard_score=jaccard,
                            overlap_score=overlap,
                            f1_score=f1 
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
    # 1. Jaccard Score (Layer 2 - Main Rank), rounded to 2 decimal places.
    # 2. F1-Score (Layer 3 - Tie-breaker 1).
    # 3. Overlap Score (Layer 3 - Tie-breaker 2).
    # 4. Intersection Count (Layer 3 - Tie-breaker 3).
    
    final_ranking = sorted(
        candidates,
        key=lambda x: (
            # 1. Primary Sort Key: Jaccard Score (ROUNDED TO 2 DECIMAL PLACES)
            round(x.jaccard_score, 2), 
            # 2. Tie-breaker 1: F1-Score
            round(x.f1_score, 4), 
            # 3. Tie-breaker 2: Overlap Score
            x.overlap_score, 
            # 4. Tie-breaker 3: Raw Intersection Count
            x.intersection_count 
        ),
        reverse=True # All criteria are sorted in descending order
    )
    
    return final_ranking[:TOP_K]


def print_results(top_results):
    """Prints the final results, showing only the ebook name, Jaccard, F1-Score, Overlap, and Rank."""
    print("\n## 🏆 Top Ebooks Ranking 🏆")
    print("--------------------------------------------------------------------------------------")
    print("{:<20} | {:<10} | {:<10} | {:<10} | {:<12} | {:<5}".format(
        "Ebook Name", "Jaccard", "F1-Score", "Overlap", "Shared Terms", "Rank"
    ))
    print("--------------------------------------------------------------------------------------")
    rank = 1
    for result in top_results:
        print("{:<20} | {:<10.2f} | {:<10.3f} | {:<10.3f} | {:<12} | {:<5}".format(
            result.ebook_name,
            result.jaccard_score,
            result.f1_score, 
            result.overlap_score,
            result.intersection_count, # NEW: Added Shared Terms
            rank
        ))
        rank += 1
    print("--------------------------------------------------------------------------------------")


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
