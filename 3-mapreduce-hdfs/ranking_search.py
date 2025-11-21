import sys
import os
from collections import namedtuple

# Constants
TOP_K = 5
options = "query"

# Define structure for results
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

def read_input_from_streaming(options):
    """Reads lines from STDIN (the input to the Reducer) and processes them."""
    all_candidates = []
    
    for line in sys.stdin:
        try:
            # Line format: Pair\tIntersection\tQueryCount\tEbookCount\tJaccardScore
            parts = line.strip().split('\t')
            
            if len(parts) < 5: 
                continue

            # Data Extraction
            intersection = int(parts[1])
            w1 = int(parts[2])
            w2 = int(parts[3])
            jaccard = float(parts[4])
            if options == "jpii":
                pair = parts[0]
                if pair.startswith("000"):
                    ebook_count = w1
                    query_count = w2
                    ebook_name = pair.split('-')[0]
                else:
                    ebook_count = w2
                    query_count = w1
                    ebook_name = pair.split('-')[1]
            else:
                ebook_name = parts[0]
            
            # Layer 1 Filter (Custom Logic: Intersection must be >= QueryCount/2)
            if intersection < query_count/10:
                continue
                
            # Calculate Overlap and F1 Score
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
            # In lỗi ra STDERR để theo dõi trong log Hadoop
            print(f"Skipping format error line: {line.strip()} - Error: {e}", file=sys.stderr)
        except IndexError as e:
            print(f"Skipping structure error line: {line.strip()} - Error: {e}", file=sys.stderr)
            
    return all_candidates


def rank_candidates(candidates):
    """Applies Layer 2 and Layer 3 (Primary Ranking and Tie-breaking)."""
    
    TOP_K = 5
    
    final_ranking = sorted(
        candidates,
        key=lambda x: (
            # 1. Primary Sort Key: Jaccard Score (ROUNDED TO 2 DECIMAL PLACES)
            round(x.jaccard_score, 2), 
            # 2. Tie-breaker 1: F1-Score (Using full precision)
            round(x.f1_score,3), 
            # 3. Tie-breaker 2: Overlap Score
            x.overlap_score, 
            # 4. Tie-breaker 3: Raw Intersection Count
            x.intersection_count 
        ),
        reverse=True
    )
    
    return final_ranking[:TOP_K]


def print_results(top_results):
    """Prints the final results to STDOUT (Hadoop Streaming output) and STDERR (monitoring)."""
    
    # Print header to STDERR for monitoring
    print("\n## 🏆 Top Ebooks Ranking 🏆", file=sys.stderr)
    print("--------------------------------------------------------------------------------------", file=sys.stderr)
    print("{:<5} | {:<30} | {:<10} | {:<10} | {:<10} | {:<12} ".format(
        "Rank", "Ebook Name", "Jaccard", "F1-Score", "Overlap", "Shared Terms"
    ), file=sys.stderr)
    print("--------------------------------------------------------------------------------------", file=sys.stderr)
    
    rank = 1
    for result in top_results:
        # Print the machine-readable output to STDOUT (Hadoop captures this)
        # We print EbookName\tRank\tJaccardScore (example format)
        print(f"{rank}\t{result.ebook_name}\t{result.jaccard_score:.2f}\t{result.f1_score:.3f}\t{result.overlap_score:.3f}\t{result.intersection_count}")
        rank += 1
    print("--------------------------------------------------------------------------------------", file=sys.stderr)


if __name__ == "__main__":
    
    # 1. Read all input data from STDIN (Input from Mapper/HDFS)
    options = os.getenv('options')
    candidate_list = read_input_from_streaming(options)
    
    if not candidate_list:
        print("No valid candidates found in Reducer input.", file=sys.stderr)
        sys.exit(0)
        
    # 2. Layer 2 & 3: Primary Ranking and Tie-breaking
    top_results = rank_candidates(candidate_list)
    
    # 3. Print results (STDOUT for Hadoop output, STDERR for monitoring)
    print_results(top_results)