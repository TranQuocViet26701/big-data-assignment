#!/usr/bin/env python3
"""
HBase Connector Module using HappyBase

Provides utility functions for connecting to HBase via Thrift server
and performing common operations for the inverted index and similarity pipeline.

Requirements:
    pip install happybase thrift

Usage:
    from hbase_connector import HBaseConnector

    connector = HBaseConnector(host='localhost', port=9090)
    connector.batch_write_inverted_index(data)
"""

import happybase
import time
from typing import Iterator, Dict, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HBaseConnector:
    """HBase connector using HappyBase client via Thrift server"""

    def __init__(self, host: str = 'localhost', port: int = 9090, timeout: int = 30000):
        """
        Initialize HBase connection via Thrift server.

        Args:
            host: HBase Thrift server hostname
            port: HBase Thrift server port (default: 9090)
            timeout: Connection timeout in milliseconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.connection = None
        self._connect()

    def _connect(self):
        """Establish connection to HBase via Thrift"""
        try:
            self.connection = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=self.timeout
            )
            logger.info(f"Connected to HBase Thrift server at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to HBase: {e}")
            raise

    def close(self):
        """Close HBase connection"""
        if self.connection:
            self.connection.close()
            logger.info("HBase connection closed")

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in HBase"""
        try:
            tables = [t.decode() for t in self.connection.tables()]
            return table_name in tables
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def get_row_count(self, table_name: str) -> int:
        """
        Get approximate row count for a table.
        Warning: This scans the entire table, can be slow for large tables.
        """
        try:
            table = self.connection.table(table_name)
            count = sum(1 for _ in table.scan())
            return count
        except Exception as e:
            logger.error(f"Error counting rows: {e}")
            return 0

    # =========================================================================
    # Inverted Index Operations
    # =========================================================================

    def batch_write_inverted_index(
        self,
        data: List[Tuple[str, str, int]],
        batch_size: int = 1000
    ) -> int:
        """
        Batch write to inverted_index table.

        Args:
            data: List of (term, document, word_count) tuples
            batch_size: Number of records per batch

        Returns:
            Number of records written

        HBase Schema:
            Row Key: term
            Column: docs:<document_name>
            Value: word_count
        """
        table = self.connection.table('inverted_index')
        count = 0

        try:
            with table.batch(batch_size=batch_size) as batch:
                for term, doc, word_count in data:
                    batch.put(
                        term.encode('utf-8'),
                        {f'docs:{doc}'.encode('utf-8'): str(word_count).encode('utf-8')}
                    )
                    count += 1

            logger.debug(f"Wrote {count} records to inverted_index")
            return count

        except Exception as e:
            logger.error(f"Error writing to inverted_index: {e}")
            raise

    def read_inverted_index_term(self, term: str) -> Dict[str, int]:
        """
        Read all documents for a specific term from inverted_index.

        Args:
            term: The term to look up

        Returns:
            Dictionary mapping document names to word counts
            Example: {'book1.txt': 123, 'book2.txt': 456}
        """
        table = self.connection.table('inverted_index')

        try:
            row = table.row(term.encode('utf-8'))

            # Parse HBase row: {b'docs:book1.txt': b'123', ...}
            result = {}
            for col_name, value in row.items():
                # Extract document name from column qualifier
                doc_name = col_name.decode('utf-8').replace('docs:', '')
                word_count = int(value.decode('utf-8'))
                result[doc_name] = word_count

            return result

        except Exception as e:
            logger.error(f"Error reading term '{term}': {e}")
            return {}

    def scan_inverted_index(
        self,
        limit: Optional[int] = None,
        row_prefix: Optional[str] = None
    ) -> Iterator[Tuple[str, Dict[str, int]]]:
        """
        Scan inverted_index table.

        Args:
            limit: Maximum number of rows to return
            row_prefix: Only return rows with this prefix

        Yields:
            Tuples of (term, {doc: count, ...})
        """
        table = self.connection.table('inverted_index')

        try:
            scan_kwargs = {}
            if limit:
                scan_kwargs['limit'] = limit
            if row_prefix:
                scan_kwargs['row_prefix'] = row_prefix.encode('utf-8')

            for key, data in table.scan(**scan_kwargs):
                term = key.decode('utf-8')
                docs = {}
                for col_name, value in data.items():
                    doc_name = col_name.decode('utf-8').replace('docs:', '')
                    word_count = int(value.decode('utf-8'))
                    docs[doc_name] = word_count

                yield (term, docs)

        except Exception as e:
            logger.error(f"Error scanning inverted_index: {e}")
            raise

    def get_all_terms(self) -> List[str]:
        """Get list of all terms in inverted index"""
        table = self.connection.table('inverted_index')

        try:
            terms = []
            for key, _ in table.scan():
                terms.append(key.decode('utf-8'))
            return terms
        except Exception as e:
            logger.error(f"Error getting all terms: {e}")
            return []

    # =========================================================================
    # Similarity Scores Operations
    # =========================================================================

    def batch_write_similarity(
        self,
        data: List[Dict],
        mode: str,
        batch_size: int = 1000
    ) -> int:
        """
        Batch write to similarity_scores table.

        Args:
            data: List of similarity result dictionaries
            mode: 'jpii' or 'pairwise'
            batch_size: Number of records per batch

        Returns:
            Number of records written

        HBase Schema:
            Row Key: <mode>:<doc1>-<doc2>
            Column Family: score
                - score:similarity (Jaccard score)
                - score:match_count (intersection)
                - score:w1 (word count doc1)
                - score:w2 (word count doc2)
            Column Family: meta
                - meta:timestamp
                - meta:mode
        """
        table = self.connection.table('similarity_scores')
        count = 0
        timestamp = str(int(time.time()))

        try:
            with table.batch(batch_size=batch_size) as batch:
                for record in data:
                    row_key = f"{mode}:{record['doc_pair']}"

                    batch.put(
                        row_key.encode('utf-8'),
                        {
                            b'score:similarity': str(record['similarity']).encode('utf-8'),
                            b'score:match_count': str(record['match_count']).encode('utf-8'),
                            b'score:w1': str(record['w1']).encode('utf-8'),
                            b'score:w2': str(record['w2']).encode('utf-8'),
                            b'meta:timestamp': timestamp.encode('utf-8'),
                            b'meta:mode': mode.encode('utf-8')
                        }
                    )
                    count += 1

            logger.debug(f"Wrote {count} similarity records")
            return count

        except Exception as e:
            logger.error(f"Error writing similarity scores: {e}")
            raise

    def query_similarity(
        self,
        mode: str,
        document: Optional[str] = None,
        threshold: float = 0.0,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Query similarity scores from HBase.

        Args:
            mode: 'jpii' or 'pairwise'
            document: Document name to filter (for JPII mode)
            threshold: Minimum similarity threshold
            limit: Maximum number of results

        Returns:
            List of similarity result dictionaries, sorted by similarity (descending)
        """
        table = self.connection.table('similarity_scores')
        results = []

        try:
            # Construct row prefix for scanning
            if document:
                row_prefix = f"{mode}:{document}-".encode('utf-8')
            else:
                row_prefix = f"{mode}:".encode('utf-8')

            # Scan table
            for key, data in table.scan(row_prefix=row_prefix):
                similarity = float(data.get(b'score:similarity', b'0').decode('utf-8'))

                # Apply threshold filter
                if similarity < threshold:
                    continue

                doc_pair = key.decode('utf-8').replace(f"{mode}:", '')

                result = {
                    'doc_pair': doc_pair,
                    'similarity': similarity,
                    'match_count': int(data.get(b'score:match_count', b'0').decode('utf-8')),
                    'w1': int(data.get(b'score:w1', b'0').decode('utf-8')),
                    'w2': int(data.get(b'score:w2', b'0').decode('utf-8')),
                    'timestamp': data.get(b'meta:timestamp', b'').decode('utf-8'),
                    'mode': data.get(b'meta:mode', b'').decode('utf-8')
                }

                results.append(result)

            # Sort by similarity (descending)
            results.sort(key=lambda x: x['similarity'], reverse=True)

            # Apply limit
            if limit:
                results = results[:limit]

            return results

        except Exception as e:
            logger.error(f"Error querying similarity scores: {e}")
            return []

    def truncate_table(self, table_name: str):
        """Truncate (clear all data from) a table"""
        try:
            logger.warning(f"Truncating table: {table_name}")
            # HappyBase doesn't have built-in truncate, need to use HBase shell
            # This is a placeholder - actual implementation would use subprocess
            raise NotImplementedError("Use clear_hbase_tables.sh script to truncate tables")
        except Exception as e:
            logger.error(f"Error truncating table: {e}")
            raise


def test_connection(host: str = 'localhost', port: int = 9090) -> bool:
    """
    Test HBase connection via Thrift server.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        connector = HBaseConnector(host=host, port=port)
        tables = [t.decode() for t in connector.connection.tables()]
        print(f"✓ Connected to HBase at {host}:{port}")
        print(f"✓ Available tables: {tables}")
        connector.close()
        return True
    except Exception as e:
        print(f"✗ Failed to connect to HBase: {e}")
        return False


if __name__ == '__main__':
    # Test connection
    print("Testing HBase connection...")
    test_connection()
