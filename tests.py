import pytest
import log_cruncher

# --- Error Handling Tests ---

def test_csv_file_not_found_raises_oserror():
    """Ensure Rust I/O errors properly map to Python OSErrors for CSV."""
    with pytest.raises(OSError):
        log_cruncher.process_csv_aggregate("this_file_does_not_exist.csv")

def test_jsonl_file_not_found_raises_oserror():
    """Ensure Rust I/O errors properly map to Python OSErrors for JSONL."""
    with pytest.raises(OSError):
        log_cruncher.process_json_aggregate("this_file_does_not_exist.jsonl")

def test_unsupported_format_raises_error():
    """Ensure BatchLogProcessor rejects bad extensions."""
    with pytest.raises(ValueError, match="Unsupported file format"):
        log_cruncher.BatchLogProcessor("test_logs.txt", None, 100)

# --- Aggregation Tests ---

def test_csv_aggregation_valid():
    """Verify that CSV aggregation successfully returns a dictionary of counts."""
    results = log_cruncher.process_csv_aggregate("test_logs.csv")
    
    assert isinstance(results, dict)
    assert results.get("INFO") == 4
    assert results.get("ERROR") == 2
    assert sum(results.values()) == 10

def test_jsonl_aggregation_valid():
    """Verify that JSONL aggregation successfully returns a dictionary of counts."""
    results = log_cruncher.process_json_aggregate("test_logs.jsonl")
    
    assert isinstance(results, dict)
    assert results.get("INFO") == 4
    assert results.get("ERROR") == 2
    assert sum(results.values()) == 10

# --- Batch Processor Tests ---

def test_batch_processor_chunking_logic():
    """
    Test that chunk limits are strictly respected.
    test_logs.csv has 10 rows. Chunk size 3 should yield 4 chunks (3, 3, 3, 1).
    """
    processor = log_cruncher.BatchLogProcessor("test_logs.csv", None, 3)
    
    chunks = list(processor)
    
    assert len(chunks) == 4, "Should have produced exactly 4 chunks"
    assert len(chunks[0]) == 3
    assert len(chunks[1]) == 3
    assert len(chunks[2]) == 3
    assert len(chunks[3]) == 1

def test_batch_processor_filtering():
    """Ensure target_level filtering evaluates strictly on the Rust side before chunking."""
    # test_logs.jsonl contains exactly 2 ERROR entries.
    processor = log_cruncher.BatchLogProcessor("test_logs.jsonl", "ERROR", 5)
    
    chunks = list(processor)
    
    # Flatten the list of lists to analyze individual rows
    all_returned_rows = [row for chunk in chunks for row in chunk]
    
    assert len(all_returned_rows) == 2, "Should have filtered exactly 2 ERROR rows"
    for row in all_returned_rows:
        assert row.level == "ERROR", "Found non-ERROR row in filtered output"
        # Validate that the Python object possesses the correct struct attributes
        assert hasattr(row, "timestamp")
        assert hasattr(row, "user_id")
        assert hasattr(row, "message")