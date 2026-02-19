#!/usr/bin/env python
"""
Test script for merge_build_logs function

Tests the merge_build_logs function with actual database files.
Requires access to HPC-specific paths (/gpfs/...).
"""

import json
import os

import pytest

from gedih3.gh3builder import merge_build_logs


@pytest.mark.integration
def test_merge_build_logs():
    """Test merging two build log files from actual databases"""
    
    log_file_1 = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world/gedih3_build_log.json'
    log_file_2 = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_a10/gedih3_build_log.json'
    output_log_file = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_merged/gedih3_build_log.json'
    
    # Verify input files exist
    assert os.path.exists(log_file_1), f"Log file not found: {log_file_1}"
    assert os.path.exists(log_file_2), f"Log file not found: {log_file_2}"
    
    # Call merge function
    merged_log = merge_build_logs(log_file_1, log_file_2, output_log_file)
    
    # Verify output file was created
    assert os.path.exists(output_log_file), f"Output log file was not created: {output_log_file}"
    
    # Verify merged log is valid
    assert isinstance(merged_log, dict), "Merged log should be a dictionary"
    assert 'gedi_version' in merged_log, "Missing gedi_version in merged log"
    assert 'h3_resolution_level' in merged_log, "Missing h3_resolution_level in merged log"
    assert 'h3_partition_level' in merged_log, "Missing h3_partition_level in merged log"
    assert 'status' in merged_log, "Missing status in merged log"
    assert merged_log['status'] == 'COMPLETED', f"Status should be COMPLETED, got {merged_log['status']}"
    
    # Verify key data was merged
    granules = merged_log.get('granules', [])
    h3_cols = merged_log.get('h3_columns', [])
    h3_parts = merged_log.get('h3_partition_ids', [])
    date_range = merged_log.get('date_range')
    
    print("✓ Test passed!")
    print(f"\nMerged log statistics:")
    print(f"  Total granules: {len(granules)}")
    print(f"  Total h3 columns: {len(h3_cols)}")
    print(f"  Total h3 partitions: {len(h3_parts)}")
    print(f"  Date range: {date_range}")
    print(f"  Status: {merged_log.get('status')}")
    print(f"  GEDI Version: {merged_log.get('gedi_version')}")
    print(f"  H3 Resolution Level: {merged_log.get('h3_resolution_level')}")
    print(f"  H3 Partition Level: {merged_log.get('h3_partition_level')}")
    
    print(f"\nProducts in merged log:")
    for prod, data in merged_log.get('products', {}).items():
        print(f"  - {prod}: status={data.get('status')}, variables={len(data.get('variables', []))}")
    
    print(f"\nMerged log written to: {output_log_file}")


if __name__ == '__main__':
    test_merge_build_logs()
