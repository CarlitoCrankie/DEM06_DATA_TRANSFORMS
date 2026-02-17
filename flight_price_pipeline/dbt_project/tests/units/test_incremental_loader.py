"""
Unit Tests for Incremental Loader Module

Tests cover:
- MD5 hash calculation
- Change detection logic (NEW/UPDATED/DELETED/UNCHANGED)
- Threshold logic for load strategy
- Metrics calculation
- Edge cases and error handling

Author: Carl Nyameakyere Crankson
Date: February 2026
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dags.utils.incremental_loader import IncrementalLoader, create_loader


class TestIncrementalLoader:
    """Test suite for IncrementalLoader class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.loader = IncrementalLoader(threshold_percentage=50.0)
    
    def test_initialization(self):
        """Test loader initialization with default and custom thresholds"""
        # Default threshold
        loader1 = IncrementalLoader()
        assert loader1.threshold_percentage == 50.0
        assert loader1.metrics['new'] == 0
        
        # Custom threshold
        loader2 = IncrementalLoader(threshold_percentage=75.0)
        assert loader2.threshold_percentage == 75.0
    
    def test_calculate_md5_consistency(self):
        """Test MD5 hash calculation is consistent for same input"""
        record = {'id': 1, 'name': 'Test', 'value': 100}
        hash1 = self.loader.calculate_md5(record)
        hash2 = self.loader.calculate_md5(record)
        
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 produces 32-character hex string
    
    def test_calculate_md5_different_records(self):
        """Test different records produce different hashes"""
        record1 = {'id': 1, 'name': 'Test1'}
        record2 = {'id': 2, 'name': 'Test2'}
        
        hash1 = self.loader.calculate_md5(record1)
        hash2 = self.loader.calculate_md5(record2)
        
        assert hash1 != hash2
    
    def test_calculate_md5_key_order_independence(self):
        """Test hash is same regardless of key order"""
        record1 = {'id': 1, 'name': 'Test', 'value': 100}
        record2 = {'value': 100, 'id': 1, 'name': 'Test'}
        
        hash1 = self.loader.calculate_md5(record1)
        hash2 = self.loader.calculate_md5(record2)
        
        assert hash1 == hash2
    
    def test_detect_changes_all_new(self):
        """Test detection of all new records"""
        source = [
            {'id': 1, 'name': 'Record1'},
            {'id': 2, 'name': 'Record2'}
        ]
        target = []
        
        changes = self.loader.detect_changes(source, target)
        
        assert len(changes['new']) == 2
        assert len(changes['updated']) == 0
        assert len(changes['deleted']) == 0
        assert len(changes['unchanged']) == 0
        
        assert self.loader.metrics['new'] == 2
        assert self.loader.metrics['total_source'] == 2
        assert self.loader.metrics['total_target'] == 0
    
    def test_detect_changes_all_deleted(self):
        """Test detection of all deleted records"""
        source = []
        target = [
            {'id': 1, 'name': 'Record1'},
            {'id': 2, 'name': 'Record2'}
        ]
        
        changes = self.loader.detect_changes(source, target)
        
        assert len(changes['new']) == 0
        assert len(changes['updated']) == 0
        assert len(changes['deleted']) == 2
        assert len(changes['unchanged']) == 0
        
        assert self.loader.metrics['deleted'] == 2
    
    def test_detect_changes_updated(self):
        """Test detection of updated records"""
        source = [
            {'id': 1, 'name': 'Record1_Updated'},
            {'id': 2, 'name': 'Record2'}
        ]
        target = [
            {'id': 1, 'name': 'Record1'},
            {'id': 2, 'name': 'Record2'}
        ]
        
        changes = self.loader.detect_changes(source, target)
        
        assert len(changes['new']) == 0
        assert len(changes['updated']) == 1
        assert len(changes['deleted']) == 0
        assert len(changes['unchanged']) == 1
        
        assert changes['updated'][0]['id'] == 1
        assert self.loader.metrics['updated'] == 1
        assert self.loader.metrics['unchanged'] == 1
    
    def test_detect_changes_mixed(self):
        """Test detection of mixed changes (new, updated, deleted, unchanged)"""
        source = [
            {'id': 1, 'name': 'Record1_Updated'},  # Updated
            {'id': 2, 'name': 'Record2'},           # Unchanged
            {'id': 4, 'name': 'Record4'}            # New
        ]
        target = [
            {'id': 1, 'name': 'Record1'},           # Will be updated
            {'id': 2, 'name': 'Record2'},           # Unchanged
            {'id': 3, 'name': 'Record3'}            # Will be deleted
        ]
        
        changes = self.loader.detect_changes(source, target)
        
        assert len(changes['new']) == 1
        assert len(changes['updated']) == 1
        assert len(changes['deleted']) == 1
        assert len(changes['unchanged']) == 1
        
        assert self.loader.metrics['new'] == 1
        assert self.loader.metrics['updated'] == 1
        assert self.loader.metrics['deleted'] == 1
        assert self.loader.metrics['unchanged'] == 1
    
    def test_detect_changes_no_changes(self):
        """Test when source and target are identical"""
        records = [
            {'id': 1, 'name': 'Record1'},
            {'id': 2, 'name': 'Record2'}
        ]
        
        changes = self.loader.detect_changes(records, records)
        
        assert len(changes['new']) == 0
        assert len(changes['updated']) == 0
        assert len(changes['deleted']) == 0
        assert len(changes['unchanged']) == 2
    
    def test_detect_changes_custom_id_field(self):
        """Test change detection with custom ID field"""
        source = [{'user_id': 1, 'name': 'User1'}]
        target = []
        
        changes = self.loader.detect_changes(source, target, id_field='user_id')
        
        assert len(changes['new']) == 1
    
    def test_calculate_change_percentage_no_changes(self):
        """Test percentage calculation with no changes"""
        self.loader.metrics = {
            'new': 0,
            'updated': 0,
            'deleted': 0,
            'unchanged': 100,
            'total_source': 100,
            'total_target': 100
        }
        
        pct = self.loader.calculate_change_percentage()
        assert pct == 0.0
    
    def test_calculate_change_percentage_some_changes(self):
        """Test percentage calculation with some changes"""
        self.loader.metrics = {
            'new': 5,
            'updated': 10,
            'deleted': 5,
            'unchanged': 80,
            'total_source': 100,
            'total_target': 100
        }
        
        pct = self.loader.calculate_change_percentage()
        assert pct == 20.0
    
    def test_calculate_change_percentage_all_new(self):
        """Test percentage calculation when all records are new"""
        self.loader.metrics = {
            'new': 100,
            'updated': 0,
            'deleted': 0,
            'unchanged': 0,
            'total_source': 100,
            'total_target': 0
        }
        
        pct = self.loader.calculate_change_percentage()
        assert pct == 100.0
    
    def test_calculate_change_percentage_zero_division(self):
        """Test percentage calculation handles zero records"""
        self.loader.metrics = {
            'new': 0,
            'updated': 0,
            'deleted': 0,
            'unchanged': 0,
            'total_source': 0,
            'total_target': 0
        }
        
        pct = self.loader.calculate_change_percentage()
        assert pct == 0.0  # Should not raise division by zero
    
    def test_should_use_incremental_below_threshold(self):
        """Test incremental is recommended below threshold"""
        self.loader.metrics = {
            'new': 10,
            'updated': 10,
            'deleted': 10,
            'unchanged': 70,
            'total_source': 100,
            'total_target': 100
        }
        
        assert self.loader.should_use_incremental() is True
    
    def test_should_use_incremental_above_threshold(self):
        """Test full load is recommended above threshold"""
        self.loader.metrics = {
            'new': 30,
            'updated': 30,
            'deleted': 30,
            'unchanged': 10,
            'total_source': 100,
            'total_target': 100
        }
        
        assert self.loader.should_use_incremental() is False
    
    def test_should_use_incremental_at_threshold(self):
        """Test behavior exactly at threshold"""
        self.loader.metrics = {
            'new': 25,
            'updated': 25,
            'deleted': 0,
            'unchanged': 50,
            'total_source': 100,
            'total_target': 100
        }
        
        # At 50%, should not use incremental (>= threshold means full)
        assert self.loader.should_use_incremental() is False
    
    def test_get_load_strategy_skip(self):
        """Test strategy returns 'skip' for no changes"""
        self.loader.metrics = {
            'new': 0,
            'updated': 0,
            'deleted': 0,
            'unchanged': 100,
            'total_source': 100,
            'total_target': 100
        }
        
        strategy = self.loader.get_load_strategy()
        assert strategy == 'skip'
    
    def test_get_load_strategy_incremental(self):
        """Test strategy returns 'incremental' for small changes"""
        self.loader.metrics = {
            'new': 10,
            'updated': 5,
            'deleted': 5,
            'unchanged': 80,
            'total_source': 100,
            'total_target': 100
        }
        
        strategy = self.loader.get_load_strategy()
        assert strategy == 'incremental'
    
    def test_get_load_strategy_full(self):
        """Test strategy returns 'full' for large changes"""
        self.loader.metrics = {
            'new': 40,
            'updated': 30,
            'deleted': 10,
            'unchanged': 20,
            'total_source': 100,
            'total_target': 100
        }
        
        strategy = self.loader.get_load_strategy()
        assert strategy == 'full'
    
    def test_get_metrics_summary(self):
        """Test metrics summary includes all required fields"""
        self.loader.metrics = {
            'new': 10,
            'updated': 5,
            'deleted': 5,
            'unchanged': 80,
            'total_source': 100,
            'total_target': 100
        }
        
        summary = self.loader.get_metrics_summary()
        
        assert 'metrics' in summary
        assert 'change_percentage' in summary
        assert 'threshold_percentage' in summary
        assert 'recommended_strategy' in summary
        assert 'timestamp' in summary
        
        assert summary['change_percentage'] == 20.0
        assert summary['recommended_strategy'] == 'incremental'
    
    def test_apply_changes_returns_counts(self):
        """Test apply_changes returns correct counts"""
        changes = {
            'new': [{'id': 1}, {'id': 2}],
            'updated': [{'id': 3}],
            'deleted': [{'id': 4}, {'id': 5}, {'id': 6}],
            'unchanged': []
        }
        
        inserted, updated, deleted = self.loader.apply_changes(
            changes,
            connection=None,  # Mock connection
            table_name='test_table'
        )
        
        assert inserted == 2
        assert updated == 1
        assert deleted == 3
    
    def test_factory_function(self):
        """Test factory function creates loader correctly"""
        loader = create_loader(threshold=75.0)
        
        assert isinstance(loader, IncrementalLoader)
        assert loader.threshold_percentage == 75.0
    
    def test_multiple_change_detections(self):
        """Test metrics reset between change detection runs"""
        source1 = [{'id': 1, 'name': 'Record1'}]
        target1 = []
        
        changes1 = self.loader.detect_changes(source1, target1)
        assert self.loader.metrics['new'] == 1
        
        # Second detection should reset metrics
        source2 = [{'id': 2, 'name': 'Record2'}]
        target2 = [{'id': 2, 'name': 'Record2'}]
        
        changes2 = self.loader.detect_changes(source2, target2)
        assert self.loader.metrics['new'] == 0
        assert self.loader.metrics['unchanged'] == 1


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_empty_source_and_target(self):
        """Test with both empty datasets"""
        loader = IncrementalLoader()
        changes = loader.detect_changes([], [])
        
        assert all(len(changes[k]) == 0 for k in changes.keys())
        assert loader.calculate_change_percentage() == 0.0
    
    def test_single_record(self):
        """Test with single record"""
        loader = IncrementalLoader()
        source = [{'id': 1, 'name': 'Single'}]
        
        changes = loader.detect_changes(source, [])
        assert len(changes['new']) == 1
        assert loader.calculate_change_percentage() == 100.0
    
    def test_large_dataset(self):
        """Test with large dataset (performance check)"""
        loader = IncrementalLoader()
        
        # Create 10,000 records
        source = [{'id': i, 'name': f'Record{i}'} for i in range(10000)]
        target = [{'id': i, 'name': f'Record{i}'} for i in range(9000)]
        
        changes = loader.detect_changes(source, target)
        
        assert len(changes['new']) == 1000
        assert len(changes['unchanged']) == 9000
        assert loader.metrics['total_source'] == 10000
    
    def test_special_characters_in_data(self):
        """Test MD5 calculation with special characters"""
        loader = IncrementalLoader()
        record = {
            'id': 1,
            'name': 'Test™©®',
            'description': 'Special chars: !@#$%^&*()',
            'unicode': '你好世界'
        }
        
        hash_val = loader.calculate_md5(record)
        assert len(hash_val) == 32
    
    def test_none_values(self):
        """Test with None values in records"""
        loader = IncrementalLoader()
        record1 = {'id': 1, 'name': None, 'value': 100}
        record2 = {'id': 1, 'name': None, 'value': 100}
        
        hash1 = loader.calculate_md5(record1)
        hash2 = loader.calculate_md5(record2)
        
        assert hash1 == hash2


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])