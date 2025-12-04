//! Physical Plan Builder State Management
//!
//! This module provides centralized index management for physical plan construction
//! to ensure continuous and non-conflicting index allocation.

use crate::planner::physical::PhysicalPlan;
use std::sync::Arc;

/// Centralized state manager for physical plan index allocation and node caching
pub struct PhysicalPlanBuilder {
    /// Next available index
    next_index: i64,
    /// Cache of already created physical nodes to ensure shared nodes remain shared
    node_cache: std::collections::HashMap<i64, Arc<PhysicalPlan>>,
}

impl PhysicalPlanBuilder {
    /// Create a new builder starting from index 0
    pub fn new() -> Self {
        Self {
            next_index: 0,
            node_cache: std::collections::HashMap::new(),
        }
    }

    /// Create a new builder starting from a specific index
    pub fn starting_from(start_index: i64) -> Self {
        Self {
            next_index: start_index,
            node_cache: std::collections::HashMap::new(),
        }
    }

    /// Allocate a new index and increment the counter
    pub fn allocate_index(&mut self) -> i64 {
        let index = self.next_index;
        self.next_index += 1;
        index
    }

    /// Allocate multiple indices at once
    pub fn allocate_indices(&mut self, count: usize) -> Vec<i64> {
        let start = self.next_index;
        self.next_index += count as i64;
        (start..self.next_index).collect()
    }

    /// Get the current next index without allocating
    pub fn current_index(&self) -> i64 {
        self.next_index
    }

    /// Peek at the next index that will be allocated
    pub fn peek_next_index(&self) -> i64 {
        self.next_index
    }

    /// Reset the builder to start from a specific index
    pub fn reset_to(&mut self, index: i64) {
        self.next_index = index;
    }

    /// Check if a logical node has already been converted to physical node
    pub fn get_cached_node(&self, logical_index: i64) -> Option<Arc<PhysicalPlan>> {
        self.node_cache.get(&logical_index).map(Arc::clone)
    }

    /// Cache a physical node for future reuse
    pub fn cache_node(&mut self, logical_index: i64, physical_node: Arc<PhysicalPlan>) {
        self.node_cache.insert(logical_index, physical_node);
    }

    /// Clear the node cache
    pub fn clear_cache(&mut self) {
        self.node_cache.clear();
    }
}

impl Default for PhysicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}
