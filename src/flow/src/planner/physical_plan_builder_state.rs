//! Physical Plan Builder State Management
//!
//! This module provides centralized index management for physical plan construction
//! to ensure continuous and non-conflicting index allocation.

use crate::processor::ProcessorError;
use std::sync::Arc;

/// Centralized state manager for physical plan index allocation
pub struct PhysicalPlanBuilder {
    /// Next available index
    next_index: i64,
}

impl PhysicalPlanBuilder {
    /// Create a new builder starting from index 0
    pub fn new() -> Self {
        Self { next_index: 0 }
    }
    
    /// Create a new builder starting from a specific index
    pub fn starting_from(start_index: i64) -> Self {
        Self { next_index: start_index }
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
}

impl Default for PhysicalPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}