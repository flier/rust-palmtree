use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use node::Node;

/// Tree operation types
enum TreeOpType {
    Find,
    Insert,
    Remove,
}

pub struct TreeOp<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    op: TreeOpType,
    key: K,
    value: Option<V>,
    result: Option<Option<V>>,
}

impl<K, V> TreeOp<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub fn insert(key: K, value: V) -> Self {
        TreeOp {
            op: TreeOpType::Insert,
            key,
            value: Some(value),
            result: None,
        }
    }

    pub fn find(key: K) -> Self {
        TreeOp {
            op: TreeOpType::Find,
            key,
            value: None,
            result: None,
        }
    }

    pub fn remove(key: K) -> Self {
        TreeOp {
            op: TreeOpType::Remove,
            key,
            value: None,
            result: None,
        }
    }
}

/// A batch of tree operations, this data structure is not thread safe
/// The major goal of this class is to amortize memory allocation of tree operations
pub struct TaskBatch<K, V>(Vec<TreeOp<K, V>>)
where
    K: Send + Sync,
    V: Send + Sync;

impl<K, V> TaskBatch<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub fn with_capacity(capacity: usize) -> Self {
        TaskBatch(Vec::with_capacity(capacity))
    }

    pub fn is_full(&self) -> bool {
        self.0.len() == self.0.capacity()
    }

    /// Add a tree operation to the batch
    pub fn add_op(&mut self, op: TreeOp<K, V>) {
        self.0.push(op);
    }
}
