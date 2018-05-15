use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use node::Node;

/// Tree operation type
pub enum TreeOp<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    Find(K),
    Insert(K, V),
    Remove(K),
}

/// A batch of tree operations, this data structure is not thread safe
/// The major goal of this class is to amortize memory allocation of tree operations
pub struct TaskBatch<K, V>(Vec<Box<TreeOp<K, V>>>)
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

    pub fn into_inner(self) -> Vec<Box<TreeOp<K, V>>> {
        self.0
    }

    pub fn is_full(&self) -> bool {
        self.0.len() == self.0.capacity()
    }

    /// Add a tree operation to the batch
    pub fn add_op(&mut self, op: TreeOp<K, V>) {
        self.0.push(Box::new(op));
    }
}

impl<I, K, V> From<I> for TaskBatch<K, V>
where
    I: IntoIterator<Item = Box<TreeOp<K, V>>>,
    K: Send + Sync,
    V: Send + Sync,
{
    fn from(ops: I) -> Self {
        TaskBatch(ops.into_iter().collect())
    }
}

impl<K, V> Deref for TaskBatch<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    type Target = Vec<Box<TreeOp<K, V>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for TaskBatch<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
