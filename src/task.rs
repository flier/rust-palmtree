use std::ops::{Deref, DerefMut};

/// Tree operation type
pub enum TreeOp<K, V> {
    Find(K),
    Insert(K, V),
    Remove(K),
}

/// A batch of tree operations, this data structure is not thread safe
/// The major goal of this class is to amortize memory allocation of tree operations
pub struct TaskBatch<K, V> {
    ops: Vec<Box<TreeOp<K, V>>>,
}

impl<K, V> TaskBatch<K, V> {
    pub fn with_capacity(capacity: usize) -> Self {
        TaskBatch {
            ops: Vec::with_capacity(capacity),
        }
    }

    pub fn into_inner(self) -> Vec<Box<TreeOp<K, V>>> {
        self.ops
    }

    pub fn is_full(&self) -> bool {
        self.ops.len() == self.ops.capacity()
    }

    /// Add a tree operation to the batch
    pub fn add_op(&mut self, op: TreeOp<K, V>) {
        self.ops.push(Box::new(op));
    }
}

impl<I, K, V> From<I> for TaskBatch<K, V>
where
    I: IntoIterator<Item = Box<TreeOp<K, V>>>,
{
    fn from(ops: I) -> Self {
        TaskBatch {
            ops: ops.into_iter().collect(),
        }
    }
}

impl<K, V> Deref for TaskBatch<K, V> {
    type Target = Vec<Box<TreeOp<K, V>>>;

    fn deref(&self) -> &Self::Target {
        &self.ops
    }
}

impl<K, V> DerefMut for TaskBatch<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}
