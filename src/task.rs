use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

/// Tree operation type
pub enum TreeOp<K, V> {
    Find {
        key: K,
        result: AtomicPtr<V>,
    },
    Insert {
        key: K,
        value: V,
        result: AtomicPtr<V>,
    },
    Remove {
        key: K,
        result: AtomicPtr<V>,
    },
}

impl<K, V> TreeOp<K, V> {
    pub fn find(key: K) -> Self {
        TreeOp::Find {
            key,
            result: AtomicPtr::default(),
        }
    }

    pub fn insert(key: K, value: V) -> Self {
        TreeOp::Insert {
            key,
            value,
            result: AtomicPtr::default(),
        }
    }

    pub fn remove(key: K) -> Self {
        TreeOp::Remove {
            key,
            result: AtomicPtr::default(),
        }
    }

    pub fn key(&self) -> &K {
        match *self {
            TreeOp::Find { ref key, .. }
            | TreeOp::Insert { ref key, .. }
            | TreeOp::Remove { ref key, .. } => key,
        }
    }

    pub fn result(&self) -> Option<Arc<V>> {
        match *self {
            TreeOp::Find { ref result, .. }
            | TreeOp::Insert { ref result, .. }
            | TreeOp::Remove { ref result, .. } => {
                let result = result.load(Ordering::Relaxed);

                if result.is_null() {
                    None
                } else {
                    Some(unsafe { Arc::from_raw(result) })
                }
            }
        }
    }
}

/// A batch of tree operations, this data structure is not thread safe
/// The major goal of this class is to amortize memory allocation of tree operations
pub struct TaskBatch<K, V> {
    ops: Vec<TreeOp<K, V>>,
}

impl<K, V> TaskBatch<K, V> {
    pub fn with_capacity(capacity: usize) -> Self {
        TaskBatch {
            ops: Vec::with_capacity(capacity),
        }
    }

    pub fn into_inner(self) -> Vec<TreeOp<K, V>> {
        self.ops
    }

    pub fn is_full(&self) -> bool {
        self.ops.len() == self.ops.capacity()
    }

    /// Add a tree operation to the batch
    pub fn add_op(&mut self, op: TreeOp<K, V>) {
        self.ops.push(op);
    }
}

impl<I, K, V> From<I> for TaskBatch<K, V>
where
    I: IntoIterator<Item = TreeOp<K, V>>,
{
    fn from(ops: I) -> Self {
        TaskBatch {
            ops: ops.into_iter().collect(),
        }
    }
}

impl<K, V> Deref for TaskBatch<K, V> {
    type Target = Vec<TreeOp<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.ops
    }
}

impl<K, V> DerefMut for TaskBatch<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}
