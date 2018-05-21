use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// Max number of slots per inner node
const INNER_MAX_SLOT: usize = 256;
// Max number of slots per leaf node
const LEAF_MAX_SLOT: usize = 64;

lazy_static! {
    static ref NODE_NUM: AtomicUsize = Default::default();
}

fn next_node_id() -> usize {
    NODE_NUM.fetch_add(1, Ordering::SeqCst)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodeType {
    Inner,
    Leaf,
}

pub trait Node<K>: Debug {
    fn node_type(&self) -> NodeType;

    fn is_leaf(&self) -> bool {
        self.node_type() == NodeType::Leaf
    }

    fn search(&self, key: &K) -> Option<usize>;
}

#[derive(Debug)]
pub struct Base<K> {
    id: usize,
    level: usize,
    lower_bound: K,
    parent: Option<Arc<Box<Node<K>>>>,
}

impl<K> Base<K>
where
    K: Default,
{
    fn new(parent: Option<Arc<Box<Node<K>>>>, level: usize) -> Self {
        Base {
            id: next_node_id(),
            level,
            lower_bound: Default::default(),
            parent,
        }
    }
}

#[derive(Debug)]
pub struct Inner<K> {
    base: Base<K>,
    // Keys for values
    keys: Vec<K>,
    // Pointers for child nodes
    values: Vec<Arc<Box<Node<K>>>>,
}

unsafe impl<K> Send for Inner<K> {}
unsafe impl<K> Sync for Inner<K> {}

impl<K> Deref for Inner<K> {
    type Target = Base<K>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<K> DerefMut for Inner<K> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<K> Inner<K>
where
    K: Default + Ord,
{
    pub fn new(parent: Option<Arc<Box<Node<K>>>>, level: usize) -> Self {
        Inner {
            base: Base::new(parent, level),
            keys: Vec::with_capacity(INNER_MAX_SLOT),
            values: Vec::with_capacity(INNER_MAX_SLOT),
        }
    }
}

pub fn inner<K>(parent: Option<Arc<Box<Node<K>>>>, level: usize) -> Arc<Box<Node<K> + Send + Sync>>
where
    K: 'static + Debug + Default + Ord,
{
    Arc::new(Box::new(Inner::<K>::new(parent, level)))
}

impl<K> Node<K> for Inner<K>
where
    K: Debug + Ord,
{
    fn node_type(&self) -> NodeType {
        NodeType::Inner
    }

    fn search(&self, key: &K) -> Option<usize> {
        Some(match self.keys.binary_search(key) {
            Ok(idx) | Err(idx) => idx,
        })
    }
}

#[derive(Debug)]
pub struct Leaf<K, V> {
    base: Base<K>,
    // Keys for leaf node
    keys: Vec<K>,
    // Values for leaf node
    values: Vec<V>,
}

unsafe impl<K, V> Send for Leaf<K, V> {}
unsafe impl<K, V> Sync for Leaf<K, V> {}

impl<K, V> Deref for Leaf<K, V> {
    type Target = Base<K>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<K, V> DerefMut for Leaf<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl<K, V> Leaf<K, V>
where
    K: Default + Ord,
{
    pub fn new(parent: Option<Arc<Box<Node<K>>>>, level: usize) -> Self {
        Leaf {
            base: Base::new(parent, level),
            keys: Vec::with_capacity(LEAF_MAX_SLOT),
            values: Vec::with_capacity(LEAF_MAX_SLOT),
        }
    }
}

pub fn leaf<K, V>(
    parent: Option<Arc<Box<Node<K>>>>,
    level: usize,
) -> Arc<Box<Node<K> + Send + Sync>>
where
    K: 'static + Debug + Default + Ord,
    V: 'static + Debug,
{
    Arc::new(Box::new(Leaf::<K, V>::new(parent, level)))
}

impl<K, V> Node<K> for Leaf<K, V>
where
    K: Debug + Ord,
    V: Debug,
{
    fn node_type(&self) -> NodeType {
        NodeType::Leaf
    }

    fn search(&self, key: &K) -> Option<usize> {
        self.keys.binary_search(key).ok()
    }
}
