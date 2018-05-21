use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub type NodeId = usize;

// Max number of slots per inner node
const INNER_MAX_SLOT: usize = 256;
// Max number of slots per leaf node
const LEAF_MAX_SLOT: usize = 64;

#[derive(Debug)]
pub struct Base<K, V> {
    id: NodeId,
    level: usize,
    lower_bound: K,
    parent: Option<Arc<Box<Node<K, V>>>>,
}

impl<K, V> Base<K, V> {
    fn next_node_id() -> NodeId {
        lazy_static! {
            static ref NODE_NUM: AtomicUsize = AtomicUsize::new(0);
        }

        NODE_NUM.fetch_add(1, Ordering::SeqCst)
    }

    pub fn id(&self) -> NodeId {
        self.id
    }
}

impl<K, V> Base<K, V>
where
    K: Default,
{
    fn new(parent: Option<Arc<Box<Node<K, V>>>>, level: usize) -> Self {
        Base {
            id: Self::next_node_id(),
            level,
            lower_bound: Default::default(),
            parent,
        }
    }
}

#[derive(Debug)]
pub enum Node<K, V> {
    Inner(Inner<K, V>),
    Leaf(Leaf<K, V>),
}

unsafe impl<K, V> Send for Node<K, V> {}
unsafe impl<K, V> Sync for Node<K, V> {}

impl<K, V> Hash for Node<K, V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl<K, V> PartialEq for Node<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<K, V> Eq for Node<K, V> {}

#[derive(Debug)]
pub struct Inner<K, V> {
    base: Base<K, V>,
    // Keys for values
    keys: Vec<K>,
    // Pointers for child nodes
    values: Vec<Arc<Box<Node<K, V>>>>,
}

unsafe impl<K, V> Send for Inner<K, V> {}
unsafe impl<K, V> Sync for Inner<K, V> {}

impl<K, V> Inner<K, V>
where
    K: Default + Ord,
{
    pub fn search(&self, key: &K) -> Arc<Box<Node<K, V>>> {
        match self.keys.binary_search(key) {
            Ok(idx) | Err(idx) => self.values
                .get(idx)
                .or_else(|| self.values.last())
                .cloned()
                .unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct Leaf<K, V> {
    base: Base<K, V>,
    // Keys for leaf node
    keys: Vec<K>,
    // Values for leaf node
    values: Vec<Arc<V>>,
}

unsafe impl<K, V> Send for Leaf<K, V> {}
unsafe impl<K, V> Sync for Leaf<K, V> {}

impl<K, V> Deref for Node<K, V> {
    type Target = Base<K, V>;

    fn deref(&self) -> &Self::Target {
        match *self {
            Node::Inner(Inner { ref base, .. }) | Node::Leaf(Leaf { ref base, .. }) => base,
        }
    }
}

impl<K, V> DerefMut for Node<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            Node::Inner(Inner { ref mut base, .. }) | Node::Leaf(Leaf { ref mut base, .. }) => base,
        }
    }
}

pub fn inner<K, V>(parent: Option<Arc<Box<Node<K, V>>>>, level: usize) -> Arc<Box<Node<K, V>>>
where
    K: Default + Ord,
{
    Arc::new(Box::new(Node::<K, V>::inner(parent, level)))
}

pub fn leaf<K, V>(parent: Option<Arc<Box<Node<K, V>>>>, level: usize) -> Arc<Box<Node<K, V>>>
where
    K: Default + Ord,
{
    Arc::new(Box::new(Node::<K, V>::leaf(parent, level)))
}

impl<K, V> Node<K, V>
where
    K: Default,
{
    pub fn inner(parent: Option<Arc<Box<Node<K, V>>>>, level: usize) -> Self {
        Node::Inner(Inner {
            base: Base::new(parent, level),
            keys: Vec::with_capacity(INNER_MAX_SLOT),
            values: Vec::with_capacity(INNER_MAX_SLOT),
        })
    }

    pub fn leaf(parent: Option<Arc<Box<Node<K, V>>>>, level: usize) -> Self {
        Node::Leaf(Leaf {
            base: Base::new(parent, level),
            keys: Vec::with_capacity(LEAF_MAX_SLOT),
            values: Vec::with_capacity(LEAF_MAX_SLOT),
        })
    }

    pub fn as_inner(&self) -> Option<&Inner<K, V>> {
        match *self {
            Node::Inner(ref inner) => Some(inner),
            Node::Leaf(_) => None,
        }
    }

    pub fn as_leaf(&self) -> Option<&Leaf<K, V>> {
        match *self {
            Node::Inner(_) => None,
            Node::Leaf(ref leaf) => Some(leaf),
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.as_leaf().is_some()
    }

    pub fn keys(&self) -> &[K] {
        match *self {
            Node::Inner(Inner { ref keys, .. }) | Node::Leaf(Leaf { ref keys, .. }) => keys,
        }
    }
}
