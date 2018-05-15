use std::fmt::Debug;
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

pub enum NodeType {
    Inner,
    Leaf,
}

pub trait Node: Debug {
    fn node_type(&self) -> NodeType;
}

#[derive(Debug)]
pub struct BaseNode<K> {
    id: usize,
    level: usize,
    lower_bound: K,
    parent: Option<Arc<Box<Node>>>,
}

impl<K> BaseNode<K>
where
    K: Default,
{
    fn new(parent: Option<Arc<Box<Node>>>, level: usize) -> Self {
        BaseNode {
            id: next_node_id(),
            level,
            lower_bound: Default::default(),
            parent,
        }
    }
}

#[derive(Debug)]
pub struct InnerNode<K> {
    base: BaseNode<K>,
    // Keys for values
    keys: Vec<K>,
    // Pointers for child nodes
    values: Vec<Arc<Box<Node>>>,
}

unsafe impl<K> Send for InnerNode<K> {}
unsafe impl<K> Sync for InnerNode<K> {}

impl<K> InnerNode<K>
where
    K: Default,
{
    pub fn new(parent: Option<Arc<Box<Node>>>, level: usize) -> InnerNode<K> {
        InnerNode {
            base: BaseNode::new(parent, level),
            keys: Vec::with_capacity(LEAF_MAX_SLOT),
            values: Vec::with_capacity(LEAF_MAX_SLOT),
        }
    }
}

pub fn inner<K>(parent: Option<Arc<Box<Node>>>, level: usize) -> Arc<Box<Node>>
where
    K: 'static + Default + Debug,
{
    Arc::new(Box::new(InnerNode::<K>::new(parent, level)))
}

impl<K> Node for InnerNode<K>
where
    K: Debug,
{
    fn node_type(&self) -> NodeType {
        NodeType::Inner
    }
}

#[derive(Debug)]
pub struct LeafNode<K, V> {
    base: BaseNode<K>,
    // Keys for leaf node
    keys: Vec<K>,
    // Values for leaf node
    values: Vec<V>,
}

pub fn leaf<K, V>(parent: Option<Arc<Box<Node>>>, level: usize) -> LeafNode<K, V>
where
    K: Default,
{
    LeafNode {
        base: BaseNode::new(parent, level),
        keys: Vec::with_capacity(LEAF_MAX_SLOT),
        values: Vec::with_capacity(LEAF_MAX_SLOT),
    }
}

impl<K, V> Node for LeafNode<K, V>
where
    K: Debug,
    V: Debug,
{
    fn node_type(&self) -> NodeType {
        NodeType::Leaf
    }
}
