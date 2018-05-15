use std::cell::RefCell;
use std::cmp;
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Barrier};
use std::thread::{Builder as ThreadBuilder, JoinHandle};

use errors::Result;
use node::{self, Node};
use task::{TaskBatch, TreeOp};
use worker::Worker;

const DEFAULT_BATCH_SIZE_PER_WORKER: usize = 4096;

#[derive(Clone)]
pub struct PalmTree<K, V> {
    inner: Rc<Inner<K, V>>,
}

struct Inner<K, V> {
    // Root of the palm tree
    tree_root: Arc<Box<Node + Send + Sync>>,
    // Height of the tree
    tree_depth: usize,
    // Number of nodes on each layer
    layer_width: Vec<AtomicUsize>,
    // Minimal key
    min_key: K,
    // Current batch of the tree
    tree_current_batch: RefCell<TaskBatch<K, V>>,
    // Number of working threads
    num_workers: usize,
    /// Number of working threads
    batch_size_per_worker: usize,
    senders: Vec<Sender<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>>,
    workers: Vec<JoinHandle<Result<()>>>,
}

impl<K, V> PalmTree<K, V>
where
    K: 'static + Send + Sync + Default + Debug,
    V: 'static + Send + Sync,
{
    pub fn new(min_key: K, num_workers: usize) -> Self {
        let num_workers = cmp::max(num_workers, 1);
        let channels: Vec<(
            Sender<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>,
            Receiver<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>,
        )> = (0..num_workers).map(|_| channel()).collect();
        let senders = channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect::<Vec<_>>();
        let receivers = channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect::<Vec<_>>();
        let barrier = Arc::new(Barrier::new(num_workers));

        PalmTree {
            inner: Rc::new(Inner {
                tree_root: node::inner::<K>(None, 1),
                tree_depth: 1,
                layer_width: vec![AtomicUsize::new(1), AtomicUsize::new(1)],
                min_key,
                tree_current_batch: RefCell::new(TaskBatch::with_capacity(
                    DEFAULT_BATCH_SIZE_PER_WORKER * num_workers,
                )),
                num_workers,
                batch_size_per_worker: DEFAULT_BATCH_SIZE_PER_WORKER,
                senders: senders.clone(),
                workers: receivers
                    .into_iter()
                    .enumerate()
                    .map(move |(worker_id, receiver)| {
                        let senders = senders.clone();
                        let barrier = barrier.clone();

                        ThreadBuilder::new()
                            .name(format!("palmtree-worker-{}", worker_id))
                            .spawn(move || Worker::new(worker_id, receiver, senders, barrier).run())
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            }),
        }
    }
}

impl<K, V> PalmTree<K, V> {
    pub fn batch_size(&self) -> usize {
        self.inner.batch_size_per_worker * self.inner.num_workers
    }
}

impl<K, V> PalmTree<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    /// Find the value for a key
    pub fn find(&self, key: K) -> Result<()> {
        self.push_task(TreeOp::Find(key))
    }

    /// insert a k,v into the tree
    pub fn insert(&self, key: K, value: V) -> Result<()> {
        self.push_task(TreeOp::Insert(key, value))
    }

    /// remove a k,v from the tree
    pub fn remove(&self, key: K) -> Result<()> {
        self.push_task(TreeOp::Remove(key))
    }

    /// Push a task into the current batch, if the batch is full, push the batch into the batch queue.
    fn push_task(&self, op: TreeOp<K, V>) -> Result<()> {
        self.inner.tree_current_batch.borrow_mut().add_op(op);

        if self.inner.tree_current_batch.borrow().is_full() {
            trace!("push task batch into the queue");

            let task_batch = self.inner
                .tree_current_batch
                .replace(TaskBatch::with_capacity(self.batch_size()));
            self.inner
                .senders
                .first()
                .unwrap()
                .send((self.inner.tree_root.clone(), task_batch))?;
        }

        Ok(())
    }
}
