use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{Builder as ThreadBuilder, JoinHandle};

use errors::Result;
use node::{self, Node};
use task::{TaskBatch, TreeOp};
use worker::Worker;

const DEFAULT_BATCH_SIZE_PER_WORKER: usize = 4096;

pub struct PalmTree<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    // Root of the palm tree
    tree_root: Arc<Box<Node>>,
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
    task_nums: AtomicUsize,
    task_batch_queue: (Sender<TaskBatch<K, V>>, Receiver<TaskBatch<K, V>>),
    workers: Vec<JoinHandle<()>>,
}

impl<K, V> PalmTree<K, V>
where
    K: 'static + Send + Sync + Default + Debug,
    V: 'static + Send + Sync,
{
    pub fn new(min_key: K, num_workers: usize) -> Self {
        let channels: Vec<(Sender<TaskBatch<K, V>>, Receiver<TaskBatch<K, V>>)> =
            (0..num_workers).map(|_| channel()).collect();
        let senders = channels
            .iter()
            .map(|(sender, _)| sender.clone())
            .collect::<Vec<_>>();
        let receivers = channels
            .into_iter()
            .map(|(_, receiver)| receiver)
            .collect::<Vec<_>>();

        PalmTree {
            tree_root: node::inner::<K>(None, 1),
            tree_depth: 1,
            layer_width: vec![AtomicUsize::new(1), AtomicUsize::new(1)],
            min_key,
            tree_current_batch: RefCell::new(TaskBatch::with_capacity(
                DEFAULT_BATCH_SIZE_PER_WORKER * num_workers,
            )),
            num_workers,
            batch_size_per_worker: DEFAULT_BATCH_SIZE_PER_WORKER,
            task_nums: AtomicUsize::new(0),
            task_batch_queue: channel(),
            workers: receivers
                .into_iter()
                .enumerate()
                .map(move |(worker_id, receiver)| {
                    let senders = senders.clone();

                    ThreadBuilder::new()
                        .name(format!("palmtree-worker-{}", worker_id))
                        .spawn(move || Worker::new(worker_id, receiver, senders).run())
                        .unwrap()
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl<K, V> PalmTree<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    fn batch_size(&self) -> usize {
        self.batch_size_per_worker * self.num_workers
    }

    /// Find the value for a key
    pub fn find(&self, key: K) -> Result<()> {
        self.push_task(TreeOp::find(key))
    }

    /// insert a k,v into the tree
    pub fn insert(&self, key: K, value: V) -> Result<()> {
        self.push_task(TreeOp::insert(key, value))
    }

    /// remove a k,v from the tree
    pub fn remove(&self, key: K) -> Result<()> {
        self.push_task(TreeOp::remove(key))
    }

    /// Push a task into the current batch, if the batch is full, push the batch into the batch queue.
    fn push_task(&self, op: TreeOp<K, V>) -> Result<()> {
        self.tree_current_batch.borrow_mut().add_op(op);

        self.task_nums.fetch_add(2, Ordering::SeqCst);

        if self.tree_current_batch.borrow().is_full() {
            trace!("push task batch into the queue");

            let task_batch = self.tree_current_batch
                .replace(TaskBatch::with_capacity(self.batch_size()));
            self.task_batch_queue.0.send(task_batch)?;
        }

        Ok(())
    }
}
