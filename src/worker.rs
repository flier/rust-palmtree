use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Barrier};

use itertools::Itertools;
use time::PreciseTime;

use errors::Result;
use node::Node;
use task::TaskBatch;

const MASTER_WORKER_ID: usize = 0;

pub struct Worker<K, V> {
    id: usize,
    terminated: Arc<AtomicBool>,
    receiver: Receiver<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>,
    senders: Vec<Sender<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>>,
    barrier: Arc<Barrier>,
}

impl<K, V> Worker<K, V> {
    pub fn new(
        id: usize,
        terminated: Arc<AtomicBool>,
        receiver: Receiver<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>,
        senders: Vec<Sender<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)>>,
        barrier: Arc<Barrier>,
    ) -> Self {
        Worker {
            id,
            terminated,
            receiver,
            senders,
            barrier,
        }
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Relaxed)
    }

    pub fn is_master(&self) -> bool {
        self.id == MASTER_WORKER_ID
    }

    fn sync(&self) -> bool {
        let start_time = PreciseTime::now();

        let result = self.barrier.wait();

        trace!("worker sync in {}", start_time.to(PreciseTime::now()));

        result.is_leader()
    }
}

impl<K, V> Worker<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    pub fn run(&self) -> Result<()> {
        debug!("worker-{} enter", self.id);

        while !self.is_terminated() {
            let start_time = PreciseTime::now();

            debug!("worker-{} STAGE 0: collect tasks", self.id);

            let (tree_root, current_tasks) = self.collect_task()?;

            self.sync();

            debug!(
                "worker-{} STAGE 0: recieved {} tasks in {}",
                self.id,
                current_tasks.len(),
                start_time.to(PreciseTime::now())
            );

            debug!("worker-{} STAGE 1: search for leaves", self.id);
        }

        debug!("worker-{} terminated", self.id);

        Ok(())
    }

    fn collect_task(&self) -> Result<(Arc<Box<Node + Send + Sync>>, TaskBatch<K, V>)> {
        let (tree_root, task_batch) = self.receiver.recv()?;
        let current_tasks = if task_batch.is_empty() || !self.is_master() {
            task_batch
        } else {
            let num_workers = self.senders.len();

            debug_assert!(!self.senders.is_empty());
            debug_assert!(task_batch.len() >= num_workers);

            let tasks_per_worker = (task_batch.len() + num_workers - 1) / num_workers;
            let mut current_tasks = None;

            for ((worker_id, tasks_batch_for_worker), sender) in task_batch
                .into_inner()
                .into_iter()
                .chunks(tasks_per_worker)
                .into_iter()
                .map(TaskBatch::from)
                .enumerate()
                .zip(self.senders.iter())
            {
                if worker_id == MASTER_WORKER_ID {
                    current_tasks = Some(tasks_batch_for_worker);
                } else {
                    sender.send((tree_root.clone(), tasks_batch_for_worker))?;
                }
            }

            current_tasks.unwrap()
        };

        Ok((tree_root, current_tasks))
    }
}
