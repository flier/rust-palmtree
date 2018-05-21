use std::ops::Deref;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Barrier};

use itertools::Itertools;
use time::PreciseTime;

use errors::Result;
use node::Node;
use task::TaskBatch;

const MASTER_WORKER_ID: usize = 0;

pub type TreeTasks<K, V> = (Arc<Box<Node<K> + Send + Sync>>, TaskBatch<K, V>);
pub type TaskSender<K, V> = Sender<TreeTasks<K, V>>;
pub type TaskReceiver<K, V> = Receiver<TreeTasks<K, V>>;

#[derive(Clone)]
pub struct Worker<K, V> {
    inner: Rc<Inner<K, V>>,
}

pub struct Inner<K, V> {
    id: usize,
    terminated: Arc<AtomicBool>,
    receiver: TaskReceiver<K, V>,
    senders: Vec<TaskSender<K, V>>,
    barrier: Arc<Barrier>,
}

impl<K, V> Deref for Worker<K, V> {
    type Target = Inner<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> Worker<K, V> {
    pub fn new(
        id: usize,
        terminated: Arc<AtomicBool>,
        receiver: TaskReceiver<K, V>,
        senders: Vec<TaskSender<K, V>>,
        barrier: Arc<Barrier>,
    ) -> Self {
        Worker {
            inner: Rc::new(Inner {
                id,
                terminated,
                receiver,
                senders,
                barrier,
            }),
        }
    }
}

enum State<K, V> {
    Collect,
    Search(Arc<Box<Node<K> + Send + Sync>>, TaskBatch<K, V>),
}

impl<K, V> Worker<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    pub fn run(&mut self) -> Result<()> {
        let mut state = State::Collect;

        debug!("worker-{} enter", self.id);

        while !self.is_terminated() {
            state = self.next_state(state)?;
        }

        debug!("worker-{} terminated", self.id);

        Ok(())
    }
}

impl<K, V> Inner<K, V> {
    fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Relaxed)
    }

    fn is_master(&self) -> bool {
        self.id == MASTER_WORKER_ID
    }

    fn sync(&self) -> bool {
        let start_time = PreciseTime::now();

        let result = self.barrier.wait();

        trace!("worker sync in {}", start_time.to(PreciseTime::now()));

        result.is_leader()
    }
}

impl<K, V> Inner<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    fn next_state(&self, state: State<K, V>) -> Result<State<K, V>> {
        let start_time = PreciseTime::now();

        match state {
            State::Collect => {
                debug!("worker-{} STAGE 0: collect tasks", self.id);

                let (tree_root, current_tasks) = self.collect_task()?;

                self.sync();

                debug!(
                    "worker-{} STAGE 0: recieved {} tasks in {}",
                    self.id,
                    current_tasks.len(),
                    start_time.to(PreciseTime::now())
                );

                Ok(State::Search(tree_root, current_tasks))
            }
            State::Search(tree_root, current_tasks) => {
                debug!("worker-{} STAGE 1: search for leaves", self.id);

                let target_nodes = current_tasks.into_inner().into_iter().map(|task| {
                    let target_node = self.search(tree_root.clone(), task.key());
                });

                Ok(State::Collect)
            }
        }
    }

    fn collect_task(&self) -> Result<(Arc<Box<Node<K> + Send + Sync>>, TaskBatch<K, V>)> {
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

    fn search(
        &self,
        tree_root: Arc<Box<Node<K> + Send + Sync>>,
        key: &K,
    ) -> Option<Arc<Box<Node<K> + Send + Sync>>> {
        let mut cur_node = tree_root;

        loop {
            let idx = cur_node.search(key)?;
        }
    }
}
