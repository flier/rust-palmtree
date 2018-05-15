use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Barrier};

use itertools::Itertools;
use time::PreciseTime;

use errors::Result;
use task::TaskBatch;

const MASTER_WORKER_ID: usize = 0;

pub struct Worker<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    id: usize,
    terminated: AtomicBool,
    receiver: Receiver<TaskBatch<K, V>>,
    senders: Vec<Sender<TaskBatch<K, V>>>,
    barrier: Arc<Barrier>,
}

impl<K, V> Worker<K, V>
where
    K: 'static + Send + Sync,
    V: 'static + Send + Sync,
{
    pub fn new(
        id: usize,
        receiver: Receiver<TaskBatch<K, V>>,
        senders: Vec<Sender<TaskBatch<K, V>>>,
        barrier: Arc<Barrier>,
    ) -> Self {
        Worker {
            id,
            terminated: AtomicBool::new(false),
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

    pub fn run(&self) -> Result<()> {
        debug!("worker-{} enter", self.id);

        while !self.is_terminated() {
            let start_time = PreciseTime::now();

            // Stage 0, collect work batch and partition

            debug!("worker-{} STAGE 0: collect tasks", self.id);

            let task_batch = self.collect_task()?;

            if task_batch.is_empty() {
                continue;
            }

            self.sync();

            debug!(
                "worker-{} STAGE 0: recieved {} tasks in {}",
                self.id,
                task_batch.len(),
                start_time.to(PreciseTime::now())
            );
        }

        debug!("worker-{} terminated", self.id);

        Ok(())
    }

    fn sync(&self) {
        let start_time = PreciseTime::now();

        self.barrier.wait();

        trace!("worker sync in {}", start_time.to(PreciseTime::now()))
    }

    fn collect_task(&self) -> Result<TaskBatch<K, V>> {
        let task_batch = self.receiver.recv()?;

        if task_batch.is_empty() || !self.is_master() {
            Ok(task_batch)
        } else {
            let tasks_per_worker = task_batch.len() / self.senders.len();
            let mut my_task_batch = None;

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
                    my_task_batch = Some(tasks_batch_for_worker);
                } else {
                    sender.send(tasks_batch_for_worker)?;
                }
            }

            Ok(my_task_batch.unwrap())
        }
    }
}
