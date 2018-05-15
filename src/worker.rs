use std::sync::mpsc::{Receiver, Sender};

use task::TaskBatch;

pub struct Worker<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    id: usize,
    receiver: Receiver<TaskBatch<K, V>>,
    senders: Vec<Sender<TaskBatch<K, V>>>,
}

impl<K, V> Worker<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub fn new(
        id: usize,
        receiver: Receiver<TaskBatch<K, V>>,
        senders: Vec<Sender<TaskBatch<K, V>>>,
    ) -> Self {
        Worker {
            id,
            receiver,
            senders,
        }
    }

    pub fn run(&self) {}
}
