use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Barrier};

use itertools::Itertools;
use time::PreciseTime;

use errors::Result;
use node::{Base, Leaf, Node};
use task::{NodeMod, TaskBatch, TreeOp};

const MASTER_WORKER_ID: usize = 0;

pub type TreeTasks<K, V> = (Arc<Node<K, V>>, TaskBatch<K, V>);
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
    Search(Arc<Node<K, V>>, TaskBatch<K, V>),
    Update(Arc<Node<K, V>>, HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>>),
}

impl<K, V> Worker<K, V>
where
    K: 'static + Send + Sync + Clone + Default + Hash + Ord,
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
    K: 'static + Send + Sync + Clone + Default + Hash + Ord,
    V: 'static + Send + Sync,
{
    fn next_state(&self, state: State<K, V>) -> Result<State<K, V>> {
        let start_time = PreciseTime::now();

        let next_state = match state {
            State::Collect => {
                debug!("worker-{} STAGE 0: collect tasks", self.id);

                let (tree_root, current_tasks) = self.collect_task()?;

                debug!(
                    "worker-{} STAGE 0: recieved {} tasks in {}",
                    self.id,
                    current_tasks.len(),
                    start_time.to(PreciseTime::now())
                );

                State::Search(tree_root, current_tasks)
            }
            State::Search(tree_root, current_tasks) => {
                debug!("worker-{} STAGE 1: search for leaves", self.id);

                let searched_tasks = self.search_tasks(tree_root.clone(), current_tasks);
                let current_tasks = self.redistribute_leaf_tasks(searched_tasks)?;

                debug!(
                    "worker-{} STAGE 1: finished in {}",
                    self.id,
                    start_time.to(PreciseTime::now())
                );

                State::Update(tree_root, current_tasks)
            }
            State::Update(tree_root, mut updates) => {
                debug!("worker-{} STAGE 2: process leaves", self.id);

                self.collect_updates(&mut updates);

                let leaf_mods = self.resolve_hazards(updates);

                debug!(
                    "worker-{} STAGE 2: finished in {}",
                    self.id,
                    start_time.to(PreciseTime::now())
                );

                State::Collect
            }
        };

        self.sync();

        Ok(next_state)
    }

    fn collect_task(&self) -> Result<(Arc<Node<K, V>>, TaskBatch<K, V>)> {
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

    fn search_tasks(
        &self,
        tree_root: Arc<Node<K, V>>,
        task_batch: TaskBatch<K, V>,
    ) -> HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>> {
        task_batch
            .into_inner()
            .into_iter()
            .fold(HashMap::new(), |mut tasks, op| {
                let target_node = self.search(tree_root.clone(), op.key());

                tasks.entry(target_node).or_insert_with(Vec::new).push(op);

                tasks
            })
    }

    /// Return the leaf node that contains the key
    fn search(&self, tree_root: Arc<Node<K, V>>, key: &K) -> Arc<Node<K, V>> {
        let mut cur_node = tree_root;

        loop {
            if let Some(inner) = cur_node.clone().as_inner() {
                cur_node = inner.search(key);
            } else {
                return cur_node;
            }
        }
    }

    fn redistribute_leaf_tasks(
        &self,
        tasks: HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>>,
    ) -> Result<HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>>> {
        let mut current_tasks = HashMap::new();

        for (target_node, tree_ops) in tasks {
            let worker_id = target_node.id() % self.senders.len();

            if worker_id == self.id {
                current_tasks.insert(target_node, tree_ops);
            } else {
                self.senders
                    .get(worker_id)
                    .unwrap()
                    .send((target_node, TaskBatch::from(tree_ops)))?;
            }
        }

        Ok(current_tasks)
    }

    fn collect_updates(&self, updates: &mut HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>>) {
        for (target_node, tree_ops) in self.receiver.try_iter() {
            updates
                .entry(target_node)
                .or_insert_with(Vec::new)
                .extend(tree_ops.into_inner());
        }
    }

    fn resolve_hazards(
        &self,
        current_tasks: HashMap<Arc<Node<K, V>>, Vec<TreeOp<K, V>>>,
    ) -> HashMap<Arc<Node<K, V>>, Vec<NodeMod<K, V>>> {
        let mut changed: HashMap<K, Arc<V>> = HashMap::new();
        let mut deleted: HashSet<K> = HashSet::new();
        let mut leaf_mods = HashMap::new();

        for (target_node, tree_ops) in current_tasks {
            for op in tree_ops {
                match op {
                    TreeOp::Find {
                        ref key,
                        ref result,
                        ..
                    } => {
                        if deleted.contains(&key) {
                            continue;
                        }

                        if let Some(value) = changed.get(key) {
                            result.store(Arc::into_raw(value.clone()) as *mut V, Ordering::Relaxed)
                        } else if let Some(value) =
                            target_node.as_leaf().and_then(|leaf| leaf.search(key))
                        {
                            result.store(Arc::into_raw(value.clone()) as *mut V, Ordering::Relaxed)
                        }
                    }
                    TreeOp::Insert {
                        ref key, ref value, ..
                    } => {
                        deleted.remove(key);
                        changed.insert(key.clone(), value.clone());

                        leaf_mods
                            .entry(target_node.clone())
                            .or_insert_with(Vec::new)
                            .push(NodeMod::add(key.clone(), value.clone()));
                    }
                    TreeOp::Remove { ref key, .. } => {
                        changed.remove(key);
                        deleted.insert(key.clone());

                        leaf_mods
                            .entry(target_node.clone())
                            .or_insert_with(Vec::new)
                            .push(NodeMod::remove(key.clone()));
                    }
                }
            }
        }

        leaf_mods
    }

    fn modify_node(&self, node: Arc<Node<K, V>>, mods: Vec<NodeMod<K, V>>) -> Vec<NodeMod<K, V>> {
        if node.is_leaf() {
            self.modify_node_leaf(node, mods)
        } else {
            self.modify_node_inner(node, mods)
        }
    }

    fn modify_node_leaf(
        &self,
        node: Arc<Node<K, V>>,
        mods: Vec<NodeMod<K, V>>,
    ) -> Vec<NodeMod<K, V>> {
        let this = node.clone();
        let leaf = node.as_leaf().unwrap();
        let mut leaf_keys = leaf.keys.clone();
        let mut leaf_values = leaf.values.clone();

        for item in mods {
            match item {
                NodeMod::Add { items, .. } => for (key, value) in items {
                    match leaf_keys.binary_search(&key) {
                        Ok(idx) => {
                            leaf_values[idx] = value;
                        }
                        Err(idx) => {
                            leaf_keys.insert(idx, key);
                            leaf_values.insert(idx, value);
                        }
                    }
                },
                NodeMod::Remove { keys, .. } => {
                    for key in keys {
                        if let Ok(idx) = leaf_keys.binary_search(&key) {
                            leaf_keys.swap_remove(idx);
                            leaf_values.swap_remove(idx);
                        }
                    }
                }
            }
        }

        if leaf_keys.len() > Leaf::<K, V>::max_slot() {
            let item_per_node = Leaf::<K, V>::max_slot() / 2;

            let nodes = leaf_keys
                .into_iter()
                .chunks(item_per_node)
                .into_iter()
                .zip(leaf_values.into_iter().chunks(item_per_node).into_iter())
                .map(|(keys, values)| {
                    Arc::new(Node::Leaf(Leaf {
                        base: Base::new(leaf.base.parent.clone(), leaf.base.level),
                        keys: keys.into_iter().collect(),
                        values: values.into_iter().collect(),
                    }))
                })
                .collect::<Vec<_>>();

            trace!(
                "split leaf node #{}: {:?}",
                leaf.base.id,
                nodes
                    .iter()
                    .map(|node| format!("Leaf#{}", node.id))
                    .collect::<Vec<_>>()
            );

            vec![
                NodeMod::Add {
                    items: Vec::new(),
                    nodes,
                },
            ]
        } else if leaf_keys.len() < Leaf::<K, V>::max_slot() / 4 {
            vec![
                NodeMod::Remove {
                    keys: Vec::new(),
                    nodes: vec![this],
                },
            ]
        } else {
            vec![]
        }
    }

    fn modify_node_inner(
        &self,
        node: Arc<Node<K, V>>,
        mods: Vec<NodeMod<K, V>>,
    ) -> Vec<NodeMod<K, V>> {
        let inner = node.as_inner().unwrap();
        let parent = inner.base.parent.clone();

        vec![]
    }
}
