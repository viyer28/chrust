// File: node.rs
//
// The purpose of this file is to perform all read and write operations
// of the local storage of the node.

use crate::handler::M;
use crate::hash;
use std::collections::HashMap;

/// Contains all local node storage, including node metadata, finger table,
/// successor list, key/value store, replica key/value stories, pending queries,
/// and the last failed successor
pub struct Node {
  id: NodeEntry,
  finger_table: Vec<FingerEntry>,
  successor: NodeEntry,
  predecessor: Option<NodeEntry>,
  successor_list: Vec<SuccessorEntry>,
  store: HashMap<String, String>,
  replica_store: HashMap<i32, HashMap<String, String>>,
  current_queries: HashMap<i32, QueryType>,
  last_failed_successor: Option<SuccessorEntry>,
}

impl Node {
  /// Returns a new Node object
  ///
  /// # Arguments
  ///
  /// * `m` - The number of entries in the finger table
  /// * `node_name` - The node's name
  /// * `id` - The hashed key of the node
  /// * `tau` - The number of entries in the successor list
  pub fn new(m: i32, node_name: &str, id: i32, tau: i32) -> Node {
    let node: NodeEntry = NodeEntry::new(id, node_name);
    let mut new_finger_table = Vec::new();
    let mut new_successor_list = Vec::new();

    // initialize finger table
    for i in 1..(m + 1) {
      let start: i32 = (id + i32::pow(2, (i - 1) as u32)) % i32::pow(2, m as u32);
      new_finger_table.push(FingerEntry::new(start, id, node_name));
    }

    // initialize successor list
    for _ in 1..(tau + 1) {
      new_successor_list.push(SuccessorEntry::new(id, node_name));
    }

    // initialize as if node is the only node in the ring
    Node {
      id: node,
      finger_table: new_finger_table,
      successor: NodeEntry::new(id, node_name),
      predecessor: Some(NodeEntry::new(id, node_name)),
      successor_list: new_successor_list,
      store: HashMap::new(),
      replica_store: HashMap::new(),
      current_queries: HashMap::new(),
      last_failed_successor: None,
    }
  }

  /// Returns the data for a given key, if it exists in the local store
  ///
  /// # Arguments
  ///
  /// * `key` - the key being queried
  pub fn get(&self, key: &str) -> Option<&String> {
    self.store.get(key)
  }

  /// Sets the data for a given key in the local store
  ///
  /// # Arguments
  ///
  /// * `key` - the key being stored
  /// * `value` - the value being stored
  pub fn set(&mut self, key: String, value: String) {
    self.store.insert(key, value);
  }

  /// Returns the node's successor
  pub fn get_successor(&self) -> NodeEntry {
    NodeEntry::new(self.successor.id, &self.successor.node_name)
  }

  /// Sets the node's successor
  ///
  /// # Arguments
  ///
  /// * `succ` - the new successor being set
  pub fn set_successor(&mut self, succ: NodeEntry) {
    self.finger_table[0].node = NodeEntry::new(succ.id, &succ.node_name);
    self.successor_list[0].node = NodeEntry::new(succ.id, &succ.node_name);
    self.successor_list[0].failed = false;
    self.successor = succ;
  }

  /// Returns the node's predecessor, if it exists
  pub fn get_predecessor(&self) -> Option<NodeEntry> {
    match &self.predecessor {
      Some(pred) => Some(NodeEntry::new(pred.id, &pred.node_name)),
      None => None,
    }
  }

  /// Sets the node's predecessor
  /// Returns a TransferType informing the handler to tranfer keys if necessary
  ///
  /// # Arguments
  ///
  /// * `pred` - the new predecessor being set
  pub fn set_predecessor(&mut self, pred: Option<NodeEntry>) -> TransferType {
    match &self.predecessor {
      None => {
        // get keys from range (n.predecessor to n] from successor
        if let Some(new_pred) = pred {
          self.predecessor = Some(NodeEntry::new(new_pred.id, &new_pred.node_name));
          TransferType::Get(new_pred.id, self.id.id)
        } else {
          TransferType::Nothing
        }
      }
      Some(old_pred) => {
        // remove + send keys from range (n.oldpredecessor to n.newpredecessor] to new predecessor
        if let Some(new_pred) = pred {
          let old_id = old_pred.id;
          self.predecessor = Some(NodeEntry::new(new_pred.id, &new_pred.node_name));
          if new_pred.node_name != self.id.node_name {
            TransferType::Send(old_id, new_pred.id, new_pred.node_name)
          } else {
            TransferType::Nothing
          }
        } else {
          self.predecessor = None;
          TransferType::Nothing
        }
      }
    }
  }

  /// Get the node's hashed id
  pub fn get_id(&self) -> i32 {
    self.id.id
  }

  /// Find the predecessor of a given id
  /// If it is not the current node, find the closest preceding finger
  /// to route the query to
  ///
  /// # Arguments
  ///
  /// * `id` - key being queried
  pub fn find_predecessor(&self, id: i32) -> (bool, NodeEntry) {
    if hash::in_range(id, self.id.id, self.successor.id, true) {
      (true, NodeEntry::new(self.id.id, &self.id.node_name))
    } else {
      (false, self.closest_preceding_finger(id))
    }
  }

  /// Pushes a new query to a queue of outstanding queries
  ///
  /// # Arguments
  ///
  /// * `query` - key being queried
  /// * `type` - the type of query being performed
  pub fn push_query(&mut self, query: i32, r#type: QueryType) {
    self.current_queries.insert(query, r#type);
  }

  /// Retrieves and removes an outstanding query from the queue,
  /// if it exists
  ///
  /// # Arguments
  ///
  /// * `query` - key being queried
  pub fn pop_query(&mut self, query: i32) -> Option<QueryType> {
    self.current_queries.remove(&query)
  }

  /// Sets the successor's predecessor as a new successor if its key is closer
  ///
  /// # Arguments
  ///
  /// * `pred_id` - the successor's predecessor (id)
  /// * `pred_name` - the successor's predecessor (name)
  pub fn stabilize_successor(&mut self, pred_id: i32, pred_name: &str) {
    if hash::in_range(pred_id, self.id.id, self.successor.id, false) {
      self.set_successor(NodeEntry::new(pred_id, pred_name))
    }
  }

  /// A node thinks it might be the current node's predecessor
  /// Returns a TransferType informing handler of how/whether to transfer keys
  /// If the previous predecessor was nil (the current node is new),
  /// transfer keys from the successor
  /// If the previous predecessor failed,
  /// transfer data from locally stored replicas
  /// Otherwise, transfer excess keys to old predecessor
  ///
  /// # Arguments
  ///
  /// * `node_id` - the node suggesting it may be the predecessor (id)
  /// * `node_name` - the node suggesting it may be the predecessor (name)
  pub fn stabilize_predecessor(
    &mut self,
    node_id: i32,
    node_name: &str,
    failed: bool,
  ) -> TransferType {
    match &self.predecessor {
      Some(pred) => {
        if failed {
          let id = pred.id;
          self.transfer_from_replicas(node_id, id);
          self.predecessor = Some(NodeEntry::new(node_id, node_name));
          TransferType::Duplicate
        } else if hash::in_range(node_id, pred.id, self.id.id, false) {
          self.set_predecessor(Some(NodeEntry::new(node_id, node_name)))
        } else {
          TransferType::Nothing
        }
      }
      None => self.set_predecessor(Some(NodeEntry::new(node_id, node_name))),
    }
  }

  /// Get the start of a finger table entry's interval
  ///
  /// # Arguments
  ///
  /// * `i` - index in finger table
  pub fn get_finger_start(&self, i: i32) -> i32 {
    self.finger_table[i as usize].start
  }

  /// Set a new finger table entry
  ///
  /// # Arguments
  ///
  /// * `i` - index in finger table
  /// * `node` - new finger table entry
  pub fn set_finger(&mut self, i: i32, node: NodeEntry) {
    self.finger_table[i as usize].node = node;
  }

  /// Remove and transfer local keys/values from a given range of keys
  /// Returns keys and values as seperate vectors
  ///
  /// # Arguments
  ///
  /// * `min` - lower bound of keys to be transferred
  /// * `max` - upper bound of keys to be transferred
  pub fn transfer_kvs_range(&mut self, min: i32, max: i32) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for k in self
      .store
      .keys()
      .filter(|key| hash::in_range(hash::hash(key), min, max, true))
    {
      keys.push(String::from(k));
    }
    for k in &keys {
      if let Some(v) = self.store.remove(k) {
        values.push(v);
      }
    }

    (keys, values)
  }

  /// Get the indices of all live successors in the successor list
  /// Returns a vector of indices
  pub fn live_successor_indexes(&self) -> Vec<usize> {
    let mut indexes = Vec::new();
    for (i, s) in self.successor_list.iter().enumerate() {
      if !s.failed && i != (self.successor_list.len() - 1) {
        indexes.push(i);
      }
    }
    indexes
  }

  /// Returns the successor's id at a given index in the successor list
  ///
  /// # Arguments
  ///
  /// * `i` - index in the successor list
  pub fn successor_at_index(&self, i: usize) -> i32 {
    self.successor_list[i].node.id
  }

  /// Refresh a successor in the successor list
  /// Returns whether or not the successor should receive a new replica
  ///
  /// # Arguments
  ///
  /// * `i` - index in the successor list
  /// * `succ` - new successor entry
  pub fn fix_successor(&mut self, i: i32, succ: NodeEntry) -> bool {
    let old_succ = (&self.successor_list[i as usize].node.node_name).to_string();
    let was_failed = self.successor_list[i as usize].failed;
    self.successor_list[i as usize].node = succ;
    self.successor_list[i as usize].failed = false;
    // if the successor is different or goes from dead to live and is not self, duplicate data to that successor
    (old_succ != self.successor_list[i as usize].node.node_name || was_failed)
      && self.successor_list[i as usize].node.node_name != self.id.node_name
  }

  /// Set a new key/value store replica for a given node's id
  ///
  /// # Arguments
  ///
  /// * `id` - replicated node's id
  /// * `kvs` - replicated node's key/value store
  pub fn set_for_replica(&mut self, id: i32, kvs: HashMap<String, String>) {
    self.replica_store.insert(id, kvs);
  }

  /// Get the live successors from the successor list
  /// Returns a vector of successor nodes
  pub fn live_successors(&self) -> Vec<NodeEntry> {
    let mut successors = Vec::new();
    for succ in self.successor_list.iter().filter(|s| !s.failed) {
      successors.push(NodeEntry::new(succ.node.id, &succ.node.node_name));
    }
    successors
  }

  /// Duplicates current node's store
  /// Returns a tuple of keys and values
  pub fn duplicate_store(&self) -> (Vec<String>, Vec<String>) {
    (
      self.store.keys().map(|k| k.to_string()).collect(),
      self.store.values().map(|v| v.to_string()).collect(),
    )
  }

  /// Handles the failure of a successor
  /// Set the successor as the last failed successor,
  /// use the next successor list entry as the new successor,
  /// and shift the successor list
  pub fn successor_failure(&mut self) {
    self.last_failed_successor = Some(self.successor_list.remove(0));
    self
      .successor_list
      .push(SuccessorEntry::new(self.id.id, &self.id.node_name));
    self.set_successor(NodeEntry::new(
      self.successor_list[0].node.id,
      &self.successor_list[0].node.node_name,
    ));
  }

  /// A debug helper function for displaying the node's successor list
  // pub fn display_ring(&self) {
  //   println!("{}'s RING", self.id.node_name);
  //   for (i, s) in self.successor_list.iter().enumerate() {
  //     println!("{}: {}", i, s.node.node_name);
  //   }
  // }

  /// Returns the last failed successor, if it exists
  pub fn get_failed_successor(&self) -> Option<NodeEntry> {
    if let Some(fail) = &self.last_failed_successor {
      Some(NodeEntry::new(fail.node.id, &fail.node.node_name))
    } else {
      None
    }
  }

  /// Resets the last failed successor to nil
  pub fn reset_failed_successor(&mut self) {
    self.last_failed_successor = None;
  }

  /// Finds the closest preceding finger for a given key
  ///
  /// # Arguments
  ///
  /// * `id` - the key being queried
  fn closest_preceding_finger(&self, id: i32) -> NodeEntry {
    for i in (0..M).rev() {
      if hash::in_range(self.finger_table[i as usize].node.id, self.id.id, id, false) {
        return NodeEntry::new(
          self.finger_table[i as usize].node.id,
          &self.finger_table[i as usize].node.node_name,
        );
      }
    }
    NodeEntry::new(self.id.id, &self.id.node_name)
  }

  /// Transfer keys from stored replicas to the local store
  /// when a predecessor fails
  ///
  /// # Arguments
  ///
  /// * `min` - lower bound of keys to transfer
  /// * `max` - upper bound of keys to transfer
  fn transfer_from_replicas(&mut self, min: i32, max: i32) {
    for i in self.replica_store.keys().collect::<Vec<&i32>>() {
      if hash::in_range(*i, min, max, true) {
        if let Some(kvs) = self.replica_store.get(i) {
          self
            .store
            .extend(kvs.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        }
      }
    }
  }
}

/// Finger data for the node's finger table
struct FingerEntry {
  start: i32,
  node: NodeEntry,
}

impl FingerEntry {
  /// Returns a new FingerEntry object
  ///
  /// # Arguments
  ///
  /// * `start` - start of the key range that the finger is the successor of
  /// * `id` - the node that contains the requested keys
  /// * `node_name` - the minimum key to be transferred
  fn new(start: i32, id: i32, node_name: &str) -> FingerEntry {
    FingerEntry {
      start,
      node: NodeEntry::new(id, node_name),
    }
  }
}

/// Node metadata
pub struct NodeEntry {
  pub id: i32,
  pub node_name: String,
}

impl NodeEntry {
  /// Returns a new NodeEntry object
  ///
  /// # Arguments
  ///
  /// * `id` - the hashed key of the node's name
  /// * `node_name` - the node's name
  pub fn new(id: i32, node_name: &str) -> NodeEntry {
    NodeEntry {
      id,
      node_name: String::from(node_name),
    }
  }
}

/// Successor entry metadata
struct SuccessorEntry {
  node: NodeEntry,
  failed: bool,
}

impl SuccessorEntry {
  /// Returns a new SuccessorEntry object
  ///
  /// # Arguments
  ///
  /// * `id` - the hashed key of the successor's name
  /// * `node_name` - the successor's name
  fn new(id: i32, node_name: &str) -> SuccessorEntry {
    SuccessorEntry {
      node: NodeEntry::new(id, node_name),
      failed: true,
    }
  }
}

/// Types of key queries a node can make to other nodes
pub enum QueryType {
  JoinAck,
  FixFinger,
  Get(String),
  Set(String, String),
  FixSuccessor,
}

/// Ways that a node can transfer/be transferred keys from other nodes
/// after a predecessor change
pub enum TransferType {
  Get(i32, i32),          // get in range
  Send(i32, i32, String), // send in range, to predecessor name
  Duplicate,
  Nothing,
}
