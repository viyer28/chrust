// File: handler.rs
//
// The purpose of this file is to handle all messages sent and received
// to the external network.

extern crate chan;
extern crate zmq;
use crate::msg;
use crate::node;
use node::{NodeEntry, QueryType, TransferType};
use serde::Serialize;
extern crate parking_lot;
use crate::hash;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

// M defines the number of keys in the ring topology (2^M)
pub const M: i32 = 8;
// The number of pongs a node can miss before it's predecessor declares it failed
pub const FAILURE_THRESHOLD: i32 = 2;
// Period to stabilize ring (1s)
pub const STABILIZE_TIME: u32 = 1000;

/// Automatic reference counted wrapper for a RWLock contained the handler data
pub struct Handler {
    inner: Arc<RwLock<HandlerInner>>,
}

/// Contains the node data, sockets to communicate with the network, and number of pongs it has missed from the next node in the ring
pub struct HandlerInner {
    connected: bool,
    node_name: String,
    peer_names: Vec<String>,
    sub_socket: zmq::Socket,
    req_socket: zmq::Socket,
    node: node::Node,
    pings: i32,
}

/// Interface for Handler
impl Handler {
    /// Returns a new Handler object
    ///
    /// # Arguments
    ///
    /// * `ctx` - A ZeroMQ Context for creating sockets
    /// * `node_name` - Name of the node
    /// * `pub_endpoint` - Endpoint for receiving messages
    /// * `router_endpoint` - Endpoint for sending messages
    /// * `peer` - List of other peers in the network
    pub fn new(
        ctx: zmq::Context,
        node_name: &str,
        pub_endpoint: &str,
        router_endpoint: &str,
        peer: Option<Vec<String>>,
    ) -> Handler {
        let sub_socket = ctx.socket(zmq::SUB).unwrap();
        sub_socket
            .connect(pub_endpoint)
            .expect("failed connecting subscriber");

        sub_socket
            .set_subscribe(node_name.as_bytes())
            .expect("failed subscribing");

        let req_socket = ctx.socket(zmq::REQ).unwrap();
        req_socket
            .connect(router_endpoint)
            .expect("failed connecting requester");
        req_socket
            .set_identity(node_name.as_bytes())
            .expect("failed requesting");

        let mut peer_names = Vec::new();
        if let Some(peers) = peer {
            peer_names.extend_from_slice(&peers)
        }

        // Length of successor list, defined in Chord as log(N)
        let tau: i32 = f64::ceil(f64::log2((peer_names.len() + 1) as f64)) as i32;
        let id = hash::hash(node_name);

        Handler {
            inner: Arc::new(RwLock::new(HandlerInner {
                connected: false,
                node_name: String::from(node_name),
                sub_socket,
                req_socket,
                peer_names,
                node: node::Node::new(M, node_name, id, tau),
                pings: 0,
            })),
        }
    }

    /// Listens to messages from the network
    pub fn listen_to_publisher(&mut self) {
        let lock = self.inner.clone();
        loop {
            // Read new message
            let read_inner_self = lock.read();
            let _address = read_inner_self.sub_socket.recv_string(0).unwrap().unwrap();
            let _skip = read_inner_self.sub_socket.recv_string(0).unwrap().unwrap();
            let contents = read_inner_self.sub_socket.recv_string(0).unwrap().unwrap();
            RwLockReadGuard::unlock_fair(read_inner_self);
            let m: msg::RcvdMessage = serde_json::from_str(&contents).unwrap();
            // Begin periodic stabilization after network detects node's existence
            if m.r#type == "hello" {
                self.periodic_stabilize();
            }
            // Handle new message
            let mut write_inner_self = lock.write();
            write_inner_self.handle_messages(m);
            RwLockWriteGuard::unlock_fair(write_inner_self);
        }
    }

    /// Periodically runs a set of tasks to return the ring of nodes to a stable state
    fn periodic_stabilize(&self) {
        let tick = chan::tick_ms(STABILIZE_TIME);

        let lock = self.inner.clone();
        thread::spawn(move || loop {
            chan_select! {
                // Every 1 second
                tick.recv() => {
                    let read_inner_self = lock.read();
                    // For debugging topology: read_inner_self.node.display_ring();
                    read_inner_self.stabilize_ring();
                    read_inner_self.ping_self();
                    read_inner_self.heal_partition();
                    RwLockReadGuard::unlock_fair(read_inner_self);
                    let mut write_inner_self = lock.write();
                    write_inner_self.fix_fingers();
                    write_inner_self.fix_successors();
                    write_inner_self.ping_successor();
                    RwLockWriteGuard::unlock_fair(write_inner_self);
                }
            }
        });
    }
}

/// Interface for HandlerInner
impl HandlerInner {
    /// A new node tries to join other nodes to form a complete ring
    fn join(&self) {
        for peer in self.peer_names.iter() {
            self.send_to_broker(&msg::Join::new(&self.node_name, &peer));
        }
    }

    /// Routes a new message through the broker
    ///
    /// # Arguments
    ///
    /// * `msg` - The new message
    fn send_to_broker<T: Serialize>(&self, msg: T) {
        let json = serde_json::json!(msg);
        let j = serde_json::to_string(&json).expect("cannot convert json to string");
        self.req_socket.send(&j, 0).expect("cannot send message");

        // Must receive acknowledgement from broker before next message can be read
        let _ = self.req_socket.recv_string(0).unwrap().unwrap();
    }

    /// Periodically verifies the current node's immediate successor
    pub fn stabilize_ring(&self) {
        let successor = self.node.get_successor();
        self.send_to_broker(&msg::GetPred::new(&self.node_name, &successor.node_name));
    }

    /// Periodically refreshes a random finger table entry
    pub fn fix_fingers(&mut self) {
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(1, M);
        let query_id = self.node.get_finger_start(i);
        self.node.push_query(query_id, QueryType::FixFinger);
        self.find_successor(query_id, &self.node_name, Some(i));
    }

    /// Periodically refreshes a random successor on the successor list
    pub fn fix_successors(&mut self) {
        let mut rng = rand::thread_rng();
        // indexes of live successors in array
        let indexes = self.node.live_successor_indexes();
        // get i in range of num live successors
        if !indexes.is_empty() {
            let i = rng.gen_range(0, indexes.len());
            let random_succ = indexes[i];
            let query_id = self.node.successor_at_index(random_succ) + 1;
            // find the successor of that one and put it in index + 1
            self.node.push_query(query_id, QueryType::FixSuccessor);
            self.find_successor(query_id, &self.node_name, Some((random_succ + 1) as i32));
        }
    }

    /// Periodically tries to rejoin the last failed successor to heal a partition
    pub fn heal_partition(&self) {
        if let Some(last_fail) = self.node.get_failed_successor() {
            self.send_to_broker(&msg::Rejoin::new(&self.node_name, &last_fail.node_name));
        }
    }

    /// Periodically sends message to self to switch the lock between period thread and ZeroMQ loop
    pub fn ping_self(&self) {
        self.send_to_broker(&msg::PingSelf::new(&self.node_name));
    }

    /// Periodically pings successor to make sure it is alive
    /// If confirmed dead (does not receive 2 pongs in a row):
    /// Takes the next successor in the successor list as its own and
    /// notifies the node that it is its new predecessor
    pub fn ping_successor(&mut self) {
        if self.pings < FAILURE_THRESHOLD {
            let successor = self.node.get_successor();
            if successor.node_name != self.node_name {
                self.pings += 1;
                self.send_to_broker(&msg::Ping::new(&self.node_name, &successor.node_name));
            }
        } else {
            self.node.successor_failure();
            let new_successor = self.node.get_successor();
            self.send_to_broker(&msg::Notify::new(
                &self.node_name,
                &new_successor.node_name,
                self.node.get_id(),
                true,
            ));
            self.pings = 0;
        }
    }

    /// Finds the successor for a given key query
    ///
    /// # Arguments
    ///
    /// * `query_id` - The key being searched
    /// * `src` - Name of the node that is searching for the key
    /// * `id` - Some queries have an id they need passed with the response
    fn find_successor(&self, query_id: i32, src: &str, id: Option<i32>) {
        match self.node.find_predecessor(query_id) {
            (true, _) => {
                let successor = self.node.get_successor();
                self.send_to_broker(&msg::FindSuccResponse::new(
                    &self.node_name,
                    &src,
                    &successor.node_name,
                    successor.id,
                    query_id,
                    id,
                ))
            }
            (false, next) => {
                self.send_to_broker(&msg::FindSucc::new(&src, &next.node_name, query_id, id))
            }
        }
    }

    /// Duplicates node's data to its successors
    fn duplicate_to_successors(&self) {
        for successor in self.node.live_successors() {
            if successor.node_name != self.node_name {
                let (keys, values) = self.node.duplicate_store();
                self.send_to_broker(&msg::Duplicate::new(
                    &self.node_name,
                    &successor.node_name,
                    self.node.get_id(),
                    keys,
                    values,
                ))
            }
        }
    }

    /// Handles a received message
    ///
    /// # Arguments
    ///
    /// * `msg` - The received message
    fn handle_messages(&mut self, msg: msg::RcvdMessage) {
        match &msg.r#type[..] {
            // Sends back a hello response
            "hello" => {
                if !self.connected {
                    self.send_to_broker(&msg::HelloResponse::new(&self.node_name));
                }
                self.connected = true;
                self.join();
            }

            // Sends back a set response and finds the successor of the key that will store the value
            "set" => {
                let id = msg.id.expect("set: needs id");
                let k = msg.key.expect("set: needs key");
                let v = msg.value.expect("set: needs value");

                self.send_to_broker(&msg::SetResponse::new(id, &k, &v));

                let query_id = hash::hash(&k);
                self.node.push_query(query_id, QueryType::Set(k, v));
                self.find_successor(query_id, &self.node_name, None);
            }

            // Finds the successor of the key that will retrieve the data if it exists
            "get" => {
                let id = msg.id.expect("get: needs id");
                let k = msg.key.expect("get: needs key");

                let query_id = hash::hash(&k);

                self.node.push_query(query_id, QueryType::Get(k));
                self.find_successor(query_id, &self.node_name, Some(id));
            }

            // Acknowledge a new node trying to join the ring
            "join" => {
                let src = msg.destination.expect("join: needs destination");
                let dest = msg.source.expect("join: needs source");

                self.send_to_broker(&msg::JoinAck::new(&src, &dest));
            }

            // Begin joining the ring by asking the acknowledger for its new successor
            "joinAck" => {
                let src = msg.source.expect("join: needs source");

                self.node.set_predecessor(None);
                self.node.push_query(self.node.get_id(), QueryType::JoinAck);
                self.send_to_broker(&msg::FindSucc::new(
                    &self.node_name,
                    &src,
                    self.node.get_id(),
                    None,
                ))
            }

            // Helps a node find the successor for a key query in the ring
            "findSucc" => {
                let query_id = msg.query_id.expect("findSucc: needs query_id");
                let id = msg.id;
                let src = msg.source.expect("findSucc: needs source");

                self.find_successor(query_id, &src, id);
            }

            // Handles the query response for a key in the ring
            "findSuccResponse" => {
                let node_name = msg.node_name.expect("findSuccResponse: needs node_name");
                let node_id = msg.node_id.expect("findSuccResponse: needs node_id");
                let query_id = msg.query_id.expect("findSuccResponse: needs query_id");

                match self.node.pop_query(query_id) {
                    // Sets the responder as its new successor; finished joining ring
                    Some(QueryType::JoinAck) => {
                        self.node.set_successor(NodeEntry::new(node_id, &node_name));
                    }
                    // Sets the responder as a successor in a finger table entry
                    Some(QueryType::FixFinger) => {
                        if let Some(id) = msg.id {
                            self.node
                                .set_finger(id, NodeEntry::new(node_id, &node_name));
                        }
                    }
                    // Tells the responder to respond to the Get
                    Some(QueryType::Get(k)) => {
                        if let Some(id) = msg.id {
                            self.send_to_broker(&msg::Retrieve::new(
                                &self.node_name,
                                &node_name,
                                &k,
                                id,
                            ))
                        }
                    }
                    // Tells the responder to store new data from a Set
                    Some(QueryType::Set(k, v)) => {
                        self.send_to_broker(&msg::Store::new(&self.node_name, &node_name, &k, &v))
                    }
                    // Sets the responder as a new entry in the successor list
                    Some(QueryType::FixSuccessor) => {
                        if let Some(i) = msg.id {
                            if self
                                .node
                                .fix_successor(i, NodeEntry::new(node_id, &node_name))
                            {
                                let (keys, values) = self.node.duplicate_store();
                                self.send_to_broker(&msg::Duplicate::new(
                                    &self.node_name,
                                    &node_name,
                                    self.node.get_id(),
                                    keys,
                                    values,
                                ))
                            }
                            // if the successor is different or goes from dead to live and is not self, duplicate data to that successor
                        }
                    }
                    None => (),
                }
            }

            // Returns its predecessor to a node trying to stabilize the ring
            "getPred" => {
                let src = msg.source.expect("getPred: needs source");

                match self.node.get_predecessor() {
                    Some(predecessor) => self.send_to_broker(&msg::GetPredResponse::new(
                        &self.node_name,
                        &src,
                        Some(predecessor.id),
                        Some(predecessor.node_name),
                    )),
                    None => self.send_to_broker(&msg::GetPredResponse::new(
                        &self.node_name,
                        &src,
                        None,
                        None,
                    )),
                }
            }

            // Uses the predecessor response to determine who its current successor is, then informs that successor
            "getPredResponse" => {
                if let Some(pred_id) = msg.pred_id {
                    if let Some(pred_name) = msg.pred_name {
                        self.node.stabilize_successor(pred_id, &pred_name);
                    }
                }
                let successor = self.node.get_successor();

                self.send_to_broker(&msg::Notify::new(
                    &self.node_name,
                    &successor.node_name,
                    self.node.get_id(),
                    false,
                ))
            }

            // A node thinks it is the current node's new predecessor
            // Transfer keys to the predecessor if it is a new predecessor
            // Or if the current node does not have a predecessor yet (because it is just joining the ring), get keys from its successor
            "notify" => {
                let node_name = msg.source.expect("notify: needs source");
                let node_id = msg.node_id.expect("notify: needs node_id");
                let failed = msg.failed.expect("notify: needs failed");

                let transfer = self.node.stabilize_predecessor(node_id, &node_name, failed);

                match transfer {
                    TransferType::Get(min, max) => {
                        let successor = self.node.get_successor();
                        self.send_to_broker(&msg::TransferRequest::new(
                            &self.node_name,
                            &successor.node_name,
                            min,
                            max,
                        ));
                    }
                    TransferType::Send(min, max, pred) => {
                        let (keys, values) = self.node.transfer_kvs_range(min, max);
                        self.send_to_broker(&msg::TransferKeys::new(
                            &self.node_name,
                            &pred,
                            keys,
                            values,
                        ));
                    }
                    TransferType::Duplicate => {
                        self.duplicate_to_successors();
                    }
                    TransferType::Nothing => (),
                }
            }

            // Retrieve data from node to send a GetResponse to the client
            "retrieve" => {
                let id = msg.id.expect("retrieve: needs id");
                let k = msg.key.expect("retieve: needs key");

                match self.node.get(&k) {
                    Some(v) => self.send_to_broker(&msg::GetSuccessResponse::new(id, &k, &v)),
                    None => self.send_to_broker(&msg::GetFailResponse::new(id, &k)),
                }
            }

            // Store data in node from a Set
            "store" => {
                let k = msg.key.expect("store: needs key");
                let v = msg.value.expect("store: needs value");

                self.node.set(k, v);

                self.duplicate_to_successors();
            }

            // A request from a node to get a range of keys from the current node
            // Remove and transfer that data back to the requester
            "transferRequest" => {
                let src = msg.source.expect("transferRequest: needs source");
                let min = msg.min.expect("transferRequest: needs min");
                let max = msg.max.expect("transferRequest: needs max");

                let (keys, values) = self.node.transfer_kvs_range(min, max);
                self.send_to_broker(&msg::TransferKeys::new(&self.node_name, &src, keys, values));
            }

            // Store new data from a transfer and duplicate that data to successors
            "transferKeys" => {
                let keys = msg.keys.expect("transferKeys: need keys");
                let values = msg.values.expect("transferKeys: need values");

                for (k, v) in keys.iter().zip(values.iter()) {
                    self.node.set(k.to_string(), v.to_string());
                }

                self.duplicate_to_successors();
            }

            // Store duplicated data in a local replica
            "duplicate" => {
                let id = msg.id.expect("duplicate: need id");
                let keys = msg.keys.expect("duplicate: need keys");
                let values = msg.values.expect("duplicate: need values");

                let mut new_kvs = HashMap::new();
                for (k, v) in keys.iter().zip(values.iter()) {
                    new_kvs.insert(k.to_string(), v.to_string());
                }
                self.node.set_for_replica(id, new_kvs);
            }

            // Received ping from predecessor checking if current node is alive
            "ping" => {
                let src = msg.source.expect("ping: need source");
                self.send_to_broker(&msg::Pong::new(&self.node_name, &src));
            }

            // Received ping from successor confirming it is alive
            "pong" => {
                self.pings = 0;
            }

            // Received ping from self to keep locks flowing from periodic thread to broker loop
            "pingSelf" => {
                self.send_to_broker(&msg::PongSelf::new(&self.node_name));
            }

            // Received pong from self
            "pongSelf" => {}

            // Request from a partitioned node to rejoin the ring
            "rejoin" => {
                let src = msg.destination.expect("rejoin: needs destination");
                let dest = msg.source.expect("rejoin: needs source");

                self.send_to_broker(&msg::RejoinAck::new(&src, &dest));
            }

            // Acknowledgement from a node that the current node wants to rejoin, requests a successor from the partitioned node's ring
            "rejoinAck" => {
                let src = msg.source.expect("rejoin: needs source");

                self.node.reset_failed_successor();
                self.node.push_query(self.node.get_id(), QueryType::JoinAck);
                self.send_to_broker(&msg::FindSucc::new(
                    &self.node_name,
                    &src,
                    self.node.get_id(),
                    None,
                ))
            }

            _ => println!("That message type cannot be handled."),
        }
    }
}

/// Interface to let Handler implement a RWLock on HandlerInner
unsafe impl Sync for HandlerInner {}
