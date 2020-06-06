// File: msg.rs
//
// The purpose of this file is to define message types
// to send/receive from the broker.

use serde::{Deserialize, Serialize};

/// For parsing data from a received message of any type
#[derive(Serialize, Deserialize, Debug)]
pub struct RcvdMessage {
    pub r#type: String,
    pub key: Option<String>,
    pub value: Option<String>,
    pub id: Option<i32>,
    pub destination: Option<String>,
    pub source: Option<String>,
    pub query_id: Option<i32>,
    pub node_name: Option<String>,
    pub node_id: Option<i32>,
    pub pred_id: Option<i32>,
    pub pred_name: Option<String>,
    pub min: Option<i32>,
    pub max: Option<i32>,
    pub keys: Option<Vec<String>>,
    pub values: Option<Vec<String>>,
    pub failed: Option<bool>,
}

/// To send to broker to confirm joining the network
#[derive(Serialize)]
pub struct HelloResponse {
    r#type: String,
    source: String,
}

impl HelloResponse {
    /// Returns a new HelloResponse object
    ///
    /// # Arguments
    ///
    /// * `source` - The source node
    pub fn new(source: &str) -> HelloResponse {
        HelloResponse {
            r#type: String::from("helloResponse"),
            source: String::from(source),
        }
    }
}

/// A confirmation of a Set operation
#[derive(Serialize)]
pub struct SetResponse {
    r#type: String,
    id: i32,
    key: String,
    value: String,
}

impl SetResponse {
    /// Returns a new SetResponse object
    ///
    /// # Arguments
    ///
    /// * `id` - id of the Set request
    /// * `key` - key set to
    /// * `value` - value set
    pub fn new(id: i32, key: &str, value: &str) -> SetResponse {
        SetResponse {
            r#type: String::from("setResponse"),
            id,
            key: String::from(key),
            value: String::from(value),
        }
    }
}

/// A successful Get query response
#[derive(Serialize)]
pub struct GetSuccessResponse {
    r#type: String,
    id: i32,
    key: String,
    value: String,
}

impl GetSuccessResponse {
    /// Returns a new GetSuccessResponse object
    ///
    /// # Arguments
    ///
    /// * `id` - id of the Get request
    /// * `key` - key requested
    /// * `value` - value found
    pub fn new(id: i32, key: &str, value: &str) -> GetSuccessResponse {
        GetSuccessResponse {
            r#type: String::from("getResponse"),
            id,
            key: String::from(key),
            value: String::from(value),
        }
    }
}

/// A failed Get query response
#[derive(Serialize)]
pub struct GetFailResponse {
    r#type: String,
    id: i32,
    error: String,
}

impl GetFailResponse {
    /// Returns a new GetFailResponse object
    ///
    /// # Arguments
    ///
    /// * `id` - id of the Get request
    /// * `key` - key requested
    pub fn new(id: i32, key: &str) -> GetFailResponse {
        GetFailResponse {
            r#type: String::from("getResponse"),
            id,
            error: String::from("No such key: ") + key,
        }
    }
}

// CUSTOM MESSAGES FOR HALO PROTOCOL

/// A request to join the ring
#[derive(Serialize)]
pub struct Join {
    r#type: String,
    source: String,
    destination: String,
}

impl Join {
    /// Returns a new Join object
    ///
    /// # Arguments
    ///
    /// * `source` - the requesting node
    /// * `destination` - the node that the requestor wants to join
    pub fn new(source: &str, destination: &str) -> Join {
        Join {
            r#type: String::from("join"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A response to a node wanting to join the ring
#[derive(Serialize)]
pub struct JoinAck {
    r#type: String,
    source: String,
    destination: String,
}

impl JoinAck {
    /// Returns a new JoinAck object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that the requestor wants to join
    /// * `destination` - the requesting node
    pub fn new(source: &str, destination: &str) -> JoinAck {
        JoinAck {
            r#type: String::from("joinAck"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A request to find the successor of a key query
#[derive(Serialize)]
pub struct FindSucc {
    r#type: String,
    source: String,
    destination: String,
    query_id: i32,
    id: Option<i32>,
}

impl FindSucc {
    /// Returns a new FindSucc object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that is requesting the key query
    /// * `destination` - a node checking itself and/or passing the query on
    /// * `query-id` - key being queried
    /// * `id` - an optional id used for some queries
    pub fn new(source: &str, destination: &str, query_id: i32, id: Option<i32>) -> FindSucc {
        FindSucc {
            r#type: String::from("findSucc"),
            source: String::from(source),
            destination: String::from(destination),
            query_id,
            id,
        }
    }
}

/// A response returning the successor of a key query
#[derive(Serialize)]
pub struct FindSuccResponse {
    r#type: String,
    source: String,
    destination: String,
    node_name: String,
    node_id: i32,
    query_id: i32,
    id: Option<i32>,
}

impl FindSuccResponse {
    /// Returns a new FindSucc object
    ///
    /// # Arguments
    ///
    /// * `source` - the node sending the response
    /// * `destination` - the requestor of the query
    /// * `node_name` - the node that is the successor of the key (name)
    /// * `node_id` - the node that is the successor of the key (hashed id)
    /// * `query-id` - key being queried
    /// * `id` - an optional id used for some queries
    pub fn new(
        source: &str,
        destination: &str,
        node_name: &str,
        node_id: i32,
        query_id: i32,
        id: Option<i32>,
    ) -> FindSuccResponse {
        FindSuccResponse {
            r#type: String::from("findSuccResponse"),
            source: String::from(source),
            destination: String::from(destination),
            node_name: String::from(node_name),
            node_id,
            query_id,
            id,
        }
    }
}

/// A request to get the predecessor of a node
#[derive(Serialize)]
pub struct GetPred {
    r#type: String,
    source: String,
    destination: String,
}

impl GetPred {
    /// Returns a new GetPred object
    ///
    /// # Arguments
    ///
    /// * `source` - the node requesting its successor's predecessor
    /// * `destination` - the node being requested
    pub fn new(source: &str, destination: &str) -> GetPred {
        GetPred {
            r#type: String::from("getPred"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A response to get the predecessor of a node
#[derive(Serialize)]
pub struct GetPredResponse {
    r#type: String,
    source: String,
    destination: String,
    pred_id: Option<i32>,
    pred_name: Option<String>,
}

impl GetPredResponse {
    /// Returns a new GetPredResponse object
    ///
    /// # Arguments
    ///
    /// * `source` - the node being requested
    /// * `destination` - the node requesting its successor's predecessor
    /// * `pred_id` - the predecessor, if it has one (hashed id)
    /// * `pred_name` - the predecessor, if it has one (name)
    pub fn new(
        source: &str,
        destination: &str,
        pred_id: Option<i32>,
        pred_name: Option<String>,
    ) -> GetPredResponse {
        GetPredResponse {
            r#type: String::from("getPredResponse"),
            source: String::from(source),
            destination: String::from(destination),
            pred_id,
            pred_name,
        }
    }
}

/// A node notifies a successor that it thinks that
/// it could be the successor's predecessor
#[derive(Serialize)]
pub struct Notify {
    r#type: String,
    source: String,
    destination: String,
    node_id: i32,
    failed: bool,
}

impl Notify {
    /// Returns a new Notify object
    ///
    /// # Arguments
    ///
    /// * `source` - the node being requested
    /// * `destination` - the node requesting its successor's predecessor
    /// * `pred_id` - the predecessor, if it has one (hashed id)
    /// * `pred_name` - the predecessor, if it has one (name)
    pub fn new(source: &str, destination: &str, node_id: i32, failed: bool) -> Notify {
        Notify {
            r#type: String::from("notify"),
            source: String::from(source),
            destination: String::from(destination),
            node_id,
            failed,
        }
    }
}

/// A request for a node to retrieve data, if it exists,
/// and return a GetResponse to the client
#[derive(Serialize)]
pub struct Retrieve {
    r#type: String,
    source: String,
    destination: String,
    key: String,
    id: i32,
}

impl Retrieve {
    /// Returns a new Retrieve object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that received the Get request from the client
    /// * `destination` - the successor of the key for the data
    /// * `key` - the key to be searched for locally
    /// * `id` - the id of the Get request from the client
    pub fn new(source: &str, destination: &str, key: &str, id: i32) -> Retrieve {
        Retrieve {
            r#type: String::from("retrieve"),
            source: String::from(source),
            destination: String::from(destination),
            key: String::from(key),
            id,
        }
    }
}

/// A request for a node to store data locally
#[derive(Serialize)]
pub struct Store {
    r#type: String,
    source: String,
    destination: String,
    key: String,
    value: String,
}

impl Store {
    /// Returns a new Store object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that received the Set request from the client
    /// * `destination` - the successor of the key for the data
    /// * `key` - the key to be stored for locally
    /// * `value` - the data to be stored
    pub fn new(source: &str, destination: &str, key: &str, value: &str) -> Store {
        Store {
            r#type: String::from("store"),
            source: String::from(source),
            destination: String::from(destination),
            key: String::from(key),
            value: String::from(value),
        }
    }
}

/// A request for a node to transfer its keys in a given range
#[derive(Serialize)]
pub struct TransferRequest {
    r#type: String,
    source: String,
    destination: String,
    min: i32,
    max: i32,
}

impl TransferRequest {
    /// Returns a new TransferRequest object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that wants the keys
    /// * `destination` - the node that contains the requested keys
    /// * `min` - the minimum key to be transferred
    /// * `max` - the maximum key to be transferred
    pub fn new(source: &str, destination: &str, min: i32, max: i32) -> TransferRequest {
        TransferRequest {
            r#type: String::from("transferRequest"),
            source: String::from(source),
            destination: String::from(destination),
            min,
            max,
        }
    }
}

/// A response from a node transferring its keys in a given range
#[derive(Serialize)]
pub struct TransferKeys {
    r#type: String,
    source: String,
    destination: String,
    keys: Vec<String>,
    values: Vec<String>,
}

impl TransferKeys {
    /// Returns a new TransferKeys object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that contains the requested keys
    /// * `destination` - the node that wants the keys
    /// * `keys` - the transferred keys
    /// * `valurs` - the transferred values
    pub fn new(
        source: &str,
        destination: &str,
        keys: Vec<String>,
        values: Vec<String>,
    ) -> TransferKeys {
        TransferKeys {
            r#type: String::from("transferKeys"),
            source: String::from(source),
            destination: String::from(destination),
            keys,
            values,
        }
    }
}

/// A node duplicating its key/values to successors to be stored as replicas
#[derive(Serialize)]
pub struct Duplicate {
    r#type: String,
    source: String,
    destination: String,
    id: i32,
    keys: Vec<String>,
    values: Vec<String>,
}

impl Duplicate {
    /// Returns a new Duplicate object
    ///
    /// # Arguments
    ///
    /// * `source` - the node that is duplicating is data
    /// * `destination` - the node that is storing the replica
    /// * `keys` - the duplicated keys
    /// * `values` - the duplicated values
    pub fn new(
        source: &str,
        destination: &str,
        id: i32,
        keys: Vec<String>,
        values: Vec<String>,
    ) -> Duplicate {
        Duplicate {
            r#type: String::from("duplicate"),
            source: String::from(source),
            destination: String::from(destination),
            id,
            keys,
            values,
        }
    }
}

/// A node checking if its successor is still alive
#[derive(Serialize)]
pub struct Ping {
    r#type: String,
    source: String,
    destination: String,
}

impl Ping {
    /// Returns a new Ping object
    ///
    /// # Arguments
    ///
    /// * `source` - the node performing the check
    /// * `destination` - the successor node to be checked
    pub fn new(source: &str, destination: &str) -> Ping {
        Ping {
            r#type: String::from("ping"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A response to a node checking if its successor is still alive
#[derive(Serialize)]
pub struct Pong {
    r#type: String,
    source: String,
    destination: String,
}

impl Pong {
    /// Returns a new Pong object
    ///
    /// # Arguments
    ///
    /// * `source` - the successor node checked
    /// * `destination` - the node performing the check
    pub fn new(source: &str, destination: &str) -> Pong {
        Pong {
            r#type: String::from("pong"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A Ping from a node to itself
/// A way of keeping the periodic stabilization thread unblocked
#[derive(Serialize)]
pub struct PingSelf {
    r#type: String,
    destination: String,
}

impl PingSelf {
    /// Returns a new PingSelf object
    ///
    /// # Arguments
    ///
    /// * `destination` - the node pinging itself
    pub fn new(destination: &str) -> PingSelf {
        PingSelf {
            r#type: String::from("pingSelf"),
            destination: String::from(destination),
        }
    }
}

/// A Pong from a node to itself
/// A way of keeping the periodic stabilization thread unblocked
#[derive(Serialize)]
pub struct PongSelf {
    r#type: String,
    destination: String,
}

impl PongSelf {
    /// Returns a new PongSelf object
    ///
    /// # Arguments
    ///
    /// * `destination` - the node ponging itself
    pub fn new(destination: &str) -> PongSelf {
        PongSelf {
            r#type: String::from("pongSelf"),
            destination: String::from(destination),
        }
    }
}

/// A request from a node to rejoin a failed successor
#[derive(Serialize)]
pub struct Rejoin {
    r#type: String,
    source: String,
    destination: String,
}

impl Rejoin {
    /// Returns a new Rejoin object
    ///
    /// # Arguments
    ///
    /// * `source` - the node wanting to rejoin
    /// * `destination` - the node to be rejoined
    pub fn new(source: &str, destination: &str) -> Rejoin {
        Rejoin {
            r#type: String::from("rejoin"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}

/// A response to a node trying to rejoin a ring
#[derive(Serialize)]
pub struct RejoinAck {
    r#type: String,
    source: String,
    destination: String,
}

impl RejoinAck {
    /// Returns a new RejoinAck object
    ///
    /// # Arguments
    ///
    /// * `source` - the node to be rejoined
    /// * `destination` - the node wanting to rejoin
    pub fn new(source: &str, destination: &str) -> RejoinAck {
        RejoinAck {
            r#type: String::from("rejoinAck"),
            source: String::from(source),
            destination: String::from(destination),
        }
    }
}
