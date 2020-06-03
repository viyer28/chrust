extern crate zmq;
use crate::msg;
use crate::node;
use serde::Serialize;

pub struct Handler {
    connected: bool,
    ctx: zmq::Context,
    node_name: String,
    sub_socket: zmq::Socket,
    req_socket: zmq::Socket,
    node: node::Node,
}

impl Handler {
    pub fn new(
        ctx: zmq::Context,
        node_name: &str,
        pub_endpoint: &str,
        router_endpoint: &str,
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

        Handler {
            ctx,
            connected: false,
            node_name: String::from(node_name),
            sub_socket,
            req_socket,
            node: node::Node::new(),
        }
    }

    pub fn send_to_broker<T: Serialize>(&self, msg: T) {
        let json = serde_json::json!(msg);
        let j = serde_json::to_string(&json).expect("cannot convert json to string");
        self.req_socket.send(&j, 0).expect("cannot send message");

        // ACK
        let _ = self.req_socket.recv_string(0).unwrap().unwrap();
    }

    pub fn listen_to_broker(&self) {
        loop {
            let msg = self.req_socket.recv_string(0).unwrap().unwrap();
            println!("{}", msg);
        }
    }

    pub fn listen_to_publisher(&mut self) {
        loop {
            let _address = self.sub_socket.recv_string(0).unwrap().unwrap();
            let _skip = self.sub_socket.recv_string(0).unwrap().unwrap();
            let contents = self.sub_socket.recv_string(0).unwrap().unwrap();
            let m: msg::RcvdMessage = serde_json::from_str(&contents).unwrap();

            self.handle_messages(m)
        }
    }

    fn handle_messages(&mut self, msg: msg::RcvdMessage) {
        match &msg.r#type[..] {
            "hello" => {
                if !self.connected {
                    self.send_to_broker(&msg::HelloResponse::new(&self.node_name));
                }
                self.connected = true;
            }

            "set" => {
                // let _dest = msg.destination;
                let id = msg.id.expect("set: needs id");
                let k = msg.key.expect("set: needs key");
                let v = msg.value.expect("set: needs value");

                self.send_to_broker(&msg::SetResponse::new(id, &k, &v));

                self.node.store.insert(k, v);
            }

            "get" => {
                // let _dest = msg.destination;
                let id = msg.id.expect("get: needs id");
                let k = msg.key.expect("get: needs key");
                match self.node.store.get(&k) {
                    Some(v) => self.send_to_broker(&msg::GetSuccessResponse::new(id, &k, &v)),
                    None => self.send_to_broker(&msg::GetFailResponse::new(id, &k)),
                }
            }

            _ => println!("no op"),
        }
    }
}
