extern crate zmq;
use crate::msg;
use serde::Serialize;

pub struct Handler {
    connected: bool,
    ctx: zmq::Context,
    node_name: String,
    sub_socket: zmq::Socket,
    req_socket: zmq::Socket,
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
            .expect("failed connecting subscriber"); // pub endpoint

        sub_socket
            .set_subscribe(node_name.as_bytes())
            .expect("failed subscribing"); //

        // REQ socket for sending messages to the broker
        let req_socket = ctx.socket(zmq::REQ).unwrap();
        req_socket.connect(router_endpoint).unwrap(); // router endpoint
        req_socket.set_identity(node_name.as_bytes()).unwrap();
        // Capture signals for shutting down node

        Handler {
            ctx,
            connected: false,
            node_name: String::from(node_name),
            sub_socket,
            req_socket,
        }
    }

    pub fn send_to_broker<T: Serialize>(&self, msg: &T) {
        let j = serde_json::to_string(&msg).unwrap();
        self.req_socket.send(&j, 0).unwrap();
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
            let m: msg::ControlMessages = serde_json::from_str(&contents).unwrap();
            println!("{:?}", m);

            match &m.r#type[..] {
                "hello" => {
                    if !self.connected {
                        self.send_to_broker(&msg::HeyBack::new(&self.node_name));
                    }
                    self.connected = true;
                }
                _ => println!("no op"),
            }
        }
    }
}
