extern crate zmq;

use std::str;

fn main() {
  let ctx = zmq::Context::new();

  // SUB socket for receiving messages from broker
  let sub_sock = ctx.socket(zmq::SUB).unwrap();
  sub_sock
    .connect("tcp://127.0.0.1:23310")
    .expect("failed connecting subscriber"); // pub endpoint
  sub_sock
    .set_subscribe(b"node 1")
    .expect("failed subscribing"); // node_name

  // Create handler for SUB scoket

  // REQ socket for sending messages to the broker
  let req_sock = ctx.socket(zmq::REQ).unwrap();
  req_sock.connect("tcp://127.0.0.1:23311").unwrap(); // router endpoint
  req_sock.set_identity(b"node 1").unwrap(); // node_name
                                             // Capture signals for shutting down node

  // loop
  loop {
    let data = sub_sock.recv_multipart(0).unwrap();
    println!(
      "Identity: {:?} Message : {}",
      data[0],
      str::from_utf8(&data[1]).unwrap()
    );
  }
}
