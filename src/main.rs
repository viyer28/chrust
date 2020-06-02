extern crate zmq;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
pub struct CLI {
  /// Activate debug mode
  // short and long flags (-d, --debug) will be deduced from the field's name
  #[structopt(short, long)]
  debug: bool,

  #[structopt(long)]
  node_name: String,

  #[structopt(long)]
  pub_endpoint: String,

  #[structopt(long)]
  router_endpoint: String,

  #[structopt(long)]
  peer: Option<Vec<String>>,
}

fn main() {
  let cli_options = CLI::from_args();
  let ctx = zmq::Context::new();

  // SUB socket for receiving messages from broker
  let sub_sock = ctx.socket(zmq::SUB).unwrap();
  sub_sock
    .connect(&cli_options.pub_endpoint)
    .expect("failed connecting subscriber"); // pub endpoint
  sub_sock
    .set_subscribe(&cli_options.node_name.as_bytes())
    .expect("failed subscribing"); // node_name

  // Create handler for SUB scoket

  // REQ socket for sending messages to the broker
  let req_sock = ctx.socket(zmq::REQ).unwrap();
  req_sock.connect(&cli_options.router_endpoint).unwrap(); // router endpoint
  req_sock
    .set_identity(&cli_options.node_name.as_bytes())
    .unwrap();
  // Capture signals for shutting down node

  // loop
  loop {
    let address = sub_sock.recv_string(0).unwrap().unwrap();
    let contents = sub_sock.recv_string(0).unwrap().unwrap();
    println!("[{}] {}", address, contents);
  }
}
