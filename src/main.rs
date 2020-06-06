// File: main.rs
//
// The purpose of this file is to initialize the node process.

extern crate zmq;
#[macro_use]
extern crate chan;
use structopt::StructOpt;
mod handler;
mod hash;
mod msg;
mod node;

/// Holds data parsed from the command line to initialize node
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
pub struct CLI {
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

/// Main function
fn main() {
  // Command line arguments are parsed and the message handler is constructed.
  let cli_options = CLI::from_args();
  let ctx = zmq::Context::new();
  let mut handler = handler::Handler::new(
    ctx,
    &cli_options.node_name,
    &cli_options.pub_endpoint,
    &cli_options.router_endpoint,
    cli_options.peer,
  );

  // The handler begins listening for messages from the broker.
  handler.listen_to_publisher();
}
