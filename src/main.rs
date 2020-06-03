extern crate zmq;
use structopt::StructOpt;

mod handler;
mod msg;
mod node;

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
  let mut handler = handler::Handler::new(
    ctx,
    &cli_options.node_name,
    &cli_options.pub_endpoint,
    &cli_options.router_endpoint,
  );

  handler.listen_to_publisher();
  //handler.listen_to_broker();
}
