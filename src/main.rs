extern crate zmq;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
pub struct CLI {
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
    println!("{:#?}", cli_options);

    let ctx = zmq::Context::new();

    let socket = ctx.socket(zmq::PUB).unwrap();
    socket.connect("tcp://127.0.0.1:1234").unwrap();
    socket.send("hello world!", 0).unwrap();
}
