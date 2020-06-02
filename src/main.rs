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
    println!("{:#?}", cli_options);

    let ctx = zmq::Context::new();

    // SUB socket for receiving messages from broker
    let sub_sock = ctx.socket(zmq::PUB).unwrap();
    sub_sock
        .connect(&cli_options.pub_endpoint)
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
            std::str::from_utf8(&data[1]).unwrap()
        );
    }
}
