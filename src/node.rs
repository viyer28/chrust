use std::collections::HashMap;

pub struct Node {
  pub store: HashMap<String, String>,
}

impl Node {
  pub fn new() -> Node {
    Node {
      store: HashMap::new(),
      // finger table will go here soon
    }
  }
}
