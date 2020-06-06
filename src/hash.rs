// File: hash.rs
//
// The purpose of this file is to hash a given key with SHA-1
// and determine if a given key is between two other keys in the ring.

use sha1::{Digest, Sha1};
use std::cmp::Ordering;

/// Returns a hash for a given key
///
/// # Arguments
///
/// * `key` - The key to be hashed
pub fn hash(key: &str) -> i32 {
  let mut hasher = Sha1::new();
  hasher.input(key.as_bytes());
  let result = hasher.result();
  *result.last().unwrap() as i32
}

/// Returns if a given id is in the range of a min and max key on the ring
/// The search is performed where min -> max is in the clockwise direction
///
/// # Arguments
///
/// * `id` - The key that is being queried
/// * `min` - The lower bound key of the range
/// * `max` - The upper bound key of the range
/// * `incl` - Whether or not the upper bound is inclusive
pub fn in_range(id: i32, min: i32, max: i32, incl: bool) -> bool {
  match min.cmp(&max) {
    Ordering::Less => {
      if incl {
        id > min && id <= max
      } else {
        id > min && id < max
      }
    }
    Ordering::Greater => {
      if incl {
        id > min || id <= max
      } else {
        id > min || id < max
      }
    }
    Ordering::Equal => true,
  }
}
