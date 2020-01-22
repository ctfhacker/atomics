use std::boxed::Box;

use core::sync::atomic::{Ordering, AtomicU64};

/// Integer Hash function from MurmurHash3's integer finalizer
pub fn hash_key(val: u64) -> u64 {
    let mut res = val;
    res ^= res >> 33;
    res = res.wrapping_mul(0xff51afd7ed558ccd);
    res ^= res >> 33;
    res = res.wrapping_mul(0xc4ceb9fe1a85ec53);
    res ^= res >> 33;
    res
}

pub struct AtomicHashMap {
    keys:   Box<[AtomicU64]>,
    values: Box<[AtomicU64]>,
    size: usize
}

unsafe impl Send for AtomicHashMap {}
unsafe impl Sync for AtomicHashMap {}

#[derive(Debug, PartialEq, Eq)]
pub enum AtomicHashMapError {
    Full
}

impl AtomicHashMap {
    /// Construct a new AtomicHashMap with a given size.
    /// NOTE: Size must be a power of two.
    pub fn new(size: usize) -> AtomicHashMap {
        let mut good_size = false;
        for i in 1..64 {
            if size == 1 << i {
                good_size = true;
                break;
            }
        } 
        
        if !good_size {
            panic!("Size of AtomicHashMap must be a power of two");
        }

        let mut keys = Vec::with_capacity(size);
        for _ in 0..size {
            keys.push(AtomicU64::new(0));
        }

        let mut values = Vec::with_capacity(size);
        for _ in 0..size {
            values.push(AtomicU64::new(0));
        }

        AtomicHashMap {
            keys: keys.into_boxed_slice(),
            values: values.into_boxed_slice(),
            size 
        }
    }

    pub fn with_capacity(size: usize) -> AtomicHashMap {
        AtomicHashMap::new(size)
    }

    /// Atomically set a key:value in the hashmap
    ///
    /// The search for an empty slot or the valid key is linear in the array of keys.
    /// For efficiency, the start of the search is pseudo random based on the key
    /// and the MurmurHash3 hashing function.
    pub fn insert(&self, key: u64, new_value: u64) -> Result<(), AtomicHashMapError> {
        assert!(key != 0, "AtomicHashMap cannot have a key with value 0");
        // Get a hash of the key
        let start_index = hash_key(key) as usize;

        // Start somewhere in the middle of the values based on the hash of the key
        for index in start_index..(start_index+self.size) {
            // Since the total capacity is a power of two,`subtract 1 | and` gives us 
            // an easy modulo of the total capacity
            let index = index & (self.size - 1);

            let curr_key = self.keys[index].load(Ordering::Acquire);
            if curr_key != key {
                if curr_key != 0 {
                    // This key is already taken.. continue
                    continue;
                }

                let prev_key = self.keys[index].compare_and_swap(0, key, Ordering::AcqRel);
                if prev_key != 0 && prev_key != key {
                    // This key was stored out from under us, can't store there now.. 
                    continue
                }
            }
            
            // Either successfuly found an empty slot, or successfully found the slot
            // previously storing this key.. 
            self.values[index].store(new_value, Ordering::Release);
            return Ok(())
        }

        return Err(AtomicHashMapError::Full);
    }

    /// Atomically get a value from the hashmap
    pub fn get(&self, key: &u64) -> Option<u64> {
        assert!(*key != 0, "AtomicHashMap cannot have a key with value 0");
        // Get a hash of the key
        let start_index = hash_key(*key) as usize;

        // Start somewhere in the middle of the values based on the hash of the key
        for index in start_index..(start_index+self.size) {
            // Since the total capacity is a power of two,`subtract 1 | and` gives us 
            // an easy modulo of the total capacity
            let index = index & (self.size - 1);

            if self.keys[index].load(Ordering::Acquire) != *key {
                // Didn't find the wanted key at this index.. continue
                continue;
            }

            let res = self.values[index].load(Ordering::Acquire);

            // Found the correct index for this key, return the value
            return Some(res);
        }

        return None;
    }

    /// Get the number of elements currently in the hashtable
    pub fn len(&self) -> u64 {
        let mut count = 0;

        // Start somewhere in the middle of the values based on the hash of the key
        for index in 0..self.size {
            let key = self.keys[index].load(Ordering::Relaxed);
            let value = self.values[index].load(Ordering::Relaxed);
            if key != 0 && value != 0 {
                count += 1;
            }
        }

        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blanket_insert_get() {
        let size: u64 = 1 << 10;
        let hashtable = AtomicHashMap::new(size as usize);

        for x in 1..=size {
            // Don't care about the Full case in the test
            let _ = hashtable.insert(x, x); 
        }

        for x in 1..=size {
            assert_eq!(hashtable.get(&x).unwrap_or(0xffffffff), x);
        }
    }

    #[test]
    fn test_threads() {
        use std::thread;
        use std::sync::Arc;

        let size: u64 = 1 << 12;
        let hashtable = Arc::new(AtomicHashMap::new(size as usize));

        let mut threads = Vec::new();
        for i in 0..10 {
            let hashtable_i = hashtable.clone();
            let t = thread::spawn(move || {
                for x in 1..=size {
                    // Don't care about the FULL error in this test
                    let _ = hashtable_i.insert(x, i * x); 
                }
            });
            threads.push(t);
        }

        for t in threads {
            let _ = t.join();
        }
    }

    #[test]
    fn test_full() {
        let size: u64 = 1 << 4;
        let hashtable = AtomicHashMap::new(size as usize);

        // Insert one element and ensure it inserted fine
        assert_eq!(hashtable.insert(10000, 10), Ok(()));

        // Fill the remaining slots and ensure they were inserted fine
        for x in 1..=(size-1) {
            // Don't care about the Full case in the test
            assert_eq!(hashtable.insert(x, x), Ok(()));
        }

        // Ensure if we insert one more element that we are full
        assert_eq!(hashtable.insert(20000, 10), Err(AtomicHashMapError::Full));
    }
}
