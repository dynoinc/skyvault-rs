use std::{
    collections::{
        BTreeMap,
        hash_map::DefaultHasher,
    },
    hash::{
        Hash,
        Hasher,
    },
};

/// A consistent hash ring implementation.
///
/// This structure maps keys to nodes in a way that minimizes redistribution
/// when nodes are added or removed.
pub struct ConsistentHashRing<T>
where
    T: Clone + Eq + Hash,
{
    /// The virtual nodes in the ring, mapping hash positions to node
    /// identifiers.
    ring: BTreeMap<u64, T>,

    /// The number of virtual nodes per real node.
    replicas: usize,
}

impl<T> ConsistentHashRing<T>
where
    T: Clone + Eq + Hash,
{
    /// Create a new consistent hash ring with the specified number of virtual
    /// nodes per real node.
    pub fn new(replicas: usize) -> Self {
        ConsistentHashRing {
            ring: BTreeMap::new(),
            replicas,
        }
    }

    /// Add a node to the hash ring.
    pub fn add_node(&mut self, node: T) {
        for i in 0..self.replicas {
            let key = self.hash(&(&node, i));
            self.ring.insert(key, node.clone());
        }
    }

    /// Remove a node from the hash ring.
    pub fn remove_node(&mut self, node: &T) {
        for i in 0..self.replicas {
            let key = self.hash(&(node, i));
            self.ring.remove(&key);
        }
    }

    /// Get the node responsible for the given key.
    ///
    /// This method accepts any type that can be borrowed as a hashable type,
    /// allowing you to pass `&str` when looking up nodes stored as `String`.
    pub fn get_node<K>(&self, key: &K) -> Option<T>
    where
        K: Hash + ?Sized,
    {
        if self.ring.is_empty() {
            return None;
        }

        let key_hash = self.hash(key);
        match self.ring.range(key_hash..).next() {
            Some((_, node)) => Some(node.clone()),
            None => self.ring.values().next().cloned(),
        }
    }

    /// Hash the given key.
    fn hash<K>(&self, key: &K) -> u64
    where
        K: Hash + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_ring() {
        let ring: ConsistentHashRing<String> = ConsistentHashRing::new(10);
        assert_eq!(ring.get_node("test_key"), None);
    }

    #[test]
    fn test_single_node() {
        let mut ring: ConsistentHashRing<String> = ConsistentHashRing::new(10);
        ring.add_node("node1".to_string());

        assert_eq!(ring.get_node("test_key"), Some("node1".to_string()));
    }

    #[test]
    fn test_multiple_nodes() {
        let mut ring: ConsistentHashRing<String> = ConsistentHashRing::new(10);
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());
        ring.add_node("node3".to_string());

        // The node assignment will depend on the hash function
        assert!(ring.get_node("test_key1").is_some());
        assert!(ring.get_node("test_key2").is_some());
    }

    #[test]
    fn test_remove_node() {
        let mut ring: ConsistentHashRing<String> = ConsistentHashRing::new(10);
        ring.add_node("node1".to_string());
        ring.add_node("node2".to_string());

        ring.remove_node(&"node1".to_string());

        // After removing node1, all keys should map to node2
        assert_eq!(ring.get_node("test_key"), Some("node2".to_string()));
    }
}
