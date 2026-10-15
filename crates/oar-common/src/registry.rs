//! Registry of the compute nodes known to the server.
//!
//! Nodes register themselves through `NodeCallbackService::RegisterNode` and
//! send heartbeats; the server also probes them (`ComputeNodeService::Ping`)
//! during the *checking nodes* automaton step, which flips the `alive` flag.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::types::{NodeId, NodeInfo};

/// Thread-safe, deterministic-ordered node registry.
#[derive(Debug, Default)]
pub struct NodeRegistry {
    nodes: RwLock<HashMap<NodeId, NodeInfo>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
        }
    }

    /// Builds a registry pre-filled with the given nodes.
    pub fn with_nodes(nodes: impl IntoIterator<Item = NodeInfo>) -> Self {
        let registry = Self::new();
        for node in nodes {
            registry.register(node);
        }
        registry
    }

    /// Inserts or replaces a node description. Returns the previous entry.
    pub fn register(&self, node: NodeInfo) -> Option<NodeInfo> {
        self.write().insert(node.id.clone(), node)
    }

    /// Registers (or refreshes the address of) a node, marking it alive.
    /// Returns `true` when the node was not known before.
    pub fn upsert(&self, id: impl Into<NodeId>, address: impl Into<String>) -> bool {
        let id = id.into();
        let address = address.into();
        let mut nodes = self.write();
        match nodes.get_mut(&id) {
            Some(node) => {
                node.address = address;
                node.alive = true;
                false
            }
            None => {
                nodes.insert(id.clone(), NodeInfo::new(id, address));
                true
            }
        }
    }

    pub fn remove(&self, id: &str) -> Option<NodeInfo> {
        self.write().remove(id)
    }

    pub fn get(&self, id: &str) -> Option<NodeInfo> {
        self.read().get(id).cloned()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.read().contains_key(id)
    }

    /// All nodes, sorted by id.
    pub fn list(&self) -> Vec<NodeInfo> {
        let mut nodes: Vec<NodeInfo> = self.read().values().cloned().collect();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        nodes
    }

    /// Alive nodes, sorted by id.
    pub fn alive(&self) -> Vec<NodeInfo> {
        self.list().into_iter().filter(|n| n.alive).collect()
    }

    /// Updates the liveness flag; returns the new value, or `None` if unknown.
    pub fn set_alive(&self, id: &str, alive: bool) -> Option<bool> {
        let mut nodes = self.write();
        nodes.get_mut(id).map(|node| {
            node.alive = alive;
            node.alive
        })
    }

    pub fn len(&self) -> usize {
        self.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<NodeId, NodeInfo>> {
        self.nodes.read().expect("registry lock poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<NodeId, NodeInfo>> {
        self.nodes.write().expect("registry lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_list_and_get() {
        let registry = NodeRegistry::new();
        assert!(registry.is_empty());
        registry.register(NodeInfo::new("n2", "127.0.0.1:7002"));
        registry.register(NodeInfo::new("n1", "127.0.0.1:7001"));

        let ids: Vec<_> = registry.list().into_iter().map(|n| n.id).collect();
        assert_eq!(ids, ["n1", "n2"]);
        assert!(registry.contains("n1"));
        assert_eq!(registry.get("n1").unwrap().address, "127.0.0.1:7001");
        assert!(registry.get("nope").is_none());
    }

    #[test]
    fn alive_filter_and_liveness_update() {
        let registry = NodeRegistry::with_nodes([
            NodeInfo::new("n1", "127.0.0.1:7001"),
            NodeInfo::new("n2", "127.0.0.1:7002"),
        ]);
        assert_eq!(registry.alive().len(), 2);
        assert_eq!(registry.set_alive("n2", false), Some(false));
        assert_eq!(registry.alive().len(), 1);
        assert_eq!(registry.set_alive("n2", true), Some(true));
        assert_eq!(registry.alive().len(), 2);
        assert_eq!(registry.set_alive("ghost", false), None);
    }

    #[test]
    fn upsert_refreshes_address_and_liveness() {
        let registry = NodeRegistry::new();
        registry.register(NodeInfo::new("n1", "127.0.0.1:1"));
        registry.set_alive("n1", false);
        assert!(!registry.upsert("n1", "127.0.0.1:2"));
        let node = registry.get("n1").unwrap();
        assert_eq!(node.address, "127.0.0.1:2");
        assert!(node.alive);
        assert_eq!(registry.len(), 1);

        assert!(registry.upsert("n2", "127.0.0.1:3"));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn remove_drops_the_node() {
        let registry = NodeRegistry::with_nodes([NodeInfo::new("n1", "127.0.0.1:1")]);
        assert!(registry.remove("n1").is_some());
        assert!(registry.is_empty());
        assert!(registry.remove("n1").is_none());
    }
}
