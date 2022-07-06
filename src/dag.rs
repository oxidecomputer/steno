// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction and Recovery of sagas

use crate::saga_action_generic::{Action, ActionData, ActionEndNode};
use crate::SagaType;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/** Unique identifier for a Saga (an execution of a saga template) */
#[derive(
    Clone,
    Copy,
    Deserialize,
    Eq,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(transparent)]
pub struct SagaId(pub Uuid);
// TODO-cleanup figure out how to use custom_derive here?
NewtypeDebug! { () pub struct SagaId(Uuid); }
/*
 * TODO-design In the Oxide consumer, we probably want to have the serialized
 * form of ids have a prefix describing the type.  This seems consumer-specific,
 * though.  Is there a good way to do support that?  Maybe the best way to do
 * this is to have the consumer have their own enum or trait that impls Display
 * using the various ids provided by consumers.
 */
NewtypeDisplay! { () pub struct SagaId(Uuid); }
NewtypeFrom! { () pub struct SagaId(Uuid); }

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct ActionName(String);

impl ActionName {
    pub fn new(name: &str) -> ActionName {
        ActionName(name.to_string())
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SagaName(pub String);

impl SagaName {
    pub fn new(name: &str) -> SagaName {
        SagaName(name.to_string())
    }
}

#[derive(Debug)]
pub enum ActionRegistryError {
    NotFound,
}

/// A macro that creates a registry of saga actions that can be used across
/// multiple sagas.
///
/// Actions can exist at multiple nodes in each saga DAG. Since saga
/// construction is dynamic and based upon user input, we need to allow a way
/// to insert actions at runtime into the DAG. While this could be achieved by
/// referencing the action during saga construction, this is not possible when
/// reloading a saga from persistent storage. In this case, the concrete type
/// of the Action is erased and the only mechanism we have to recover it is an
/// `ActionName`. We therefore have all users register their actions for use
/// across sagas so we can dynamically construct and restore sagas.
///
/// TODO: How do we handle actions wtih different saga types?
#[derive(Debug)]
pub struct ActionRegistry<UserType: SagaType> {
    actions: BTreeMap<ActionName, Arc<dyn Action<UserType>>>,
}

impl<UserType: SagaType> ActionRegistry<UserType> {
    pub fn new() -> ActionRegistry<UserType> {
        let mut actions = BTreeMap::new();

        // Insert the default end action for a saga
        let action: Arc<dyn Action<UserType> + 'static> =
            Arc::new(ActionEndNode {});
        let name = ActionName("__steno_action_end_node__".to_string());
        actions.insert(name, action).unwrap();

        ActionRegistry { actions }
    }

    pub fn register(
        &mut self,
        name: ActionName,
        action: Arc<dyn Action<UserType>>,
    ) {
        let already_inserted = self.actions.insert(name, action);
        assert!(already_inserted.is_none());
    }

    pub fn get(
        &self,
        name: &ActionName,
    ) -> Result<Arc<dyn Action<UserType>>, ActionRegistryError> {
        self.actions.get(name).cloned().ok_or(ActionRegistryError::NotFound)
    }
}

/// A Node in the graph
///
/// Since sagas are constructed dynamically at runtime, we don't know the
/// shape of the graph ahead of time. We need to maintain enough information
/// to reconstruct the saga when loaded from persistent storage. The easiest
/// way to do that is to store the graph itself with enough information in
/// each node that allows us to recreate the Saga. Note that we don't store
/// the execution state of the saga here. That continues to reside in saga log
/// consisting of `SagaNodeEvent`s.
///
/// There can be multiple subsagas with nodes that run the same actions. In order
/// to distinguish the action outputs we tag each one with an `instance_id`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Node {
    pub name: String,
    pub instance_id: u16,
    pub label: String,
    pub action: ActionName,

    /// Each node may be the start of the outer saga, or a subsaga. We store
    /// the creation parameters for the saga or subsaga as output of the first
    /// node so that it can be retrieved by child nodes. In order to feed the
    /// output of the node execution, the params themselves must be stored
    /// as input to the DAG, and so we store them here. We only store these
    /// parameters for the first node of a saga or child saga.
    pub create_params: Option<serde_json::Value>,
}

impl Node {
    /// Create a new child node for a saga or subsaga
    pub fn new_child(
        name: &str,
        instance_id: u16,
        label: &str,
        action: ActionName,
    ) -> Node {
        Node {
            name: name.to_string(),
            instance_id,
            label: label.to_string(),
            action,
            create_params: None,
        }
    }

    /// Create a new root node for a saga or subsaga
    ///
    /// The first node in a subsaga is always a root node even though it is
    /// technically a child node of the outer saga.
    pub fn new_root<T: ActionData>(
        name: &str,
        instance_id: u16,
        label: &str,
        action: ActionName,
        create_params: &T,
    ) -> Node {
        let create_params = serde_json::to_value(create_params).unwrap();
        Node {
            name: name.to_string(),
            instance_id,
            label: label.to_string(),
            action,
            create_params: Some(create_params),
        }
    }
}

/// A DAG describing a saga
//
// Note: This doesn't implement JSON schema because of Graph and NodeIndex
// TODO(AJS) - Create a separate metadata type?
//
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    pub(crate) name: SagaName,
    pub(crate) graph: Graph<Node, ()>,
    pub(crate) start_node: NodeIndex,
    pub(crate) end_node: NodeIndex,
}

impl Dag {
    pub fn get(&self, node_index: NodeIndex) -> Option<&Node> {
        self.graph.node_weight(node_index)
    }
}

#[derive(Debug)]
pub struct DagBuilder {
    name: SagaName,
    graph: Graph<Node, ()>,
    root: NodeIndex,
    last_added: Vec<NodeIndex>,
}

impl DagBuilder {
    /// Create a new DAG for a Saga
    pub fn new(name: SagaName, root: Node) -> DagBuilder {
        let mut graph = Graph::new();
        let root = graph.add_node(root);

        DagBuilder { name, graph, root, last_added: vec![root] }
    }

    /// Adds a new node to the graph
    ///
    /// The new node will depend on completion of all actions that were added in
    /// the last call to `append` or `append_parallel`.  (The idea is to `append`
    /// a sequence of steps that run one after another.)
    ///
    /// `action` will be used when this node is being executed.
    ///
    /// The node is called `name`. This name is used along with `instance_id`
    /// for storing the output of the action so that descendant nodes can
    /// access it using [`crate::ActionContext::lookup`].
    ///
    /// The instance id is useful because there may be many subsagas of the same
    /// type that run as part of the main saga. Each one of nodes in each subsaga
    /// will only want to access data from its own nodes, and a common name for those nodes
    /// is not enough to distinguish them across sagas. Furthermore, instance ids allow
    /// the parent saga to identify data generated from different subsagas.
    pub fn append(&mut self, node: Node) {
        let newnode = self.graph.add_node(node);
        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        self.last_added = vec![newnode];
    }

    /// Adds a set of nodes to the graph that can be executed concurrently
    ///
    /// The new nodes will individually depend on completion of all actions that
    /// were added in the last call to `append` or `append_parallel`.  `actions`
    /// is a vector of `(name, action)` tuples analogous to the arguments to
    /// [`SagaTemplateBuilder::append`].
    pub fn append_parallel(&mut self, actions: Vec<Node>) {
        let newnodes: Vec<NodeIndex> =
            actions.into_iter().map(|node| self.graph.add_node(node)).collect();

        // TODO-design For this exploration, we assume that any nodes appended
        // after a parallel set are intended to depend on _all_ nodes in the
        // parallel set.  This doesn't have to be the case in general, but if
        // you wanted to do something else, you probably would need pretty
        // fine-grained control over the construction of the graph.  This is
        // mostly a question of how to express the construction of the graph,
        // not the graph itself nor how it gets processed, so let's defer for
        // now.
        //
        // Even given that, it might make more sense to implement this by
        // creating an intermediate node that all the parallel nodes have edges
        // to, and then edges from this intermediate node to the next set of
        // parallel nodes.
        for node in &self.last_added {
            for newnode in &newnodes {
                self.graph.add_edge(*node, *newnode, ());
            }
        }

        self.last_added = newnodes;
    }

    /// Return the constructed DAG
    ///
    /// Building the DAG automatically creates an "end" node
    pub fn build(mut self) -> Dag {
        // Append an "end" node so that we can easily tell when the saga has
        // completed.
        let newnode = self.graph.add_node(Node::new_child(
            "__EndNode__",
            0,
            "EndNode",
            ActionName::new("__steno_action_end_node__"),
        ));

        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        Dag {
            name: self.name,
            graph: self.graph,
            start_node: self.root,
            end_node: newnode,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    struct TestParams;

    #[derive(Debug)]
    struct TestContext {}

    #[derive(Debug)]
    struct TestUserData;
    impl SagaType for TestUserData {
        type SagaParamsType = TestParams;
        type ExecContextType = TestContext;
    }

    #[test]
    fn test_saga_registry() {}
}
