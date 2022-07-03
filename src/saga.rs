// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction and Recovery of sagas

use crate::saga_action_generic::{Action, ActionData, ActionStartNode};
use crate::SagaType;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ActionName(String);

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct SagaName(String);

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
pub struct ActionRegistry<UserType: SagaType> {
    actions: BTreeMap<ActionName, Arc<dyn Action<UserType>>>,
}

impl<UserType: SagaType> ActionRegistry<UserType> {
    pub fn new() -> ActionRegistry<UserType> {
        let mut actions = BTreeMap::new();
        // Insert the default start action for a saga
        let action: Arc<dyn Action<UserType> + 'static> =
            Arc::new(ActionStartNode {});
        let name = ActionName("__steno_action_start_node__".to_string());
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    name: String,
    label: String,
    action: ActionName,
}

#[derive(Debug)]
pub struct SagaBuilder {
    name: SagaName,
    graph: Graph<Node, ()>,
    create_params: Arc<serde_json::Value>,
    last_added: Vec<NodeIndex>,
}

impl SagaBuilder {
    /// Create a new Saga
    ///
    /// Creation of a saga results in the automatic creation of a "start" node.
    /// The output of this node is the `create_params`. This allows descendant nodes
    /// to lookup the creation parameters via [`crate::ActionContext::lookup`].
    pub fn new<C>(name: SagaName, create_params: C) -> SagaBuilder
    where
        C: ActionData,
    {
        let mut graph = Graph::new();
        let root = graph.add_node(Node {
            name: "__StartNode__".to_string(),
            label: "StartNode".to_string(),
            action: ActionName("__steno_action_start_node__".to_string()),
        });

        let create_params =
            Arc::new(serde_json::to_value(create_params).unwrap());

        SagaBuilder { name, graph, create_params, last_added: vec![root] }
    }

    /// Adds a new node to the graph
    ///
    /// The new node will depend on completion of all actions that were added in
    /// the last call to `append` or `append_parallel`.  (The idea is to `append`
    /// a sequence of steps that run one after another.)
    ///
    /// `action` will be used when this node is being executed.
    ///
    /// The node is called `name`.  This name is used for storing the output of
    /// the action so that descendant nodes can access it using
    /// [`crate::ActionContext::lookup`].
    pub fn append(&mut self, name: &str, label: &str, action: ActionName) {
        let newnode = self.graph.add_node(Node {
            name: name.to_string(),
            label: label.to_string(),
            action,
        });
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
    pub fn append_parallel(&mut self, actions: Vec<(&str, &str, ActionName)>) {
        let newnodes: Vec<NodeIndex> = actions
            .into_iter()
            .map(|(n, l, a)| {
                self.graph.add_node(Node {
                    name: n.to_string(),
                    label: l.to_string(),
                    action: a,
                })
            })
            .collect();

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
