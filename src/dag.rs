// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction and Recovery of sagas

use crate::saga_action_generic::{
    Action, ActionData, ActionEndNode, ActionStartNode,
};
use crate::SagaType;
use anyhow::Context;
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
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ActionName(String);

impl ActionName {
    pub fn new(name: &str) -> ActionName {
        ActionName(name.to_string())
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct SagaName(String);

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
        // Insert the default start action for a saga
        let action: Arc<dyn Action<UserType> + 'static> =
            Arc::new(ActionStartNode {});
        let name = ActionName("__steno_action_start_node__".to_string());
        actions.insert(name, action).unwrap();

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub label: String,
    pub action: ActionName,
}

/// A DAG describing a saga
//
// TODO(AJS) - Create a separate metadata type?
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    name: SagaName,
    pub(crate) graph: Graph<Node, ()>,
    create_params: Arc<serde_json::Value>,
    pub(crate) start_node: NodeIndex,
    pub(crate) end_node: NodeIndex,
}

impl Dag {
    pub fn get(&self, node_index: NodeIndex) -> Option<&Node> {
        self.graph.node_weight(node_index)
    }

    pub fn recover<ST>(
        self: Arc<Dag>,
        registry: Arc<ActionRegistry<ST>>,
        log: slog::Logger,
        saga_id: crate::SagaId,
        user_context: Arc<ST::ExecContextType>,
        sec: crate::sec::SecExecClient,
        sglog: crate::SagaLog,
    ) -> Result<Arc<dyn crate::SagaExecManager>, anyhow::Error>
    where
        ST: SagaType,
    {
        let params_deserialized =
            serde_json::from_value::<ST::SagaParamsType>(*self.create_params)
                .context("deserializing saga parameters")?;
        Ok(Arc::new(crate::saga_exec::SagaExecutor::new_recover(
            log,
            saga_id,
            self,
            registry,
            user_context,
            params_deserialized,
            sec,
            sglog,
        )?))
    }
}

#[derive(Debug)]
pub struct DagBuilder {
    name: SagaName,
    graph: Graph<Node, ()>,
    root: NodeIndex,
    create_params: Arc<serde_json::Value>,
    last_added: Vec<NodeIndex>,
}

impl DagBuilder {
    /// Create a new DAG for a Saga
    ///
    /// Creation of a DAG results in the automatic creation of a "start" node.
    /// The output of this node is the `create_params`. This allows descendant nodes
    /// to lookup the creation parameters via [`crate::ActionContext::lookup`].
    pub fn new<C>(name: SagaName, create_params: &C) -> DagBuilder
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

        DagBuilder { name, graph, root, create_params, last_added: vec![root] }
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

    /// Return the constructed DAG
    ///
    /// Building the DAG automatically creates an "end" node
    pub fn build(mut self) -> Dag {
        // Append an "end" node so that we can easily tell when the saga has
        // completed.
        let newnode = self.graph.add_node(Node {
            name: "__EndNode__".to_string(),
            label: "EndNode".to_string(),
            action: ActionName("__steno_action_end_node__".to_string()),
        });

        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        Dag {
            name: self.name,
            graph: self.graph,
            create_params: self.create_params,
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
