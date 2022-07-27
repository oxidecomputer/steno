// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Construction and Recovery of sagas

use crate::saga_action_generic::Action;
use crate::saga_action_generic::ActionData;
use crate::SagaType;
use anyhow::anyhow;
use petgraph::dot;
use petgraph::graph::NodeIndex;
use petgraph::Directed;
use petgraph::Graph;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
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
    pub fn new<S: AsRef<str>>(name: S) -> ActionName {
        ActionName(name.as_ref().to_string())
    }
}

impl<S> From<S> for ActionName
where
    S: AsRef<str>,
{
    fn from(s: S) -> Self {
        ActionName::new(s)
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct NodeName(String);

impl NodeName {
    pub fn new<S: AsRef<str>>(name: S) -> NodeName {
        NodeName(name.as_ref().to_string())
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
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

/// A registry of saga actions that can be used across multiple sagas.
///
/// Actions can exist at multiple nodes in each saga DAG. Since saga
/// construction is dynamic and based upon user input, we need to allow a way
/// to insert actions at runtime into the DAG. While this could be achieved by
/// referencing the action during saga construction, this is not possible when
/// reloading a saga from persistent storage. In this case, the concrete type
/// of the Action is erased and the only mechanism we have to recover it is an
/// `ActionName`. We therefore have all users register their actions for use
/// across sagas so we can dynamically construct and restore sagas.
#[derive(Debug)]
pub struct ActionRegistry<UserType: SagaType> {
    actions: BTreeMap<ActionName, Arc<dyn Action<UserType>>>,
}

impl<UserType: SagaType> ActionRegistry<UserType> {
    pub fn new() -> ActionRegistry<UserType> {
        ActionRegistry { actions: BTreeMap::new() }
    }

    pub fn register(&mut self, action: Arc<dyn Action<UserType>>) {
        let already_inserted = self.actions.insert(action.name(), action);
        assert!(already_inserted.is_none());
    }

    pub fn get(
        &self,
        name: &ActionName,
    ) -> Result<Arc<dyn Action<UserType>>, ActionRegistryError> {
        self.actions.get(name).cloned().ok_or(ActionRegistryError::NotFound)
    }
}

/// Describes a node in the saga graph
///
/// There are three kinds of nodes you can add to a graph:
///
/// * an _action_ (see [`UserNode::action`]), which executes a particular
///   [`Action`] with an associated undo action
/// * a _constant_ (see [`UserNode::constant`]), which is like an action that
///   outputs a value that's known when the DAG is constructed
/// * a _subsaga_ (see [`UserNode::subsaga`]), which executes another DAG in the
///   context of this saga
///
/// Each of these node types has a `node_name` and produces an output.  Other
/// nodes that depend on this node (directly or indirectly) can access the
/// output by looking it up by the node name using
/// [`crate::ActionContext::lookup`]:
///
/// * The output of an action node is emitted by the action itself.
/// * The output of a constant node is the value provided when the node was
///   created (see [`UserNode::constant`]).
/// * The output of a subsaga node is the output of the subsaga itself.  Note
///   that the output of individual nodes from the subsaga DAG is _not_
///   available to other nodes in this DAG.  Only the final output is available.
#[derive(Debug, Clone)]
pub struct UserNode {
    node_name: NodeName,
    kind: UserNodeKind,
}

#[derive(Debug, Clone)]
enum UserNodeKind {
    Action { label: String, action_name: ActionName },
    Constant { value: serde_json::Value },
    Subsaga { params_node_name: NodeName, dag: Dag },
}

impl UserNode {
    pub fn action<N: AsRef<str>, L: AsRef<str>, A: Into<ActionName>>(
        node_name: N,
        label: L,
        action_name: A,
    ) -> UserNode {
        UserNode {
            node_name: NodeName::new(node_name),
            kind: UserNodeKind::Action {
                label: label.as_ref().to_string(),
                action_name: action_name.into(),
            },
        }
    }

    pub fn constant<N: AsRef<str>, V: Serialize>(
        node_name: N,
        value: V,
    ) -> UserNode {
        // XXX-dap unwrap
        UserNode {
            node_name: NodeName::new(node_name),
            kind: UserNodeKind::Constant {
                value: serde_json::to_value(value).unwrap(),
            },
        }
    }

    pub fn subsaga<N1: AsRef<str>, N2: AsRef<str>>(
        node_name: N1,
        dag: Dag,
        params_node_name: N2,
    ) -> UserNode {
        UserNode {
            node_name: NodeName::new(node_name),
            kind: UserNodeKind::Subsaga {
                params_node_name: NodeName::new(params_node_name),
                dag,
            },
        }
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
// XXX-dap TODO-doc
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum Node {
    Start { params: serde_json::Value },
    End,
    Action { name: NodeName, label: String, action_name: ActionName },
    Constant { name: NodeName, value: serde_json::Value },
    SubsagaStart { saga_name: SagaName, params_node_name: NodeName },
    SubsagaEnd { name: NodeName },
}

impl Node {
    pub fn name(&self) -> Option<&NodeName> {
        match self {
            Node::Start { .. } | Node::End | Node::SubsagaStart { .. } => None,
            Node::Action { name, .. } => Some(&name),
            Node::Constant { name, .. } => Some(&name),
            Node::SubsagaEnd { name, .. } => Some(&name),
        }
    }

    pub fn label(&self) -> String {
        match self {
            Node::Start { .. } => String::from("(start node)"),
            Node::End => String::from("(end node)"),
            Node::Action { label, .. } => label.clone(),
            // XXX-dap include the constant value
            Node::Constant { .. } => String::from("(constant)"),
            Node::SubsagaStart { saga_name, .. } => {
                format!("(subsaga start: {:?})", saga_name)
            }
            Node::SubsagaEnd { .. } => String::from("(subsaga end)"),
        }
    }
}

/// A DAG describing a saga
//
// Note: This doesn't implement JSON schema because of Graph and NodeIndex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    pub(crate) name: SagaName,
    pub(crate) graph: Graph<Node, ()>,
    pub(crate) start_node: NodeIndex,
    pub(crate) end_node: NodeIndex,
}

impl Dag {
    /// Return a node given its index
    pub fn get(&self, node_index: NodeIndex) -> Option<&Node> {
        self.graph.node_weight(node_index)
    }

    /// Return the index for a given node name
    pub fn get_index(&self, name: &str) -> Result<NodeIndex, anyhow::Error> {
        self.graph
            .node_indices()
            .find(|i| {
                self.graph[*i]
                    .name()
                    .map(|n| n.as_ref() == name)
                    .unwrap_or(false)
            })
            .ok_or_else(|| anyhow!("saga has no node named \"{}\"", name))
    }

    /// Returns an object that can be used to print a graphviz-format
    /// representation of the underlying node graph.
    pub fn dot(&self) -> DagDot<'_> {
        DagDot(&self.graph)
    }
}

/// Graphviz-formatted view of a saga graph
///
/// Use the `Display` impl to print a representation suitable as input to
/// the `dot` command.  You could put this into a file `graph.out` and run
/// something like `dot -Tpng -o graph.png graph.out` to produce `graph.png`, a
/// visual representation of the saga graph.
pub struct DagDot<'a>(&'a Graph<Node, (), Directed, u32>);
impl<'a> fmt::Display for DagDot<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = &[dot::Config::EdgeNoLabel];
        let dot = dot::Dot::with_config(&self.0, config);
        write!(f, "{:?}", dot)
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
    pub fn new<T>(name: SagaName, params: T) -> DagBuilder
    where
        T: ActionData,
    {
        let mut graph = Graph::new();

        // Append a default "start" node.
        // XXX-dap unwrap
        let params = serde_json::to_value(params)
            .expect("failed to serialize saga parameters");
        let root = graph.add_node(Node::Start { params });
        DagBuilder { name, graph, root, last_added: vec![root] }
    }

    /// Adds a new node to the graph
    ///
    /// The new node will depend on completion of all actions that were added in
    /// the last call to `append` or `append_parallel`.  (The idea is to `append`
    /// a sequence of steps that run one after another.)
    ///
    pub fn append(&mut self, user_node: UserNode) {
        let newnodes = &[self.add_node(user_node)];
        self.set_last(newnodes);
    }

    fn add_node(&mut self, user_node: UserNode) -> NodeIndex {
        match user_node.kind {
            UserNodeKind::Action { label, action_name } => {
                self.add_simple(Node::Action {
                    name: user_node.node_name,
                    label,
                    action_name,
                })
            }
            UserNodeKind::Constant { value } => {
                self.add_simple(Node::Constant {
                    name: user_node.node_name,
                    value,
                })
            }
            UserNodeKind::Subsaga { params_node_name, dag } => {
                self.add_subsaga(user_node.node_name, dag, params_node_name)
            }
        }
    }

    // XXX-dap TODO-doc The add_* functions add graph nodes, add dependencies
    // from "last", but do not _set_ "last".  They return a NodeIndex of the
    // last thing they added.

    /// Appends a `Node::Constant` or `Node::Action` to the graph
    fn add_simple(&mut self, node: Node) -> NodeIndex {
        assert!(matches!(node, Node::Constant { .. } | Node::Action { .. }));
        let newnode = self.graph.add_node(node);
        self.depends_on_last(newnode);
        newnode
    }

    fn depends_on_last(&mut self, newnode: NodeIndex) {
        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }
    }

    fn set_last(&mut self, nodes: &[NodeIndex]) {
        self.last_added = nodes.to_vec();
    }

    /// Append another DAG to this one (insert a subsaga)
    fn add_subsaga(
        &mut self,
        name: NodeName,
        subsaga: Dag,
        params_node_name: NodeName,
    ) -> NodeIndex {
        let node_start = Node::SubsagaStart {
            saga_name: subsaga.name.clone(),
            params_node_name: NodeName::new(params_node_name),
        };
        let subsaga_start = self.graph.add_node(node_start);
        self.depends_on_last(subsaga_start);

        // Insert all the nodes of the subsaga into this saga.
        let subgraph = &subsaga.graph;
        let mut subsaga_idx_to_saga_idx = BTreeMap::new();
        for child_node_index in 0..subgraph.node_count() {
            let child_node_index = NodeIndex::from(child_node_index as u32);
            let node = subgraph.node_weight(child_node_index).unwrap();

            // Skip the start node -- we handled that cleanly above.
            if let Node::Start { .. } = node {
                continue;
            }

            // When we get to the end node, we'll add a SubsagaEnd instead.
            // Other nodes are copied directly into the parent graph.
            // XXX-dap TODO-cleanup make this an exhaustive match
            let node = if let Node::End = node {
                Node::SubsagaEnd { name: name.clone() }
            } else {
                node.clone()
            };

            let parent_node_index = self.graph.add_node(node);
            assert!(subsaga_idx_to_saga_idx
                .insert(child_node_index, parent_node_index)
                .is_none());

            // For any incoming edges for this node in the subgraph, create a
            // corresponding edge in the new graph.
            for ancestor_child_node_index in subgraph
                .neighbors_directed(child_node_index, petgraph::Incoming)
            {
                let ancestor_child_node =
                    subgraph.node_weight(ancestor_child_node_index).unwrap();
                if matches!(ancestor_child_node, Node::Start { .. }) {
                    continue;
                }
                let ancestor_parent_node_index = subsaga_idx_to_saga_idx
                    .get(&ancestor_child_node_index)
                    .expect("graph was not a DAG");
                self.graph.add_edge(
                    *ancestor_parent_node_index,
                    parent_node_index,
                    (),
                );
            }
        }

        // Create the edges representing dependencies for the initial node(s) in
        // the subsaga.  These are the outgoing edges from the subsaga's start
        // node.
        for descendent_child_node_index in
            subgraph.neighbors_directed(subsaga.start_node, petgraph::Outgoing)
        {
            if let Some(descendent_parent_node_index) =
                subsaga_idx_to_saga_idx.get(&descendent_child_node_index)
            {
                self.graph.add_edge(
                    subsaga_start,
                    *descendent_parent_node_index,
                    (),
                );
            }
        }

        // Set `last_added` so that we can keep adding to it.
        let end_child_node_index = subsaga.end_node;
        let end_parent_node_index = subsaga_idx_to_saga_idx
            .get(&end_child_node_index)
            .expect("end node was not processed");
        *end_parent_node_index
    }

    /// Adds a set of nodes to the graph that can be executed concurrently
    ///
    /// The new nodes will individually depend on completion of all actions that
    /// were added in the last call to `append` or `append_parallel`.  `actions`
    /// is a vector of `(name, action)` tuples analogous to the arguments to
    /// [`DagBuilder::append`].
    pub fn append_parallel(&mut self, user_nodes: Vec<UserNode>) {
        let newnodes: Vec<NodeIndex> = user_nodes
            .into_iter()
            .map(|user_node| self.add_node(user_node))
            .collect();

        // XXX-dap TODO-coverage test two sagas in parallel

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
        self.set_last(&newnodes);
    }

    /// Return the constructed DAG
    ///
    /// Building the DAG automatically creates an "end" node
    pub fn build(mut self) -> Dag {
        // Append an "end" node so that we can easily tell when the saga has
        // completed.
        let newnode = self.graph.add_node(Node::End);

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

/// An abstract specification for a subsaga that can be used by a Dag
/// to generate and append saga nodes.
///
/// An example implementation is shown below:
///
/// ```ignore
/// fn create_disk_subsaga<'a>() -> SagaSpec<'a> {
///     SagaSpec(
///         vec![
///             NodeConcurrency::Linear(vec![NodeSpec {
///                 name: "disk_id",
///                 label: "GenerateDiskId",
///                 action: "saga_generate_uuid",
///             }]),
///             NodeConcurrency::Parallel(vec![NodeSpec {...}, NodeSpec {...}]),
///         ],
///     )
/// }
/// ```
///
///
/// TODO: Semantic validation (actions exist in registry, etc...)
pub struct SagaSpec<'a>(pub Vec<NodeConcurrency<'a>>);

/// An abstract description of the shape of node execution
///
/// TODO: Do we want to allow arbitrary recursion here?
pub enum NodeConcurrency<'a> {
    Linear(Vec<NodeSpec<'a>>),
    Parallel(Vec<NodeSpec<'a>>),
}

/// A specification for creating a node. This allows dynamic creation of nodes
/// for subsagas with different instance_ids and creation parameters.
pub struct NodeSpec<'a> {
    pub name: &'a str,
    pub label: &'a str,
    pub action: &'a str,
}

impl<'a> SagaSpec<'a> {
    pub fn to_dag<T>(&self, saga_name: SagaName, params: T) -> Dag
    where
        T: ActionData,
    {
        let mut builder = DagBuilder::new(saga_name, params);

        for concurrency in &self.0 {
            match concurrency {
                NodeConcurrency::Linear(specs) => {
                    for spec in specs {
                        builder.append(UserNode::action(
                            spec.name,
                            spec.label,
                            spec.action,
                        ));
                    }
                }
                NodeConcurrency::Parallel(specs) => {
                    let nodes = specs
                        .iter()
                        .map(|spec| {
                            UserNode::action(spec.name, spec.label, spec.action)
                        })
                        .collect();
                    builder.append_parallel(nodes);
                }
            }
        }

        builder.build()
    }
}