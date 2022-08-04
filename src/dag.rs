// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Saga and saga DAG construction
//!
//! A _saga_ is primarily made up of a directed acylic graph (DAG) of saga
//! _nodes_, most of which are _actions_.  The facilities in this module are
//! used to build up sagas.  It looks like this:
//!
//! At the lowest layer, we have [`Node`]s, which usually describe an action.
//! Use [`DagBuilder`] to assemble these into a [`Dag`].  The resulting `Dag`
//! can be used in one of two ways:
//!
//! 1. When combined with parameters for the saga, the `Dag` becomes a
//!    [`SagaDag`] and can be executed using the [`crate::sec()`].
//! 2. Alternatively, the `Dag` can be used as a _subsaga_ of some other saga.
//!    To do this, use [`Node::subsaga`] to create a subsaga _node_ containing
//!    the subsaga `Dag`, then append this to the [`DagBuilder`] that's used to
//!    construct the outer saga.
// Note: The graph-related types here don't implement JSON schema because of
// Graph and NodeIndex.

use crate::saga_action_generic::Action;
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
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// Unique identifier for a Saga (an execution of a saga template)
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
// TODO-design In the Oxide consumer, we probably want to have the serialized
// form of ids have a prefix describing the type.  This seems consumer-specific,
// though.  Is there a good way to do support that?  Maybe the best way to do
// this is to have the consumer have their own enum or trait that impls Display
// using the various ids provided by consumers.
NewtypeDisplay! { () pub struct SagaId(Uuid); }
NewtypeFrom! { () pub struct SagaId(Uuid); }

/// Unique name for a saga [`Action`]
///
/// Each action requires a string name that's unique within an
/// [`ActionRegistry`].  During normal execution and when recovering sagas after
/// a crash, the name is used to link each node in the DAG with the
/// [`Action`] implementation.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct ActionName(String);

impl ActionName {
    pub fn new<S: AsRef<str>>(name: S) -> ActionName {
        ActionName(name.as_ref().to_string())
    }
}

impl fmt::Debug for ActionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
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

/// Unique name for a saga [`Node`]
///
/// Each node requires a string name that's unique within its DAG.  The name is
/// used to identify its output.  Nodes that depend on a given node (either
/// directly or indirectly) can access the node's output using its name.
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

/// Human-readable name for a particular saga
///
/// Steno makes no assumptions about the semantics of this name.  Consumers may
/// wish to use this as a unique identifier.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct SagaName(String);

NewtypeDisplay! { () pub struct SagaName(String); }

impl SagaName {
    pub fn new(name: &str) -> SagaName {
        SagaName(name.to_string())
    }
}

impl fmt::Debug for SagaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}", self.0))
    }
}

/// An error returned from [`ActionRegistry::get()`]
#[derive(Debug)]
pub enum ActionRegistryError {
    /// No action has been registered with the specified name
    NotFound,
}

/// A registry of saga actions that can be used across multiple sagas.
///
/// Actions are identified by their [`ActionName`].
// Actions can exist at multiple nodes in each saga DAG. Since saga construction
// is dynamic and based upon user input, we need to allow a way to insert
// actions at runtime into the DAG. While this could be achieved by referencing
// the action during saga construction, this is not possible when reloading a
// saga from persistent storage. In this case, the concrete type of the Action
// is erased and the only mechanism we have to recover it is an `ActionName`. We
// therefore have all users register their actions for use across sagas so we
// can dynamically construct and restore sagas.
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

/// Describes a node in the saga DAG
///
/// There are three kinds of nodes you can add to a graph:
///
/// * an _action_ (see [`Node::action`]), which executes a particular [`Action`]
///   with an associated undo action
/// * a _constant_ (see [`Node::constant`]), which is like an action that
///   outputs a value that's known when the DAG is constructed
/// * a _subsaga_ (see [`Node::subsaga`]), which executes another DAG in the
///   context of this saga
///
/// Each of these node types has a `node_name` and produces an output.  Other
/// nodes that depend on this node (directly or indirectly) can access the
/// output by looking it up by the node name using
/// [`crate::ActionContext::lookup`]:
///
/// * The output of an action node is emitted by the action itself.
/// * The output of a constant node is the value provided when the node was
///   created (see [`Node::constant`]).
/// * The output of a subsaga node is the output of the subsaga itself.  Note
///   that the output of individual nodes from the subsaga DAG is _not_
///   available to other nodes in this DAG.  Only the final output is available.
#[derive(Debug, Clone)]
pub struct Node {
    node_name: NodeName,
    kind: NodeKind,
}

#[derive(Debug, Clone)]
enum NodeKind {
    Action { label: String, action_name: ActionName },
    Constant { value: serde_json::Value },
    Subsaga { params_node_name: NodeName, dag: Dag },
}

impl Node {
    /// Make a new action node (see [`Node`])
    ///
    /// This node is used to execute the given action.  The action's output will
    /// be available to dependent nodes by looking up the name `node_name`.  See
    /// [`Action`] for more information.
    pub fn action<N: AsRef<str>, L: AsRef<str>, A: SagaType>(
        node_name: N,
        label: L,
        action: &dyn Action<A>,
    ) -> Node {
        Node {
            node_name: NodeName::new(node_name),
            kind: NodeKind::Action {
                label: label.as_ref().to_string(),
                action_name: action.name(),
            },
        }
    }

    /// Make a new constant node (see [`Node`])
    ///
    /// This node immediately emits `value`.  Why would you want this?  Suppose
    /// you're working with some saga action that expects input to come from
    /// some previous saga node.  But in your case, you know the input up front.
    /// You can use this to provide the value to the downstream action.
    pub fn constant<N: AsRef<str>>(
        node_name: N,
        value: serde_json::Value,
    ) -> Node {
        Node {
            node_name: NodeName::new(node_name),
            kind: NodeKind::Constant { value },
        }
    }

    /// Make a new subsaga node (see [`Node`])
    ///
    /// This is used to insert a subsaga into another saga.  The output of the
    /// subsaga will have name `node_name` in the outer saga.  The subsaga's DAG
    /// is described by `dag`.  Its input parameters will come from node
    /// `params_node_name` in the outer saga.
    pub fn subsaga<N1: AsRef<str>, N2: AsRef<str>>(
        node_name: N1,
        dag: Dag,
        params_node_name: N2,
    ) -> Node {
        Node {
            node_name: NodeName::new(node_name),
            kind: NodeKind::Subsaga {
                params_node_name: NodeName::new(params_node_name),
                dag,
            },
        }
    }
}

/// A Node in the saga DAG (internal representation)
///
/// Since sagas are constructed dynamically at runtime, we don't know the
/// shape of the graph ahead of time. We need to maintain enough information
/// to reconstruct the saga when loaded from persistent storage. The easiest
/// way to do that is to store the graph itself with enough information in
/// each node that allows us to recreate the Saga. Note that we don't store
/// the execution state of the saga here. That continues to reside in saga log
/// consisting of `SagaNodeEvent`s.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum InternalNode {
    Start { params: Arc<serde_json::Value> },
    End,
    Action { name: NodeName, label: String, action_name: ActionName },
    Constant { name: NodeName, value: Arc<serde_json::Value> },
    SubsagaStart { saga_name: SagaName, params_node_name: NodeName },
    SubsagaEnd { name: NodeName },
}

impl InternalNode {
    pub fn node_name(&self) -> Option<&NodeName> {
        match self {
            InternalNode::Start { .. }
            | InternalNode::End
            | InternalNode::SubsagaStart { .. } => None,
            InternalNode::Action { name, .. } => Some(&name),
            InternalNode::Constant { name, .. } => Some(&name),
            InternalNode::SubsagaEnd { name, .. } => Some(&name),
        }
    }

    pub fn label(&self) -> String {
        match self {
            InternalNode::Start { .. } => String::from("(start node)"),
            InternalNode::End => String::from("(end node)"),
            InternalNode::Action { label, .. } => label.clone(),
            InternalNode::Constant { value, .. } => {
                let value_as_json = serde_json::to_string(value)
                    .unwrap_or_else(|e| {
                        format!("(failed to serialize constant value: {:#})", e)
                    });
                format!("(constant = {})", value_as_json)
            }
            InternalNode::SubsagaStart { saga_name, .. } => {
                format!("(subsaga start: {:?})", saga_name)
            }
            InternalNode::SubsagaEnd { .. } => String::from("(subsaga end)"),
        }
    }
}

/// A [`Dag`] plus saga input parameters that together can be used to execute a
/// saga
///
/// The output of the saga is the output of the last node in the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaDag {
    /// name of the saga (intended primarily for use by the consumer)
    pub(crate) saga_name: SagaName,
    /// the actual DAG representation
    ///
    /// Unlike [`Dag`], [`SagaDag`]'s graph can contain any type of [`Node`].
    /// There is always exactly one [`InternalNode::Start`] node and exactly one
    /// [`InternalNode::End`] node.  The graph can contain subsagas, which are
    /// always bracketed by [`InternalNode::SubsagaStart`] and
    /// [`InternalNode::SubsagaEnd`] nodes.
    pub(crate) graph: Graph<InternalNode, ()>,
    /// the index of the [`InternalNode::Start`] node for this Saga
    pub(crate) start_node: NodeIndex,
    /// the index of the [`InternalNode::End`] node for this Saga
    pub(crate) end_node: NodeIndex,
}

impl SagaDag {
    /// Make a [`SagaDag`] from the given DAG and input parameters
    pub fn new(dagfrag: Dag, params: serde_json::Value) -> SagaDag {
        // Wrap the DAG with a Start node (which stores the parameters) and an
        // end node so that we can easily tell when the saga has completed.
        let mut graph = dagfrag.graph;
        let start_node =
            graph.add_node(InternalNode::Start { params: Arc::new(params) });
        let end_node = graph.add_node(InternalNode::End);

        // The first-added nodes in the graph depend on the "start" node.
        for first_node in &dagfrag.first_nodes {
            graph.add_edge(start_node, *first_node, ());
        }

        // The "end" node depends on the last-added nodes in the DAG.
        for last_node in &dagfrag.last_nodes {
            graph.add_edge(*last_node, end_node, ());
        }

        SagaDag {
            saga_name: dagfrag.saga_name,
            graph: graph,
            start_node,
            end_node,
        }
    }

    pub fn saga_name(&self) -> &SagaName {
        &self.saga_name
    }

    /// Return a node given its index
    pub(crate) fn get(&self, node_index: NodeIndex) -> Option<&InternalNode> {
        self.graph.node_weight(node_index)
    }

    /// Return the index for a given node name
    pub fn get_index(&self, name: &str) -> Result<NodeIndex, anyhow::Error> {
        self.graph
            .node_indices()
            .find(|i| {
                self.graph[*i]
                    .node_name()
                    .map(|n| n.as_ref() == name)
                    .unwrap_or(false)
            })
            .ok_or_else(|| anyhow!("saga has no node named \"{}\"", name))
    }

    /// Returns an object that can be used to print a graphviz-format
    /// representation of the underlying DAG
    pub fn dot(&self) -> DagDot<'_> {
        DagDot(&self.graph)
    }
}

/// Graphviz-formatted view of a saga DAG
///
/// Use the `Display` impl to print a representation suitable as input to
/// the `dot` command.  You could put this into a file `graph.out` and run
/// something like `dot -Tpng -o graph.png graph.out` to produce `graph.png`, a
/// visual representation of the saga graph.
pub struct DagDot<'a>(&'a Graph<InternalNode, (), Directed, u32>);
impl<'a> fmt::Display for DagDot<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = &[dot::Config::EdgeNoLabel];
        let dot = dot::Dot::with_config(&self.0, config);
        write!(f, "{:?}", dot)
    }
}

/// Describes a directed acyclic graph (DAG) to be used as a saga or subsaga
///
/// If you want to run this `Dag` as a saga, you need to create a [`SagaDag`]
/// (which requires providing input parameters).
///
/// If you want to insert this `Dag` into another saga (as a subsaga), use
/// [`Node::subsaga()`] to create a subsaga _node_ and append this to the outer
/// saga's [`DagBuilder`].
///
/// This type is built with [`DagBuilder`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    /// name of the saga (intended primarily for use by the consumer)
    saga_name: SagaName,

    /// the actual DAG representation
    ///
    /// This graph does *not* contain a [`InternalNode::Start`] or
    /// [`InternalNode::End`] node.  Those only make sense for `Dag`s that will
    /// become top-level sagas (as opposed to subsagas).  Instead, we keep track
    /// of the first group of DAG (root nodes) and the last group of DAG nodes
    /// (leaf nodes).  Later, we'll wrap this `Dag` in either [`SagaDag`] (for
    /// use as a top-level saga), in which case we'll add the start and end
    /// nodes, or we'll use it as a subsaga, in which case we'll add
    /// SubsagaStart and SubsagaEnd nodes.
    graph: Graph<InternalNode, ()>,

    /// the initial nodes (root nodes) of the DAG
    first_nodes: Vec<NodeIndex>,

    /// the last nodes (leaf nodes) of the DAG
    last_nodes: Vec<NodeIndex>,
}

/// Used to build a [`Dag`] that can then be executed as either a saga or
/// subsaga
///
/// Use [`DagBuilder::append()`] and [`DagBuilder::append_parallel()`] to add
/// nodes to the graph.  Use [`DagBuilder::build()`] to finish construction and
/// build a [`Dag`].
#[derive(Debug)]
pub struct DagBuilder {
    /// name of the saga (intended primarily for use by the consumer)
    saga_name: SagaName,
    /// the actual DAG representation
    ///
    /// This looks the same as [`Dag`]'s `graph`.
    graph: Graph<InternalNode, ()>,

    /// the initial set of nodes (root nodes), if any have been added
    first_added: Option<Vec<NodeIndex>>,

    /// the most-recently-added set of nodes (current leaf nodes)
    ///
    /// Callers use the builder by appending a sequence of nodes (or subsagas).
    /// Some of these may run concurrently.
    ///
    /// The `append()`/`append_parallel()` functions are public.  As the names
    /// imply, they append new nodes to the graph.  They also update
    /// "last_added" so that the next set of nodes will depend on the ones that
    /// were just added.
    ///
    /// The `add_*()` functions are private, for use only by
    /// `append()`/`append_parallel()`.  These functions have a consistent
    /// pattern: they add nodes to the graph, they create dependencies from
    /// each node in "last_added" to each of the new nodes, and they return the
    /// index of the last node they added.  They do _not_ update "last_added"
    /// themselves.
    last_added: Vec<NodeIndex>,

    /// names of nodes added so far
    node_names: BTreeSet<NodeName>,

    /// error from any builder operation (returned by `build()`)
    error: Option<DagBuilderErrorKind>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("building saga \"{saga_name}\": {kind:#}")]
pub struct DagBuilderError {
    saga_name: SagaName,

    #[source]
    kind: DagBuilderErrorKind,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
enum DagBuilderErrorKind {
    #[error("saga must end with exactly one node")]
    /// The saga ended with zero nodes (if it was empty) or more than one node.
    /// Sagas are required to end with a single node that emits the output for
    /// the saga itself.
    BadOutputNode,

    #[error(
        "subsaga node {0:?} has parameters that come from node {1:?}, but it \
        does not depend on any such node"
    )]
    /// A subsaga was appended whose parameters were supposed to come from a
    /// node that does not exist or that the subsaga does not depend on.
    BadSubsagaParams(NodeName, NodeName),

    #[error("name was used multiple times in the same Dag: {0:?}")]
    /// The same name was given to multiple nodes in the same DAG.  This is not
    /// allowed.  Node names must be unique because they are used to identify
    /// outputs.
    DuplicateName(NodeName),

    #[error("attempted to append 0 nodes in parallel")]
    /// It's not allowed to append 0 nodes in parallel.
    EmptyStage,
}

impl DagBuilder {
    /// Begin building a DAG for a saga or subsaga
    pub fn new(saga_name: SagaName) -> DagBuilder {
        DagBuilder {
            saga_name,
            graph: Graph::new(),
            first_added: None,
            last_added: vec![],
            node_names: BTreeSet::new(),
            error: None,
        }
    }

    /// Adds a new node to the graph to be run after the most-recently-appended
    /// node(s)
    ///
    /// The new node will depend on completion of all actions that were added in
    /// the last call to `append` or `append_parallel`.  The idea is to `append`
    /// a sequence of steps that run one after another.
    pub fn append(&mut self, user_node: Node) {
        self.append_parallel(vec![user_node])
    }

    /// Adds a set of nodes to the graph that depend on the
    /// most-recently-appended node(s) but that can be executed concurrently
    /// with each other
    ///
    /// The new nodes will individually depend on completion of all actions that
    /// were added in the last call to `append()` or `append_parallel()`.
    pub fn append_parallel(&mut self, user_nodes: Vec<Node>) {
        // If we've encountered an error already, don't do anything.  We'll
        // report this when the user invokes `build()`.
        if self.error.is_some() {
            return;
        }

        // It's not allowed to have an empty stage.  It's not clear what would
        // be intended by this.  With the current implementation, you'd wind up
        // creating two separate connected components of the DAG, which violates
        // all kinds of assumptions.
        if user_nodes.len() == 0 {
            self.error = Some(DagBuilderErrorKind::EmptyStage);
            return;
        }

        // Validate that if we're appending a subsaga, then the node from which
        // it gets its parameters has already been appended.  If it hasn't been
        // appended, this is definitely invalid because we'd have no way to get
        // these parameters when we need them.  As long as the parameters node
        // has been appended already, then the new subsaga node will depend on
        // that node (either directly or indirectly).
        //
        // It might seem more natural to validate this in `add_subsaga()`.  But
        // if we're appending multiple nodes in parallel here, then by the time
        // we get to `add_subsaga()`, it's possible that we've added nodes to
        // the graph on which the subsaga does _not_ depend.  That would cause
        // us to erroneously think this is valid when it's not.
        //
        // If this fails, we won't report it until the user calls `build()`.  In
        // the meantime, we proceed with the building.  The problem here doesn't
        // make that any harder.
        for node in &user_nodes {
            if let NodeKind::Subsaga { params_node_name, .. } = &node.kind {
                if !self.node_names.contains(&params_node_name)
                    && self.error.is_none()
                {
                    self.error = Some(DagBuilderErrorKind::BadSubsagaParams(
                        node.node_name.clone(),
                        params_node_name.clone(),
                    ));
                    return;
                }
            }
        }

        // Now validate that the names of these new nodes are unique.  It's
        // important that we do this after the above check because otherwise if
        // the user added a subsaga node whose parameters came from a parallel
        // node (added in the same call), we wouldn't catch the problem.
        for node in &user_nodes {
            if !self.node_names.insert(node.node_name.clone()) {
                self.error = Some(DagBuilderErrorKind::DuplicateName(
                    node.node_name.clone(),
                ));
                return;
            }
        }

        // Now we can proceed with adding the nodes.
        let newnodes: Vec<NodeIndex> = user_nodes
            .into_iter()
            .map(|user_node| self.add_node(user_node))
            .collect();

        if self.first_added.is_none() {
            self.first_added = Some(newnodes.clone());
        }

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

    // Implementation note: the add_* functions here add nodes to the DAG, add
    // dependencies from `self.last` (the last set of nodes added), but do NOT
    // set `self.last`.  They return the `NodeIndex` of the last thing they
    // added.

    /// Adds any kind of [`Node`] to the graph
    fn add_node(&mut self, user_node: Node) -> NodeIndex {
        match user_node.kind {
            NodeKind::Action { label, action_name } => {
                self.add_simple(InternalNode::Action {
                    name: user_node.node_name,
                    label,
                    action_name,
                })
            }
            NodeKind::Constant { value } => {
                self.add_simple(InternalNode::Constant {
                    name: user_node.node_name,
                    value: Arc::new(value),
                })
            }
            NodeKind::Subsaga { params_node_name, dag } => {
                self.add_subsaga(user_node.node_name, dag, params_node_name)
            }
        }
    }

    /// Adds a `InternalNode::Constant` or `InternalNode::Action` to the graph
    fn add_simple(&mut self, node: InternalNode) -> NodeIndex {
        assert!(matches!(
            node,
            InternalNode::Constant { .. } | InternalNode::Action { .. }
        ));
        let newnode = self.graph.add_node(node);
        self.depends_on_last(newnode);
        newnode
    }

    /// Adds another DAG to this one as a subsaga
    ///
    /// This isn't quite the same as inserting the given DAG into the DAG that
    /// we're building.  Subsaga nodes live in a separate namespace of node
    /// names.  We do this by adding the SubsagaStart and SubsagaEnd nodes
    /// around the given DAG.
    fn add_subsaga(
        &mut self,
        name: NodeName,
        subsaga_dag: Dag,
        params_node_name: NodeName,
    ) -> NodeIndex {
        let node_start = InternalNode::SubsagaStart {
            saga_name: subsaga_dag.saga_name.clone(),
            params_node_name: NodeName::new(params_node_name),
        };
        let subsaga_start = self.graph.add_node(node_start);
        self.depends_on_last(subsaga_start);

        // Insert all the nodes of the subsaga into this saga.
        let subgraph = &subsaga_dag.graph;
        let mut subsaga_idx_to_saga_idx = BTreeMap::new();
        for child_node_index in 0..subgraph.node_count() {
            let child_node_index = NodeIndex::from(child_node_index as u32);
            let node = subgraph.node_weight(child_node_index).unwrap().clone();

            // Dags are not allowed to have Start/End nodes.  These are only
            // added to `SagaDag`s.  Given that, we can copy the rest of the
            // nodes directly into the parent graph.
            match node {
                InternalNode::Start { .. } | InternalNode::End => {
                    panic!("subsaga Dag contained unexpected node: {:?}", node);
                }
                InternalNode::Action { .. }
                | InternalNode::Constant { .. }
                | InternalNode::SubsagaStart { .. }
                | InternalNode::SubsagaEnd { .. } => (),
            };

            // We already appended the start node
            let parent_node_index = self.graph.add_node(node);

            assert!(subsaga_idx_to_saga_idx
                .insert(child_node_index, parent_node_index)
                .is_none());

            // For any incoming edges for this node in the subgraph, create a
            // corresponding edge in the new graph.
            for ancestor_child_node_index in subgraph
                .neighbors_directed(child_node_index, petgraph::Incoming)
            {
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

        // The initial nodes of the subsaga DAG must depend on the SubsagaStart
        // node that we added.
        for child_first_node in &subsaga_dag.first_nodes {
            let parent_first_node =
                subsaga_idx_to_saga_idx.get(&child_first_node).unwrap();
            self.graph.add_edge(subsaga_start, *parent_first_node, ());
        }

        // Add a SubsagaEnd node that depends on the last nodes of the subsaga
        // DAG.
        let subsaga_end =
            self.graph.add_node(InternalNode::SubsagaEnd { name });
        for child_last_node in &subsaga_dag.last_nodes {
            let parent_last_node =
                subsaga_idx_to_saga_idx.get(&child_last_node).unwrap();
            self.graph.add_edge(*parent_last_node, subsaga_end, ());
        }

        subsaga_end
    }

    /// Record that node `newnode` depends on the last set of nodes that were
    /// appended
    fn depends_on_last(&mut self, newnode: NodeIndex) {
        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }
    }

    /// Record that the nodes in `nodes` should be ancestors of whatever nodes
    /// get added next.
    fn set_last(&mut self, nodes: &[NodeIndex]) {
        self.last_added = nodes.to_vec();
    }

    /// Return the constructed DAG
    pub fn build(self) -> Result<Dag, DagBuilderError> {
        // If we ran into a problem along the way, report it now.
        if let Some(error) = self.error {
            return Err(DagBuilderError {
                saga_name: self.saga_name.clone(),
                kind: error,
            });
        }

        // Every saga must end in exactly one leaf node.
        if self.last_added.len() != 1 {
            return Err(DagBuilderError {
                saga_name: self.saga_name.clone(),
                kind: DagBuilderErrorKind::BadOutputNode,
            });
        }

        Ok(Dag {
            saga_name: self.saga_name,
            graph: self.graph,
            first_nodes: self.first_added.unwrap_or_else(|| Vec::new()),
            last_nodes: self.last_added,
        })
    }
}

#[cfg(test)]
mod test {
    use super::DagBuilder;
    use super::DagBuilderErrorKind;
    use super::Node;
    use super::NodeName;
    use super::SagaName;

    #[test]
    fn test_builder_bad_output_nodes() {
        // error case: totally empty DAG
        let builder = DagBuilder::new(SagaName::new("test-saga"));
        let result = builder.build();
        println!("{:?}", result);
        match result {
            Ok(_) => panic!("unexpected success"),
            Err(error) => {
                assert_eq!(error.saga_name.to_string(), "test-saga");
                assert!(matches!(
                    error.kind,
                    DagBuilderErrorKind::BadOutputNode
                ));
                assert_eq!(
                    error.to_string(),
                    "building saga \"test-saga\": \
                    saga must end with exactly one node"
                );
            }
        };

        // error case: a DAG that ends with two nodes
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append_parallel(vec![
            Node::constant("a", serde_json::Value::Null),
            Node::constant("b", serde_json::Value::Null),
        ]);
        let result = builder.build();
        println!("{:?}", result);
        assert!(matches!(
            result.unwrap_err().kind,
            DagBuilderErrorKind::BadOutputNode
        ));
    }

    #[test]
    fn test_builder_empty_stage() {
        // error case: a DAG with a 0-node stage in it
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append_parallel(vec![]);
        let result = builder.build();
        println!("{:?}", result);
        let error = result.unwrap_err();
        assert!(matches!(error.kind, DagBuilderErrorKind::EmptyStage));
        assert_eq!(
            error.to_string(),
            "building saga \"test-saga\": \
            attempted to append 0 nodes in parallel"
        );
    }

    #[test]
    fn test_builder_duplicate_names() {
        // error case: a DAG that duplicates names (direct ancestor)
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append(Node::constant("a", serde_json::Value::Null));
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::DuplicateName(NodeName::new("a"))
        );
        assert_eq!(
            error.to_string(),
            "building saga \"test-saga\": \
            name was used multiple times in the same Dag: \"a\""
        );

        // error case: a DAG that duplicates names (indirect ancestor)
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append_parallel(vec![
            Node::constant("b", serde_json::Value::Null),
            Node::constant("c", serde_json::Value::Null),
        ]);
        builder.append(Node::constant("a", serde_json::Value::Null));
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::DuplicateName(NodeName::new("a"))
        );

        // error case: a DAG that duplicates names (parallel)
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append_parallel(vec![
            Node::constant("b", serde_json::Value::Null),
            Node::constant("b", serde_json::Value::Null),
        ]);
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::DuplicateName(NodeName::new("b"))
        );

        // success case: a DAG that uses the same name for a node outside and
        // inside a subsaga
        let mut inner_builder = DagBuilder::new(SagaName::new("inner-saga"));
        inner_builder.append(Node::constant("a", serde_json::Value::Null));
        let inner_dag = inner_builder.build().unwrap();
        let mut outer_builder = DagBuilder::new(SagaName::new("outer-saga"));
        outer_builder.append(Node::constant("a", serde_json::Value::Null));
        outer_builder.append(Node::subsaga("b", inner_dag, "a"));
        let _ = outer_builder.build().unwrap();
    }

    #[test]
    fn test_builder_bad_subsaga_params() {
        let mut subsaga_builder = DagBuilder::new(SagaName::new("inner-saga"));
        subsaga_builder.append(Node::constant("a", serde_json::Value::Null));
        let subsaga_dag = subsaga_builder.build().unwrap();

        // error case: subsaga depends on params node that doesn't exist
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append(Node::subsaga("b", subsaga_dag.clone(), "barf"));
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::BadSubsagaParams(
                NodeName::new("b"),
                NodeName::new("barf")
            )
        );
        assert_eq!(
            error.to_string(),
            "building saga \"test-saga\": \
            subsaga node \"b\" has parameters that come from node \"barf\", \
            but it does not depend on any such node"
        );

        // error case: subsaga depends on params node that doesn't exist
        // (itself)
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append(Node::subsaga("b", subsaga_dag.clone(), "b"));
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::BadSubsagaParams(
                NodeName::new("b"),
                NodeName::new("b")
            )
        );

        // error case: subsaga depends on params node that doesn't exist
        // (added in parallel)
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::constant("a", serde_json::Value::Null));
        builder.append_parallel(vec![
            Node::constant("c", serde_json::Value::Null),
            Node::subsaga("b", subsaga_dag.clone(), "c"),
        ]);
        let error = builder.build().unwrap_err();
        println!("{:?}", error);
        assert_eq!(
            error.kind,
            DagBuilderErrorKind::BadSubsagaParams(
                NodeName::new("b"),
                NodeName::new("c")
            )
        );
    }
}
