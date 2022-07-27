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
    // XXX don't want the variants to be public
    Start { params: serde_json::Value },
    End,
    Action { name: NodeName, label: String, action: ActionName },
    Constant { name: NodeName, value: serde_json::Value },
    SubsagaStart { saga_name: SagaName, params_node_name: NodeName },
    SubsagaEnd { name: NodeName },
}

impl Node {
    /// Create a new child node for a saga or subsaga
    pub fn new_child<S1: AsRef<str>, S2: AsRef<str>, A: Into<ActionName>>(
        name: S1,
        label: S2,
        action: A,
    ) -> Node {
        Node::Action {
            name: NodeName::new(name),
            label: label.as_ref().to_string(),
            action: action.into(),
        }
    }

    /**
     * Create a node that emits a constant value (known when the saga is created)
     *
     * Why would you want this?  Suppose you're working with some saga action
     * that expects input to come from some previous saga node.  But in your
     * case, you know the input up front.  You can use this to provide the value
     * to the downstream action.
     */
    pub fn new_constant<S: AsRef<str>, T: ActionData>(
        name: S,
        value: T,
    ) -> Node {
        let value = serde_json::to_value(value).unwrap(); // XXX-dap
        Node::Constant { name: NodeName::new(name), value }
    }

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
    /// `action` will be used when this node is being executed.
    ///
    /// The node is called `name`. This name is used along with `instance_id`
    /// for storing the output of the action so that descendant nodes can
    /// access it using [`crate::ActionContext::lookup`].
    ///
    /// The instance id is useful because there may be many subsagas of the same
    /// type that run as part of the main saga. Each one of nodes in each subsaga
    /// will only want to access data from its own nodes, and a common name for
    /// those nodes is not enough to distinguish them across sagas. Furthermore,
    /// instance ids allow the parent saga to identify data generated from
    /// different subsagas.
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
    /// [`DagBuilder::append`].
    pub fn append_parallel(&mut self, nodes: Vec<Node>) {
        let newnodes: Vec<NodeIndex> =
            nodes.into_iter().map(|node| self.graph.add_node(node)).collect();

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

    /// Append another DAG to this one (insert a subsaga)
    // TODO: We probably want an `append_parallel_subsaga` that allows us to
    // link all subsagas to the `last_added` nodes in a parallel fashion.
    pub fn append_subsaga(
        &mut self,
        name: &str,
        subsaga: &Dag,
        params_node_name: &str,
    ) {
        self.append(Node::SubsagaStart {
            saga_name: subsaga.name.clone(),
            params_node_name: NodeName::new(params_node_name),
        });

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
                Node::SubsagaEnd { name: NodeName::new(name) }
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
        self.last_added = vec![*end_parent_node_index];
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
                        builder.append(Node::new_child(
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
                            Node::new_child(spec.name, spec.label, spec.action)
                        })
                        .collect();
                    builder.append_parallel(nodes);
                }
            }
        }

        builder.build()
    }
}
