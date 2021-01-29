//! Facilities for constructing saga graphs

use crate::rust_features::ExpectNone;
use crate::saga_action::SagaAction;
use crate::saga_action::SagaActionEndNode;
use crate::saga_action::SagaActionStartNode;
use anyhow::anyhow;
use petgraph::dot;
use petgraph::graph::NodeIndex;
use petgraph::Directed;
use petgraph::Graph;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

/** Unique identifier for a Saga Template */
/*
 * TODO-design We may want the display form to have a "st-" prefix or something
 * like that.  (Does that mean the type needs to be caller-provided?)
 */
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct SagaTemplateId(pub Uuid);
impl fmt::Display for SagaTemplateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sgt-{}", self.0)
    }
}

/** Unique identifier for a Saga (an execution of a saga template) */
/* TODO-design Same as for Saga Template */
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct SagaId(pub Uuid);
impl fmt::Display for SagaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sg-{}", self.0)
    }
}

/**
 * A directed acyclic graph (DAG) where each node implements `SagaAction`
 *
 * With each node, there's an execution action and an undo action.  Execution of
 * the saga guarantees that eventually all saga nodes will complete successfully
 * or else that any nodes whose actions may have run have also had their undo
 * action run.
 *
 * You define a saga template using [`SagaTemplateBuilder`].  You can execute a
 * saga as many times as you want using [`crate::SagaExecutor`].
 */
#[derive(Debug)]
pub struct SagaTemplate {
    /** describes the nodes in the graph and their dependencies */
    pub(crate) graph: Graph<String, ()>,
    /** action associated with each node in the graph */
    pub(crate) launchers: BTreeMap<NodeIndex, Arc<dyn SagaAction>>,
    /** name associated with each node in the graph */
    pub(crate) node_names: BTreeMap<NodeIndex, String>,
    /** human-readable labels associated with each node in the graph */
    pub(crate) node_labels: BTreeMap<NodeIndex, String>,
    /** start node */
    pub(crate) start_node: NodeIndex,
    /** end node */
    pub(crate) end_node: NodeIndex,
}

impl SagaTemplate {
    /*
     * TODO-cleanup we may want to use a newtype for NodeIndex here.  It's
     * sketchy that this is exposed publicly but there's no way for callers to
     * write down this type.
     */
    pub fn node_for_name(
        &self,
        target_name: &str,
    ) -> Result<NodeIndex, anyhow::Error> {
        for (node, name) in &self.node_names {
            if name == target_name {
                return Ok(*node);
            }
        }

        /* TODO-debug saga templates should have names, too */
        Err(anyhow!("saga template has no node named \"{}\"", target_name))
    }

    /**
     * Returns an object that can be used to print a graphiz-format
     * representation of the underlying node graph.
     */
    pub fn dot<'a>(&'a self) -> SagaTemplateDot<'a> {
        SagaTemplateDot(&self.graph)
    }
}

/**
 * Graphviz-formatted view of a saga graph
 *
 * Use the `Display` impl to print a representation suitable as input to the
 * `dot` command.  You could put this into a file `graph.out` and run something
 * like `dot -Tpng -o graph.png graph.out` to produce `graph.png`, a visual
 * representation of the saga graph.
 */
pub struct SagaTemplateDot<'a>(&'a Graph<String, (), Directed, u32>);
impl<'a> fmt::Display for SagaTemplateDot<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config = &[dot::Config::EdgeNoLabel];
        let dot = dot::Dot::with_config(&self.0, config);
        write!(f, "{:?}", dot)
    }
}

/**
 * Builder for constructing a SagaTemplate
 *
 * The interface here only supports linear construction using an "append"
 * operation.  See [`SagaTemplateBuilder::append`] and
 * [`SagaTemplateBuilder::append_parallel`].
 */
#[derive(Debug)]
pub struct SagaTemplateBuilder {
    /** DAG of saga nodes.  Weights for nodes are debug labels. */
    graph: Graph<String, ()>,
    /** For each node, the [`SagaAction`] executed at that node. */
    launchers: BTreeMap<NodeIndex, Arc<dyn SagaAction>>,
    /**
     * For each node, the name of the node.  This is used for data stored by
     * that node.
     */
    node_names: BTreeMap<NodeIndex, String>,
    /** For each node, a human-readable label for the node. */
    node_labels: BTreeMap<NodeIndex, String>,
    /** Root node of the graph */
    root: NodeIndex,
    /** Last set of nodes added.  This is used when appending to the graph. */
    last_added: Vec<NodeIndex>,
}

impl SagaTemplateBuilder {
    pub fn new() -> SagaTemplateBuilder {
        let mut graph = Graph::new();
        let mut launchers = BTreeMap::new();
        let node_names = BTreeMap::new();
        let node_labels = BTreeMap::new();
        let first: Arc<dyn SagaAction + 'static> =
            Arc::new(SagaActionStartNode {});
        let label = format!("{:?}", first);
        let root = graph.add_node(label);
        launchers.insert(root, first).expect_none("empty map had an element");

        SagaTemplateBuilder {
            graph,
            launchers,
            root,
            node_names,
            node_labels,
            last_added: vec![root],
        }
    }

    /**
     * Adds a new node to the graph
     *
     * The new node will depend on completion of all actions that were added in
     * the last call to `append` or `append_parallel`.  (The idea is to `append`
     * a sequence of steps that run one after another.)
     *
     * `action` will be used when this node is being executed.
     *
     * The node is called `name`.  This name is used for storing the output of
     * the action so that descendant nodes can access it using
     * [`crate::SagaContext::lookup`].
     */
    pub fn append(
        &mut self,
        name: &str,
        label: &str,
        action: Arc<dyn SagaAction>,
    ) {
        let newnode = self.graph.add_node(label.to_string());
        self.launchers
            .insert(newnode, action)
            .expect_none("action already present for newly created node");
        /* TODO-correctness this doesn't check name uniqueness! */
        self.node_names
            .insert(newnode, name.to_string())
            .expect_none("name already used in this saga template");
        self.node_labels
            .insert(newnode, label.to_string())
            .expect_none("labels already used in this saga template");
        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        self.last_added = vec![newnode];
    }

    /**
     * Adds a set of nodes to the graph that can be executed concurrently
     *
     * The new nodes will individually depend on completion of all actions that
     * were added in the last call to `append` or `append_parallel`.  `actions`
     * is a vector of `(name, action)` tuples analogous to the arguments to
     * [`SagaTemplateBuilder::append`].
     */
    pub fn append_parallel(
        &mut self,
        actions: Vec<(&str, &str, Arc<dyn SagaAction>)>,
    ) {
        let newnodes: Vec<NodeIndex> = actions
            .into_iter()
            .map(|(n, l, a)| {
                let node = self.graph.add_node(l.to_string());
                self.launchers.insert(node, a).expect_none(
                    "action already present for newly created node",
                );
                /* TODO-correctness does not validate the name! */
                self.node_names
                    .insert(node, n.to_string())
                    .expect_none("name already used in this saga template");
                self.node_labels
                    .insert(node, l.to_string())
                    .expect_none("node already has a label");
                node
            })
            .collect();

        /*
         * TODO-design For this exploration, we assume that any nodes appended
         * after a parallel set are intended to depend on _all_ nodes in the
         * parallel set.  This doesn't have to be the case in general, but if
         * you wanted to do something else, you probably would need pretty
         * fine-grained control over the construction of the graph.  This is
         * mostly a question of how to express the construction of the graph,
         * not the graph itself nor how it gets processed, so let's defer for
         * now.
         *
         * Even given that, it might make more sense to implement this by
         * creating an intermediate node that all the parallel nodes have edges
         * to, and then edges from this intermediate node to the next set of
         * parallel nodes.
         */
        for node in &self.last_added {
            for newnode in &newnodes {
                self.graph.add_edge(*node, *newnode, ());
            }
        }

        self.last_added = newnodes;
    }

    /** Finishes building the saga template */
    pub fn build(mut self) -> SagaTemplate {
        /*
         * Append an "end" node so that we can easily tell when the saga has
         * completed.
         */
        let last: Arc<dyn SagaAction + 'static> =
            Arc::new(SagaActionEndNode {});
        let label = format!("{:?}", last);
        let newnode = self.graph.add_node(label);
        /*
         * It seems sketchy to have assertions with side effects.  We'd prefer
         * `unwrap_none()`, but that's still experimental.
         */
        assert!(self.launchers.insert(newnode, last).is_none());

        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        SagaTemplate {
            graph: self.graph,
            launchers: self.launchers,
            node_names: self.node_names,
            node_labels: self.node_labels,
            start_node: self.root,
            end_node: newnode,
        }
    }
}
