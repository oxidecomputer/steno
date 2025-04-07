//! Manages execution of a saga

// Work around proptest-rs/proptest#364.
#![allow(clippy::arc_with_non_send_sync)]

use crate::dag::InternalNode;
use crate::dag::NodeName;
use crate::rust_features::ExpectNone;
use crate::saga_action_error::ActionError;
use crate::saga_action_error::UndoActionError;
use crate::saga_action_generic::Action;
use crate::saga_action_generic::ActionConstant;
use crate::saga_action_generic::ActionData;
use crate::saga_action_generic::ActionInjectError;
use crate::saga_log::SagaNodeEventType;
use crate::saga_log::SagaNodeLoadStatus;
use crate::sec::RepeatInjected;
use crate::sec::SecExecClient;
use crate::ActionRegistry;
use crate::SagaCachedState;
use crate::SagaDag;
use crate::SagaId;
use crate::SagaLog;
use crate::SagaNodeEvent;
use crate::SagaNodeId;
use crate::SagaType;
use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use petgraph::algo::toposort;
use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

// TODO-design Should we go even further and say that each node is its own
// struct with incoming channels from parents (to notify when done), from
// children (to notify when undone), and to each direction as well?  Then the
// whole thing is a message passing exercise?
struct SgnsDone(Arc<serde_json::Value>);
struct SgnsFailed(ActionError);
struct SgnsUndone(UndoMode);
struct SgnsUndoFailed(UndoActionError);

struct SagaNode<S: SagaNodeStateType> {
    node_id: NodeIndex,
    state: S,
}

trait SagaNodeStateType {}
impl SagaNodeStateType for SgnsDone {}
impl SagaNodeStateType for SgnsFailed {}
impl SagaNodeStateType for SgnsUndone {}
impl SagaNodeStateType for SgnsUndoFailed {}

// TODO-design Is this right?  Is the trait supposed to be empty?
trait SagaNodeRest<UserType: SagaType>: Send + Sync {
    fn propagate(
        &self,
        exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState,
    );
    fn log_event(&self) -> SagaNodeEventType;
}

impl<UserType: SagaType> SagaNodeRest<UserType> for SagaNode<SgnsDone> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::Succeeded(Arc::clone(&self.state.0))
    }

    fn propagate(
        &self,
        exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.dag.graph;
        assert!(!live_state.node_errors.contains_key(&self.node_id));
        live_state
            .node_outputs
            .insert(self.node_id, Arc::clone(&self.state.0))
            .expect_none("node finished twice (storing output)");

        if self.node_id == exec.dag.end_node {
            // If we've completed the last node, the saga is done.
            assert!(!live_state.stopping);
            assert_eq!(live_state.exec_state, SagaCachedState::Running);
            assert_eq!(graph.node_count(), live_state.node_outputs.len());
            live_state.mark_saga_done();
            return;
        }

        // If we're stopping, don't kick off anything else.
        if live_state.stopping {
            return;
        }

        if live_state.exec_state == SagaCachedState::Unwinding {
            // If the saga is currently unwinding, then this node finishing
            // doesn't unblock any other nodes.  However, it potentially
            // unblocks undoing itself.  We'll only proceed if all of our child
            // nodes are "undone" already.
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_undone.contains_key(child)
            }) {
                live_state.queue_undo.push(self.node_id);
            }
            return;
        }

        // Under normal execution, this node's completion means it's time to
        // check dependent nodes to see if they're now runnable.
        for child in graph.neighbors_directed(self.node_id, Outgoing) {
            if neighbors_all(graph, &child, Incoming, |parent| {
                live_state.node_outputs.contains_key(parent)
            }) {
                live_state.queue_todo.push(child);
            }
        }
    }
}

impl<UserType: SagaType> SagaNodeRest<UserType> for SagaNode<SgnsFailed> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::Failed(self.state.0.clone())
    }

    fn propagate(
        &self,
        exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.dag.graph;
        assert!(!live_state.node_outputs.contains_key(&self.node_id));
        live_state
            .node_errors
            .insert(self.node_id, self.state.0.clone())
            .expect_none("node finished twice (storing error)");

        // If we're stopping, don't kick off anything else.
        if live_state.stopping {
            return;
        }

        if live_state.exec_state == SagaCachedState::Unwinding {
            // This node failed while we're already unwinding.  We don't
            // need to kick off unwinding again.  We could in theory
            // immediately move this node to "undone" and unblock its
            // dependents, but for consistency with a simpler algorithm,
            // we'll wait for unwinding to propagate from the end node.
            // If all of our children are already undone, however, we
            // must go ahead and mark ourselves undone and propagate
            // that.
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_undone.contains_key(child)
            }) {
                let new_node = SagaNode {
                    node_id: self.node_id,
                    state: SgnsUndone(UndoMode::ActionFailed),
                };
                new_node.propagate(exec, live_state);
            }
        } else {
            // Begin the unwinding process.  Start with the end node: mark
            // it trivially "undone" and propagate that.
            live_state.exec_state = SagaCachedState::Unwinding;
            assert_ne!(self.node_id, exec.dag.end_node);
            let new_node = SagaNode {
                node_id: exec.dag.end_node,
                state: SgnsUndone(UndoMode::ActionNeverRan),
            };
            new_node.propagate(exec, live_state);
        }
    }
}

impl<UserType: SagaType> SagaNodeRest<UserType> for SagaNode<SgnsUndone> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::UndoFinished
    }

    fn propagate(
        &self,
        exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.dag.graph;
        live_state
            .nodes_undone
            .insert(self.node_id, self.state.0)
            .expect_none("node already undone");

        if self.node_id == exec.dag.start_node {
            // If we've undone the start node, the saga is done.
            assert!(!live_state.stopping);
            live_state.mark_saga_done();
            return;
        }

        // If we're stopping, don't kick off anything else.
        if live_state.stopping {
            return;
        }

        assert_eq!(live_state.exec_state, SagaCachedState::Unwinding);

        // During unwinding, a node's becoming undone means it's time to check
        // ancestor nodes to see if they're now undoable.
        for parent in graph.neighbors_directed(self.node_id, Incoming) {
            if neighbors_all(graph, &parent, Outgoing, |child| {
                live_state.nodes_undone.contains_key(child)
            }) {
                // We're ready to undo "parent".  We don't know whether it's
                // finished running, on the todo queue, or currenting
                // outstanding.  (It should not be on the undo queue!)
                // TODO-design Here's an awful approach just intended to let us
                // flesh out more of the rest of this to better understand how
                // to manage state.
                match live_state.node_exec_state(parent) {
                    // If the node never started or if it failed, we can
                    // just mark it undone without doing anything else.
                    NodeExecState::Blocked => {
                        let new_node = SagaNode {
                            node_id: parent,
                            state: SgnsUndone(UndoMode::ActionNeverRan),
                        };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    NodeExecState::Failed => {
                        let new_node = SagaNode {
                            node_id: parent,
                            state: SgnsUndone(UndoMode::ActionFailed),
                        };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    NodeExecState::QueuedToRun
                    | NodeExecState::TaskInProgress => {
                        // If we're running an action for this task, there's
                        // nothing we can do right now, but we'll handle it when
                        // it finishes.  We could do better with queued (and
                        // there's a TODO-design in kick_off_ready() to do so),
                        // but this isn't wrong as-is.
                        continue;
                    }

                    NodeExecState::Done => {
                        // We have to actually run the undo action.
                        live_state.queue_undo.push(parent);
                    }

                    NodeExecState::QueuedToUndo
                    | NodeExecState::UndoInProgress
                    | NodeExecState::Undone(_)
                    | NodeExecState::UndoFailed => {
                        panic!(
                            "already undoing or undone node whose child was \
                             just now undone"
                        );
                    }
                }
            }
        }
    }
}

impl<UserType: SagaType> SagaNodeRest<UserType> for SagaNode<SgnsUndoFailed> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::UndoFailed(self.state.0.clone())
    }

    fn propagate(
        &self,
        _exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState,
    ) {
        assert!(live_state.exec_state == SagaCachedState::Unwinding);
        live_state
            .undo_errors
            .insert(self.node_id, self.state.0.clone())
            .expect_none("undo node failed twice (storing error)");

        // Once any node's undo action has failed, the saga becomes stuck.
        live_state.saga_stuck();
    }
}

/// Message sent from (tokio) task that executes an action to the executor
/// indicating that the action has completed
struct TaskCompletion<UserType: SagaType> {
    // TODO-cleanup can this be removed? The node field is a SagaNode, which
    // has a node_id.
    node_id: NodeIndex,
    node: Box<dyn SagaNodeRest<UserType>>,
}

/// Context provided to the (tokio) task that executes an action
struct TaskParams<UserType: SagaType> {
    dag: Arc<SagaDag>,
    user_context: Arc<UserType::ExecContextType>,

    /// Handle to the saga's live state
    ///
    /// This is used only to update state for status purposes.  We want to
    /// avoid any tight coupling between this task and the internal state.
    live_state: Arc<Mutex<SagaExecLiveState>>,

    /// id of the graph node whose action we're running
    node_id: NodeIndex,
    /// channel over which to send completion message
    done_tx: mpsc::Sender<TaskCompletion<UserType>>,
    /// Ancestor tree for this node.  See [`ActionContext`].
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<NodeName, Arc<serde_json::Value>>>,
    /// Saga parameters for the closest enclosing saga
    saga_params: Arc<serde_json::Value>,
    /// The action itself that we're executing.
    action: Arc<dyn Action<UserType>>,
    /// If true, indicates that the action should be executed multiple
    /// times, and the latter result should be used. This is useful
    /// when testing idempotency of a user-specified action.
    injected_repeat: Option<RepeatInjected>,
}

/// Executes a saga
///
/// Call `SagaExecutor.run()` to get a Future.  You must `await` this Future to
/// actually execute the saga.
// TODO Lots more could be said here, but the basic idea matches distributed
// sagas.
// This will be a good place to put things like concurrency limits, canarying,
// etc.
//
// TODO Design note: SagaExecutor's constructors consume Arc<E> and store Arc<E>
// to reference the user-provided context "E".  This makes it easy for us to
// pass references to the various places that need it.  It would seem nice if
// the constructor accepted "E" and stored that, since "E" is already Send +
// Sync + 'static.  There are two challenges here: (1) There are a bunch of
// other types that store a reference to E, including TaskParams and
// ActionContext, the latter of which is exposed to the user.  These would have
// to store &E, which would be okay, but they'd need to have annoying lifetime
// parameters.  (2) child sagas (and so child saga executors) are a thing.
// Since there's only one "E", the child would have to reference &E, which means
// it would need a lifetime parameter on it _and_ that might mean it would have
// to be a different type than SagaExecutor, even though they're otherwise the
// same.
#[derive(Debug)]
pub struct SagaExecutor<UserType: SagaType> {
    #[allow(dead_code)]
    log: slog::Logger,

    dag: Arc<SagaDag>,
    action_registry: Arc<ActionRegistry<UserType>>,

    /// Channel for monitoring execution completion
    finish_tx: broadcast::Sender<()>,

    /// Unique identifier for this saga (an execution of a saga template)
    saga_id: SagaId,

    /// For each node, the NodeIndex of the start of its saga or subsaga
    node_saga_start: BTreeMap<NodeIndex, NodeIndex>,

    live_state: Arc<Mutex<SagaExecLiveState>>,
    user_context: Arc<UserType::ExecContextType>,
}

#[derive(Debug)]
enum RecoveryDirection {
    Forward(bool),
    Unwind(bool),
}

impl<UserType: SagaType> SagaExecutor<UserType> {
    /// Create an executor to run the given saga.
    pub fn new(
        log: slog::Logger,
        saga_id: SagaId,
        dag: Arc<SagaDag>,
        registry: Arc<ActionRegistry<UserType>>,
        user_context: Arc<UserType::ExecContextType>,
        sec_hdl: SecExecClient,
    ) -> Result<SagaExecutor<UserType>, anyhow::Error> {
        let sglog = SagaLog::new_empty(saga_id);
        SagaExecutor::new_recover(
            log,
            saga_id,
            dag,
            registry,
            user_context,
            sec_hdl,
            sglog,
        )
    }

    /// Create an executor to run the given saga that may have already
    /// started, using the given log events.
    pub fn new_recover(
        log: slog::Logger,
        saga_id: SagaId,
        dag: Arc<SagaDag>,
        registry: Arc<ActionRegistry<UserType>>,
        user_context: Arc<UserType::ExecContextType>,
        sec_hdl: SecExecClient,
        sglog: SagaLog,
    ) -> Result<SagaExecutor<UserType>, anyhow::Error> {
        // Before anything else, do some basic checks on the DAG.
        Self::validate_saga(&dag, &registry).with_context(|| {
            format!("validating saga {:?}", dag.saga_name())
        })?;

        // During recovery, there's a fine line between operational errors and
        // programmer errors.  If we discover semantically invalid saga state,
        // that's an operational error that we must handle gracefully.  We use
        // lots of assertions to check invariants about our own process for
        // loading the state.  We panic if those are violated.  For example, if
        // we find that we've loaded the same node twice, that's a bug in this
        // code right here (which walks each node of the graph exactly once),
        // not a result of corrupted database state.
        let forward = !sglog.unwinding();
        let mut live_state = SagaExecLiveState {
            stopping: false,
            exec_state: if forward {
                SagaCachedState::Running
            } else {
                SagaCachedState::Unwinding
            },
            queue_todo: Vec::new(),
            queue_undo: Vec::new(),
            node_tasks: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            nodes_undone: BTreeMap::new(),
            node_errors: BTreeMap::new(),
            undo_errors: BTreeMap::new(),
            sglog,
            injected_errors: BTreeSet::new(),
            injected_undo_errors: BTreeSet::new(),
            injected_repeats: BTreeMap::new(),
            sec_hdl,
            saga_id,
        };
        let mut loaded = BTreeSet::new();
        let graph = &dag.graph;

        // Precompute a mapping from each node to the start of its containing
        // saga or subsaga.  This is used for quickly finding each node's saga
        // parameters and also when building ancestor trees for skipping over
        // entire subsagas.
        let nodes_sorted = toposort(&graph, None).expect("saga DAG had cycles");
        let node_saga_start = {
            let mut node_saga_start = BTreeMap::new();
            for node_index in &nodes_sorted {
                let node = graph.node_weight(*node_index).unwrap();
                let subsaga_start_index = match node {
                    InternalNode::Start { .. }
                    | InternalNode::SubsagaStart { .. } => {
                        // For a top-level start node or subsaga start node, the
                        // containing saga start node is itself.
                        *node_index
                    }
                    InternalNode::End
                    | InternalNode::Action { .. }
                    | InternalNode::Constant { .. }
                    | InternalNode::SubsagaEnd { .. } => {
                        // For every other kind of node, first, there must be at
                        // least one ancestor.  And we must have already visited
                        // because we're iterating in topological order.  In
                        // most cases, this node's saga starts at the same node
                        // as its ancestor.  However, if the ancestor is a
                        // SubsagaEnd, then we need to skip over the whole
                        // subsaga.
                        let immed_ancestor = graph
                            .neighbors_directed(*node_index, petgraph::Incoming)
                            .next()
                            .unwrap();
                        let immed_ancestor_node =
                            dag.get(immed_ancestor).unwrap();
                        let ancestor = match immed_ancestor_node {
                            InternalNode::SubsagaEnd { .. } => {
                                let subsaga_start = *node_saga_start
                                    .get(&immed_ancestor)
                                    .unwrap();
                                graph
                                    .neighbors_directed(
                                        subsaga_start,
                                        petgraph::Incoming,
                                    )
                                    .next()
                                    .unwrap()
                            }
                            _ => immed_ancestor,
                        };

                        *node_saga_start.get(&ancestor).expect(
                            "expected to compute ancestor's subsaga start \
                             node first",
                        )
                    }
                };
                node_saga_start.insert(*node_index, subsaga_start_index);
            }
            node_saga_start
        };

        // Iterate in the direction of current execution: for normal execution,
        // a standard topological sort.  For unwinding, reverse that.
        let graph_nodes = {
            let mut nodes = nodes_sorted;
            if !forward {
                nodes.reverse();
            }
            nodes
        };

        for node_id in graph_nodes {
            let node_status =
                live_state.sglog.load_status_for_node(node_id.into());

            // Validate this node's state against its parent nodes' states.  By
            // induction, this validates everything in the graph from the start
            // or end node to the current node.
            for parent in graph.neighbors_directed(node_id, Incoming) {
                let parent_status =
                    live_state.sglog.load_status_for_node(parent.into());
                if !recovery_validate_parent(parent_status, node_status) {
                    return Err(anyhow!(
                        "recovery for saga {}: node {:?}: load status is \
                         \"{:?}\", which is illegal for parent load status \
                         \"{:?}\"",
                        saga_id,
                        node_id,
                        node_status,
                        parent_status,
                    ));
                }
            }

            let direction = if forward {
                RecoveryDirection::Forward(neighbors_all(
                    graph,
                    &node_id,
                    Incoming,
                    |p| {
                        assert!(loaded.contains(p));
                        live_state.node_outputs.contains_key(p)
                    },
                ))
            } else {
                RecoveryDirection::Unwind(neighbors_all(
                    graph,
                    &node_id,
                    Outgoing,
                    |c| {
                        assert!(loaded.contains(c));
                        live_state.nodes_undone.contains_key(c)
                    },
                ))
            };

            match node_status {
                SagaNodeLoadStatus::NeverStarted => {
                    match direction {
                        RecoveryDirection::Forward(true) => {
                            // We're recovering a node in the forward direction
                            // where all parents completed successfully.  Add it
                            // to the ready queue.
                            live_state.queue_todo.push(node_id);
                        }
                        RecoveryDirection::Unwind(true) => {
                            // We're recovering a node in the reverse direction
                            // (unwinding) whose children have all been
                            // undone and which has never started.  Just mark
                            // it undone.
                            // TODO-design Does this suggest a better way to do
                            // this might be to simply load all the state that
                            // we have into the SagaExecLiveState and execute
                            // the saga as normal, but have normal execution
                            // check for cached values instead of running
                            // actions?  In a sense, this makes the recovery
                            // path look like the normal path rather than having
                            // the normal path look like the recovery path.  On
                            // the other hand, it seems kind of nasty to have to
                            // hold onto the recovery state for the duration.
                            // It doesn't make it a whole lot easier to test or
                            // have fewer code paths, in a real sense.  It moves
                            // those code paths to normal execution, but they're
                            // still bifurcated from the case where we didn't
                            // recover the saga.
                            live_state
                                .nodes_undone
                                .insert(node_id, UndoMode::ActionNeverRan);
                        }
                        _ => (),
                    }
                }
                SagaNodeLoadStatus::Started => {
                    // Whether we're unwinding or not, we have to finish
                    // execution of this action.
                    live_state.queue_todo.push(node_id);
                }
                SagaNodeLoadStatus::Succeeded(output) => {
                    // If the node has finished executing and not started
                    // undoing, and if we're unwinding and the children have
                    // all finished undoing, then it's time to undo this
                    // one.
                    assert!(!live_state.node_errors.contains_key(&node_id));
                    live_state
                        .node_outputs
                        .insert(node_id, Arc::clone(output))
                        .expect_none("recovered node twice (success case)");
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state.queue_undo.push(node_id);
                    }
                }
                SagaNodeLoadStatus::Failed(error) => {
                    assert!(!live_state.node_outputs.contains_key(&node_id));
                    live_state
                        .node_errors
                        .insert(node_id, error.clone())
                        .expect_none("recovered node twice (failure case)");

                    // If the node failed, and we're unwinding, and the children
                    // have all been undone, it's time to undo this one.
                    // But we just mark it undone -- we don't execute the
                    // undo action.
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state
                            .nodes_undone
                            .insert(node_id, UndoMode::ActionFailed);
                    }
                }
                SagaNodeLoadStatus::UndoStarted(output) => {
                    // We know we're unwinding. (Otherwise, we should have
                    // failed validation earlier.)  Execute the undo action.
                    assert!(!forward);
                    live_state.queue_undo.push(node_id);

                    // We still need to record the output because it's available
                    // to the undo action.
                    live_state
                        .node_outputs
                        .insert(node_id, Arc::clone(output))
                        .expect_none("recovered node twice (undo case)");
                }
                SagaNodeLoadStatus::UndoFinished => {
                    // Again, we know we're unwinding.  We've also finished
                    // undoing this node.
                    assert!(!forward);
                    live_state
                        .nodes_undone
                        .insert(node_id, UndoMode::ActionUndone);
                }
                SagaNodeLoadStatus::UndoFailed(error) => {
                    assert!(!forward);
                    live_state
                        .undo_errors
                        .insert(node_id, error.clone())
                        .expect_none(
                            "recovered node twice (undo failure case)",
                        );
                    live_state.saga_stuck();
                }
            }

            // TODO-correctness is it appropriate to have side effects in an
            // assertion here?
            assert!(loaded.insert(node_id));
        }

        // Check our done conditions.
        if live_state.node_outputs.contains_key(&dag.end_node)
            || live_state.nodes_undone.contains_key(&dag.start_node)
        {
            live_state.mark_saga_done();
        }

        let (finish_tx, _) = broadcast::channel(1);

        Ok(SagaExecutor {
            log,
            dag,
            finish_tx,
            saga_id,
            user_context,
            live_state: Arc::new(Mutex::new(live_state)),
            action_registry: Arc::clone(&registry),
            node_saga_start,
        })
    }

    /// Validates some basic properties of the saga
    // Many of these properties may be validated when we construct the saga DAG.
    // Checking them again here makes sure that we gracefully handle a case
    // where we got an invalid DAG in some other way (e.g., bad database state).
    // See `DagBuilderError` for more information about these conditions.
    fn validate_saga(
        saga: &SagaDag,
        registry: &ActionRegistry<UserType>,
    ) -> Result<(), anyhow::Error> {
        let mut nsubsaga_start = 0;
        let mut nsubsaga_end = 0;

        for node_index in saga.graph.node_indices() {
            let node = &saga.graph[node_index];
            match node {
                InternalNode::Start { .. } => {
                    ensure!(
                        node_index == saga.start_node,
                        "found start node at unexpected index {:?}",
                        node_index
                    );
                }
                InternalNode::End => {
                    ensure!(
                        node_index == saga.end_node,
                        "found end node at unexpected index {:?}",
                        node_index
                    );
                }
                InternalNode::Action { name, action_name, .. } => {
                    let action = registry.get(&action_name);
                    ensure!(
                        action.is_ok(),
                        "action for node {:?} not registered: {:?}",
                        name,
                        action_name
                    );
                }
                InternalNode::Constant { .. } => (),
                InternalNode::SubsagaStart { .. } => {
                    nsubsaga_start += 1;
                }
                InternalNode::SubsagaEnd { .. } => {
                    nsubsaga_end += 1;
                }
            }
        }

        ensure!(
            saga.start_node.index() < saga.graph.node_count(),
            "bad saga graph (missing start node)",
        );
        ensure!(
            saga.end_node.index() < saga.graph.node_count(),
            "bad saga graph (missing end node)",
        );
        ensure!(
            nsubsaga_start == nsubsaga_end,
            "bad saga graph (found {} subsaga start nodes but {} subsaga end \
             nodes)",
            nsubsaga_start,
            nsubsaga_end
        );

        let nend_ancestors =
            saga.graph.neighbors_directed(saga.end_node, Incoming).count();
        ensure!(
            nend_ancestors == 1,
            "expected saga to end with exactly one node"
        );

        Ok(())
    }

    /// Builds the "ancestor tree" for a node whose dependencies have all
    /// completed
    ///
    /// The ancestor tree for a node is a map whose keys are strings that
    /// identify ancestor nodes in the graph and whose values represent the
    /// outputs from those nodes.  This is used by [`ActionContext::lookup`].
    /// See where we use this function in poll() for more details.
    fn make_ancestor_tree(
        &self,
        tree: &mut BTreeMap<NodeName, Arc<serde_json::Value>>,
        live_state: &SagaExecLiveState,
        node_index: NodeIndex,
        include_self: bool,
    ) {
        if include_self {
            self.make_ancestor_tree_node(tree, live_state, node_index);
            return;
        }

        let ancestors = self.dag.graph.neighbors_directed(node_index, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, live_state, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<NodeName, Arc<serde_json::Value>>,
        live_state: &SagaExecLiveState,
        node_index: NodeIndex,
    ) {
        let dag_node = self.dag.get(node_index).unwrap();

        // Record any output from the current node.
        match dag_node {
            InternalNode::Constant { name, .. }
            | InternalNode::Action { name, .. }
            | InternalNode::SubsagaEnd { name, .. } => {
                // TODO-cleanup This implementation may encounter the same node
                // twice.  This feels a little sloppy and inefficient.
                let output = live_state.node_output(node_index);
                tree.insert(name.clone(), output);
            }
            InternalNode::Start { .. }
            | InternalNode::End
            | InternalNode::SubsagaStart { .. } => (),
        }

        // Figure out where to resume the traversal.
        let resume_node = match dag_node {
            InternalNode::SubsagaStart { .. } => {
                // We were traversing nodes in a subsaga and we're now done.
                None
            }
            InternalNode::SubsagaEnd { .. } => {
                // We're traversing nodes in a saga that contains a subsaga.
                // Skip over the subsaga.
                Some(*self.node_saga_start.get(&node_index).unwrap())
            }
            InternalNode::Constant { .. }
            | InternalNode::Action { .. }
            | InternalNode::Start { .. }
            | InternalNode::End => {
                // Ordinary traversal -- keep going from where we are.
                Some(node_index)
            }
        };

        if let Some(resume_node) = resume_node {
            self.make_ancestor_tree(tree, live_state, resume_node, false);
        }
    }

    /// Returns the saga parameters for the current node
    // If this node is not within a subsaga, then these will be the top-level
    // saga parameters.  If this node is contained within a subsaga, then these
    // will be the parameters of the _subsaga_.
    fn saga_params_for(
        &self,
        live_state: &SagaExecLiveState,
        node_index: NodeIndex,
    ) -> Arc<serde_json::Value> {
        let subsaga_start_index = self.node_saga_start[&node_index];
        let subsaga_start_node = self.dag.get(subsaga_start_index).unwrap();
        match subsaga_start_node {
            InternalNode::Start { params } => params.clone(),
            InternalNode::SubsagaStart { params_node_name, .. } => {
                // TODO-performance We're going to repeat this for every node in
                // the subsaga.  We may as well cache it somewhere.  The tricky
                // part is figuring out when to generate the value that accounts
                // for all the cases where we may land here, including recovery
                // (in which case we may not have executed the SubsagaStart
                // node since crashing) and for undo actions in the subsaga.
                let mut tree = BTreeMap::new();
                self.make_ancestor_tree(
                    &mut tree,
                    live_state,
                    subsaga_start_index,
                    false,
                );
                Arc::clone(tree.get(params_node_name).unwrap())
            }
            InternalNode::SubsagaEnd { .. }
            | InternalNode::End
            | InternalNode::Action { .. }
            | InternalNode::Constant { .. } => {
                panic!(
                    "containing saga cannot have started with {:?}",
                    subsaga_start_node
                );
            }
        }
    }

    /// Simulates an error at a given node in the saga graph
    ///
    /// When execution reaches this node, instead of running the normal action
    /// for this node, an error will be generated and processed as though the
    /// action itself had produced the error.
    pub async fn inject_error(&self, node_id: NodeIndex) {
        let mut live_state = self.live_state.lock().await;
        live_state.injected_errors.insert(node_id);
    }

    /// Simulates an error in the undo action at a given node in the saga graph
    ///
    /// When unwinding reaches this node, instead of running the normal undo
    /// action for this node, an error will be generated and processed as though
    /// the undo action itself had produced the error.
    pub async fn inject_error_undo(&self, node_id: NodeIndex) {
        let mut live_state = self.live_state.lock().await;
        live_state.injected_undo_errors.insert(node_id);
    }

    /// Forces a given node to be executed twice
    ///
    /// When execution reaches this node, the action and undo actions
    /// are invoked twice by the saga executor.
    ///
    /// If this node produces output, only the second value is stored.
    pub async fn inject_repeat(
        &self,
        node_id: NodeIndex,
        repeat: RepeatInjected,
    ) {
        let mut live_state = self.live_state.lock().await;
        live_state.injected_repeats.insert(node_id, repeat);
    }

    /// Runs the saga
    ///
    /// This might be running a saga that has never been started before or
    /// one that has been recovered from persistent state.
    async fn run_saga(&self) {
        {
            // TODO-design Every SagaExec should be able to run_saga() exactly
            // once.  We don't really want to let you re-run it and get a new
            // message on finish_tx.  However, we _do_ want to handle this
            // particular case when we've recovered a "done" saga and the
            // consumer has run() it (once).
            let live_state = self.live_state.lock().await;
            if live_state.exec_state == SagaCachedState::Done {
                self.finish_tx.send(()).expect("failed to send finish message");
                live_state.sec_hdl.saga_update(live_state.exec_state).await;
                return;
            }
        }

        // Allocate the channel used for node tasks to tell us when they've
        // completed.  In practice, each node can enqueue only two messages in
        // its lifetime: one for completion of the action, and one for
        // completion of the compensating action.  We bound this channel's size
        // at twice the graph node count for this worst case.
        let (tx, mut rx) = mpsc::channel(2 * self.dag.graph.node_count());

        loop {
            self.kick_off_ready(&tx).await;

            // Process any messages available on our channel.
            // It shouldn't be possible to get None back here.  That would mean
            // that all of the consumers have closed their ends, but we still
            // have a consumer of our own in "tx".
            // TODO-robustness Can we assert that there are outstanding tasks
            // when we block on this channel?
            let message = rx.next().await.expect("broken tx");
            let task = {
                let mut live_state = self.live_state.lock().await;
                live_state.node_task_done(message.node_id)
            };

            // This should really not take long, as there's nothing else this
            // task does after sending the message that we just received.  It's
            // good to wait here to make sure things are cleaned up.
            // TODO-robustness can we enforce that this won't take long?
            task.await.expect("node task failed unexpectedly");

            let mut live_state = self.live_state.lock().await;
            let prev_state = live_state.exec_state;
            message.node.propagate(&self, &mut live_state);
            // TODO-cleanup This condition ought to be simplified.  We want to
            // update the saga state when we become Unwinding (which we do here)
            // and when we become Done (which we do below).  There may be a
            // better place to put this logic that's less ad hoc.
            if live_state.exec_state == SagaCachedState::Unwinding
                && prev_state != SagaCachedState::Unwinding
            {
                live_state
                    .sec_hdl
                    .saga_update(SagaCachedState::Unwinding)
                    .await;
            }
            if live_state.exec_state == SagaCachedState::Done {
                break;
            }
        }

        let live_state = self.live_state.try_lock().unwrap();
        assert_eq!(live_state.exec_state, SagaCachedState::Done);
        self.finish_tx.send(()).expect("failed to send finish message");
        live_state.sec_hdl.saga_update(live_state.exec_state).await;
    }

    // Kick off any nodes that are ready to run.  (Right now, we kick off
    // everything, so it might seem unnecessary to store this vector in
    // "self" to begin with.  However, the intent is to add capacity limits,
    // in which case we may return without having scheduled everything, and
    // we want to track whatever's still ready to go.)
    // TODO-cleanup revisit dance with the vec to satisfy borrow rules
    async fn kick_off_ready(
        &self,
        tx: &mpsc::Sender<TaskCompletion<UserType>>,
    ) {
        let mut live_state = self.live_state.lock().await;

        if live_state.stopping {
            // We're waiting for outstanding tasks to stop.  Don't kick off any
            // more.
            assert!(!live_state.node_tasks.is_empty());
            return;
        }

        // TODO is it possible to deadlock with a concurrency limit given that
        // we always do "todo" before "undo"?

        let todo_queue = live_state.queue_todo.clone();
        live_state.queue_todo = Vec::new();

        for node_id in todo_queue {
            // TODO-design It would be good to check whether the saga is
            // unwinding, and if so, whether this action has ever started
            // running before.  If not, then we can send this straight to
            // undoing without doing any more work here.  What we're
            // doing here should also be safe, though.  We run the action
            // regardless, and when we complete it, we'll undo it.
            // TODO we could be much more efficient without copying this tree
            // each time.
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                false,
            );

            let saga_params = self.saga_params_for(&live_state, node_id);
            let sgaction = if live_state.injected_errors.contains(&node_id) {
                Arc::new(ActionInjectError {}) as Arc<dyn Action<UserType>>
            } else {
                self.node_action(&live_state, node_id)
            };

            let task_params = TaskParams {
                dag: Arc::clone(&self.dag),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                saga_params,
                action: sgaction,
                user_context: Arc::clone(&self.user_context),
                injected_repeat: live_state
                    .injected_repeats
                    .get(&node_id)
                    .map(|r| *r),
            };

            let task = tokio::spawn(SagaExecutor::exec_node(task_params));
            live_state.node_task(node_id, task);
        }

        if live_state.exec_state == SagaCachedState::Running {
            assert!(live_state.queue_undo.is_empty());
            return;
        }

        let undo_queue = live_state.queue_undo.clone();
        live_state.queue_undo = Vec::new();

        for node_id in undo_queue {
            // TODO commonize with code above
            // TODO we could be much more efficient without copying this tree
            // each time.
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                true,
            );

            let saga_params = self.saga_params_for(&live_state, node_id);
            let sgaction = if live_state.injected_undo_errors.contains(&node_id)
            {
                Arc::new(ActionInjectError {}) as Arc<dyn Action<UserType>>
            } else {
                self.node_action(&live_state, node_id)
            };
            let task_params = TaskParams {
                dag: Arc::clone(&self.dag),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                saga_params,
                action: sgaction,
                user_context: Arc::clone(&self.user_context),
                injected_repeat: live_state
                    .injected_repeats
                    .get(&node_id)
                    .map(|r| *r),
            };

            let task = tokio::spawn(SagaExecutor::undo_node(task_params));
            live_state.node_task(node_id, task);
        }
    }

    fn node_action(
        &self,
        live_state: &SagaExecLiveState,
        node_index: NodeIndex,
    ) -> Arc<dyn Action<UserType>> {
        let registry = &self.action_registry;
        let dag = &self.dag;
        match dag.get(node_index).unwrap() {
            InternalNode::Action { action_name: action, .. } => {
                // This condition was checked in SagaExec::validate_saga().  It
                // shouldn't be possible to blow this assertion.
                registry.get(action).expect("missing action for node")
            }

            InternalNode::Constant { value, .. } => {
                Arc::new(ActionConstant::new(Arc::clone(value)))
            }

            InternalNode::Start { .. }
            | InternalNode::End
            | InternalNode::SubsagaStart { .. } => {
                // These nodes are no-ops in terms of the action that happens at
                // the node itself.
                Arc::new(ActionConstant::new(Arc::new(serde_json::Value::Null)))
            }

            InternalNode::SubsagaEnd { .. } => {
                // We record the subsaga's output here, as the output of the
                // `SubsagaEnd` node.  (We don't _have_ to do this -- we could
                // instead change the logic that _finds_ the saga's output to
                // look in the same place that we do here.  But this is a simple
                // and clear way to do this.)
                // TODO-robustness we validate that there's exactly one final
                // node when we build the DAG, but we should also validate it
                // during recovery or else fail more gracefully here.  See
                // steno#32 and steno#106.
                let ancestors: Vec<_> = dag
                    .graph
                    .neighbors_directed(node_index, Incoming)
                    .collect();
                assert_eq!(ancestors.len(), 1);
                Arc::new(ActionConstant::new(
                    live_state.node_output(ancestors[0]),
                ))
            }
        }
    }

    /// Body of a (tokio) task that executes an action.
    async fn exec_node(task_params: TaskParams<UserType>) {
        let node_id = task_params.node_id;

        {
            // TODO-liveness We don't want to hold this lock across a call
            // to the database.  It's fair to say that if the database
            // hangs, the saga's corked anyway, but we should at least be
            // able to view its state, and we can't do that with this
            // design.
            let mut live_state = task_params.live_state.lock().await;
            let load_status =
                live_state.sglog.load_status_for_node(node_id.into());
            match load_status {
                SagaNodeLoadStatus::NeverStarted => {
                    record_now(
                        &mut live_state,
                        node_id,
                        SagaNodeEventType::Started,
                    )
                    .await;
                }
                SagaNodeLoadStatus::Started => (),
                SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::Failed(_)
                | SagaNodeLoadStatus::UndoStarted(_)
                | SagaNodeLoadStatus::UndoFinished
                | SagaNodeLoadStatus::UndoFailed(_) => {
                    panic!("starting node in bad state")
                }
            }
        }

        let make_action_context = || ActionContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            saga_params: Arc::clone(&task_params.saga_params),
            node_id,
            dag: Arc::clone(&task_params.dag),
            user_context: Arc::clone(&task_params.user_context),
        };

        let mut result = task_params.action.do_it(make_action_context()).await;

        if let Some(repeat) = task_params.injected_repeat {
            for _ in 0..repeat.action.get() - 1 {
                result = task_params.action.do_it(make_action_context()).await;
            }
        }

        let node: Box<dyn SagaNodeRest<UserType>> = match result {
            Ok(output) => {
                Box::new(SagaNode { node_id, state: SgnsDone(output) })
            }
            Err(error) => {
                Box::new(SagaNode { node_id, state: SgnsFailed(error) })
            }
        };

        SagaExecutor::finish_task(task_params, node).await;
    }

    /// Body of a (tokio) task that executes a compensation action.
    // TODO-cleanup This has a lot in common with exec_node(), but enough
    // different that it doesn't make sense to parametrize that one.  Still, it
    // sure would be nice to clean this up.
    async fn undo_node(task_params: TaskParams<UserType>) {
        let node_id = task_params.node_id;

        {
            let mut live_state = task_params.live_state.lock().await;
            let load_status =
                live_state.sglog.load_status_for_node(node_id.into());
            match load_status {
                SagaNodeLoadStatus::Succeeded(_) => {
                    record_now(
                        &mut live_state,
                        node_id,
                        SagaNodeEventType::UndoStarted,
                    )
                    .await;
                }
                SagaNodeLoadStatus::UndoStarted(_) => (),
                SagaNodeLoadStatus::NeverStarted
                | SagaNodeLoadStatus::Started
                | SagaNodeLoadStatus::Failed(_)
                | SagaNodeLoadStatus::UndoFinished
                | SagaNodeLoadStatus::UndoFailed(_) => {
                    panic!("undoing node in bad state")
                }
            }
        }

        let make_action_context = || ActionContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            saga_params: Arc::clone(&task_params.saga_params),
            node_id,
            dag: Arc::clone(&task_params.dag),
            user_context: Arc::clone(&task_params.user_context),
        };

        let count =
            task_params.injected_repeat.map(|r| r.undo.get()).unwrap_or(1);
        let action = &task_params.action;
        let undo_error = futures::stream::iter(0..count)
            .map(Ok::<u32, _>)
            .try_for_each(|i| async move {
                action
                    .undo_it(make_action_context())
                    .await
                    .with_context(|| format!("undo action attempt {}", i + 1))
            })
            .await;

        if let Err(error) = undo_error {
            let node = Box::new(SagaNode {
                node_id,
                state: SgnsUndoFailed(UndoActionError::PermanentFailure {
                    source_error: json!({ "message": format!("{:#}", error) }),
                }),
            });
            SagaExecutor::finish_task(task_params, node).await;
        } else {
            let node = Box::new(SagaNode {
                node_id,
                state: SgnsUndone(UndoMode::ActionUndone),
            });
            SagaExecutor::finish_task(task_params, node).await;
        };
    }

    async fn finish_task(
        mut task_params: TaskParams<UserType>,
        node: Box<dyn SagaNodeRest<UserType>>,
    ) {
        let node_id = task_params.node_id;
        let event_type = node.log_event();

        {
            let mut live_state = task_params.live_state.lock().await;
            record_now(&mut live_state, node_id, event_type).await;
        }

        task_params
            .done_tx
            .try_send(TaskCompletion { node_id, node })
            .expect("unexpected channel failure");
    }

    // TODO-design Today, callers that invoke run() maintain a handle to the
    // SagaExec so that they can control and check the status of execution.
    // But ideally, once a caller has invoked run(), they wouldn't be able to
    // call it again; and ideally, they wouldn't be able to get the result of
    // the saga until run() had finished.  One way we might do this is to
    // have run() consume the WfExec, return immediately an object that can be
    // used only for status and control, and provide a method on that object
    // that turns into the result.
    /// Runs the saga to completion asynchronously
    pub fn run(&self) -> impl Future<Output = ()> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_saga().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    /// Returns a [`SagaResult`] describing the result of the saga, including
    /// data produced by its actions.
    ///
    /// # Panics
    ///
    /// If the saga has not yet completed.
    pub fn result(&self) -> SagaResult {
        // TODO-cleanup is there a way to make this safer?  If we could know
        // that there were no other references to the live_state (which should
        // be true, if we're done), then we could consume it, as well as "self",
        // and avoid several copies below.
        let live_state = self
            .live_state
            .try_lock()
            .expect("attempted to get result while saga still running?");
        assert_eq!(live_state.exec_state, SagaCachedState::Done);

        if !live_state.undo_errors.is_empty() {
            let (error_node_id, error_source) =
                live_state.node_errors.iter().next().expect(
                    "expected an action to have failed if an \
                        undo action failed",
                );
            let (undo_error_node_id, undo_error_source) =
                live_state.undo_errors.iter().next().unwrap();
            let error_node_name = self
                .dag
                .get(*error_node_id)
                .unwrap()
                .node_name()
                .expect("unexpected failure from unnamed node")
                .clone();
            let undo_error_node_name = self
                .dag
                .get(*undo_error_node_id)
                .unwrap()
                .node_name()
                .expect("unexpected failure from unnamed undo node")
                .clone();
            return SagaResult {
                saga_id: self.saga_id,
                saga_log: live_state.sglog.clone(),
                kind: Err(SagaResultErr {
                    error_node_name,
                    error_source: error_source.clone(),
                    undo_failure: Some((
                        undo_error_node_name,
                        undo_error_source.clone(),
                    )),
                }),
            };
        }

        if live_state.nodes_undone.contains_key(&self.dag.start_node) {
            assert!(live_state.nodes_undone.contains_key(&self.dag.end_node));

            // Choosing the first node_id in node_errors will find the
            // topologically-first node that failed.  This may not be the one
            // that actually triggered the saga to fail, but it could have done
            // so.  (That is, if there were another action that failed that
            // triggered the saga to fail, this one did not depend on it, so it
            // could as well have happened in the other order.)
            let (error_node_id, error_source) =
                live_state.node_errors.iter().next().unwrap();
            let error_node_name = self
                .dag
                .get(*error_node_id)
                .unwrap()
                .node_name()
                .expect("unexpected failure from unnamed node")
                .clone();
            return SagaResult {
                saga_id: self.saga_id,
                saga_log: live_state.sglog.clone(),
                kind: Err(SagaResultErr {
                    error_node_name,
                    error_source: error_source.clone(),
                    undo_failure: None,
                }),
            };
        }

        assert!(live_state.nodes_undone.is_empty());
        let node_outputs = live_state
            .node_outputs
            .iter()
            .filter_map(|(node_id, node_output)| {
                self.dag.get(*node_id).unwrap().node_name().map(|node_name| {
                    (node_name.clone(), Arc::clone(node_output))
                })
            })
            .collect();

        // The output node is the (sole) ancestor of the "end" node.
        // There must be exactly one.  This was checked in
        // SagaExec::validate_dag().
        let output_node_index = self
            .dag
            .graph
            .neighbors_directed(self.dag.end_node, Incoming)
            .next()
            .unwrap();
        let saga_output = live_state.node_output(output_node_index);

        SagaResult {
            saga_id: self.saga_id,
            saga_log: live_state.sglog.clone(),
            kind: Ok(SagaResultOk { saga_output, node_outputs }),
        }
    }

    pub fn status(&self) -> BoxFuture<'_, SagaExecStatus> {
        async move {
            let live_state = self.live_state.lock().await;

            let mut node_exec_states = BTreeMap::new();

            let graph = &self.dag.graph;
            let topo_visitor = Topo::new(graph);
            for node in topo_visitor.iter(graph) {
                // Record the current execution state for this node.
                node_exec_states.insert(node, live_state.node_exec_state(node));
            }

            SagaExecStatus {
                saga_id: self.saga_id,
                dag: Arc::clone(&self.dag),
                node_exec_states,
                sglog: live_state.sglog.clone(),
            }
        }
        .boxed()
    }
}

/// Encapsulates the (mutable) execution state of a saga
// This is linked to a `SagaExecutor` and protected by a Mutex.  The state is
// mainly modified by [`SagaExecutor::run_saga`].  We may add methods for
// controlling the saga (e.g., pausing), which would modify this as well.
// We also intend to add methods for viewing saga state, which will take the
// lock to read state.
//
// If the view of a saga were just (1) that it's running, and maybe (2) a
// set of outstanding actions, then we might take a pretty different approach
// here.  We might create a read-only view object that's populated periodically
// by the saga executor.  This still might be the way to go, but at the
// moment we anticipate wanting pretty detailed debug information (like what
// outputs were produced by what steps), so the view would essentially be a
// whole copy of this object.
// TODO This would be a good place for a debug log.
#[derive(Debug)]
struct SagaExecLiveState {
    /// Unique identifier for this saga (an execution of a saga template)
    saga_id: SagaId,

    sec_hdl: SecExecClient,

    /// Overall execution state
    exec_state: SagaCachedState,

    /// We're coming to rest, neither having fully finished nor unwound
    /// (as might happen if encountering an unretryable error from an undo
    /// action)
    stopping: bool,

    /// Queue of nodes that have not started but whose deps are satisfied
    queue_todo: Vec<NodeIndex>,
    /// Queue of nodes whose undo action needs to be run.
    queue_undo: Vec<NodeIndex>,

    /// Outstanding tokio tasks for each node in the graph
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,

    /// Outputs saved by completed actions.
    node_outputs: BTreeMap<NodeIndex, Arc<serde_json::Value>>,
    /// Set of undone nodes.
    nodes_undone: BTreeMap<NodeIndex, UndoMode>,
    /// Errors produced by failed actions.
    node_errors: BTreeMap<NodeIndex, ActionError>,
    /// Errors produced by failed undo actions.
    undo_errors: BTreeMap<NodeIndex, UndoActionError>,

    /// Persistent state
    sglog: SagaLog,

    /// Injected errors
    injected_errors: BTreeSet<NodeIndex>,

    /// Injected errors into undo actions
    injected_undo_errors: BTreeSet<NodeIndex>,

    /// Injected actions which should be called repeatedly
    injected_repeats: BTreeMap<NodeIndex, RepeatInjected>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NodeExecState {
    Blocked,
    QueuedToRun,
    TaskInProgress,
    Done,
    Failed,
    QueuedToUndo,
    UndoInProgress,
    Undone(UndoMode),
    UndoFailed,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum UndoMode {
    ActionNeverRan,
    ActionUndone,
    ActionFailed,
}

impl fmt::Display for NodeExecState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NodeExecState::Blocked => "blocked",
            NodeExecState::QueuedToRun => "queued-todo",
            NodeExecState::TaskInProgress => "working",
            NodeExecState::Done => "done",
            NodeExecState::Failed => "failed",
            NodeExecState::QueuedToUndo => "queued-undo",
            NodeExecState::UndoInProgress => "undo-working",
            NodeExecState::Undone(UndoMode::ActionNeverRan) => "abandoned",
            NodeExecState::Undone(UndoMode::ActionUndone) => "undone",
            NodeExecState::Undone(UndoMode::ActionFailed) => "failed",
            NodeExecState::UndoFailed => "undo-failed",
        })
    }
}

impl SagaExecLiveState {
    // TODO-design The current implementation does not use explicit state.  In
    // most cases, this made things better than before because each hunk of code
    // was structured to accept only nodes in states that were valid.  But
    // there are a few cases where we need a bit more state than we're currently
    // keeping.  This function is used there.
    //
    // It's especially questionable to use load_status here -- or is that the
    // way we should go more generally?  See TODO-design in new_recover().
    fn node_exec_state(&self, node_id: NodeIndex) -> NodeExecState {
        // This seems like overkill but it seems helpful to validate state.
        let mut set: BTreeSet<NodeExecState> = BTreeSet::new();
        let load_status = self.sglog.load_status_for_node(node_id.into());
        if let Some(undo_mode) = self.nodes_undone.get(&node_id) {
            set.insert(NodeExecState::Undone(*undo_mode));
        } else if self.queue_undo.contains(&node_id) {
            set.insert(NodeExecState::QueuedToUndo);
        } else if let SagaNodeLoadStatus::Failed(_) = load_status {
            assert!(self.node_errors.contains_key(&node_id));
            set.insert(NodeExecState::Failed);
        } else if let SagaNodeLoadStatus::UndoFailed(_) = load_status {
            assert!(self.undo_errors.contains_key(&node_id));
            set.insert(NodeExecState::UndoFailed);
        } else if self.node_outputs.contains_key(&node_id) {
            if self.node_tasks.contains_key(&node_id) {
                set.insert(NodeExecState::UndoInProgress);
            } else {
                set.insert(NodeExecState::Done);
            }
        } else if self.node_tasks.contains_key(&node_id) {
            set.insert(NodeExecState::TaskInProgress);
        }

        if self.queue_todo.contains(&node_id) {
            set.insert(NodeExecState::QueuedToRun);
        }

        if set.is_empty() {
            if let SagaNodeLoadStatus::NeverStarted = load_status {
                NodeExecState::Blocked
            } else {
                panic!("could not determine node state");
            }
        } else {
            assert_eq!(set.len(), 1);
            let the_state = set.into_iter().next().unwrap();
            the_state
        }
    }

    fn mark_saga_done(&mut self) {
        assert!(!self.stopping);
        assert!(self.queue_todo.is_empty());
        assert!(self.queue_undo.is_empty());
        assert!(
            self.exec_state == SagaCachedState::Running
                || self.exec_state == SagaCachedState::Unwinding
        );
        self.exec_state = SagaCachedState::Done;
    }

    fn saga_stuck(&mut self) {
        assert!(self.exec_state == SagaCachedState::Unwinding);
        self.stopping = true;
        if self.node_tasks.is_empty() {
            self.exec_state = SagaCachedState::Done;
        }
    }

    fn node_task(&mut self, node_id: NodeIndex, task: JoinHandle<()>) {
        assert!(!self.stopping);
        self.node_tasks.insert(node_id, task);
    }

    fn node_task_done(&mut self, node_id: NodeIndex) -> JoinHandle<()> {
        let rv = self
            .node_tasks
            .remove(&node_id)
            .expect("processing task completion with no task present");

        if self.stopping && self.node_tasks.is_empty() {
            self.exec_state = SagaCachedState::Done;
        }

        rv
    }

    fn node_output(&self, node_id: NodeIndex) -> Arc<serde_json::Value> {
        let output =
            self.node_outputs.get(&node_id).expect("node has no output");
        Arc::clone(output)
    }
}

/// Summarizes the final state of a saga execution
#[derive(Clone, Debug)]
pub struct SagaResult {
    pub saga_id: SagaId,
    pub saga_log: SagaLog,
    pub kind: Result<SagaResultOk, SagaResultErr>,
}

/// Provides access to outputs from a saga that completed successfully
#[derive(Clone, Debug)]
pub struct SagaResultOk {
    saga_output: Arc<serde_json::Value>,
    node_outputs: BTreeMap<NodeName, Arc<serde_json::Value>>,
}

impl SagaResultOk {
    /// Returns the final output of the saga (the output from the last node)
    pub fn saga_output<T: ActionData + 'static>(
        &self,
    ) -> Result<T, ActionError> {
        serde_json::from_value((*self.saga_output).clone())
            .context("final saga output")
            .map_err(ActionError::new_deserialize)
    }

    /// Returns the data produced by a node in the saga.
    ///
    /// # Panics
    ///
    /// If the saga has no node called `name`.
    pub fn lookup_node_output<T: ActionData + 'static>(
        &self,
        name: &str,
    ) -> Result<T, ActionError> {
        let key = NodeName::new(name);
        let output_json =
            self.node_outputs.get(&key).unwrap_or_else(|| {
                panic!(
                    "node with name \"{}\": not part of this saga",
                    key.as_ref(),
                )
            });
        // TODO-cleanup double-asterisk seems odd?
        serde_json::from_value((**output_json).clone())
            .context("final node output")
            .map_err(ActionError::new_deserialize)
    }
}

/// Provides access to failure details for a saga that failed
///
/// When a saga fails, it's always one action's failure triggers failure of the
/// saga.  It's possible that other actions also failed, but only if they were
/// running concurrently.  This structure describes one of these errors, any of
/// which _could_ have caused the saga to fail (depending on the order in which
/// they completed).
///
/// After such a failure, when the saga is unwinding, it's possible for an undo
/// action to also fail.   Again, there could be more than one undo action
/// failure.  If any undo actions failed, this structure also describes one of
/// those failures.
// TODO-coverage We should test that sagas do the right thing when two actions
// fail concurrently.
//
// We don't allow callers to access outputs from a saga that failed
// because it's not obvious yet why this would be useful and it's too
// easy to shoot yourself in the foot by not checking whether the saga
// failed.  In practice, the enum that wraps this type ensures that the caller
// has checked for failure, so it wouldn't be unreasonable to provide outputs
// here.  (A strong case: there are cases where it's useful to get outputs even
// while the saga is running, as might happen for a saga that generates a
// database record whose id you want to return to a client without waiting for
// the saga to complete.  It's silly to let you get this id while the saga is
// running, but not after it's failed.)
#[derive(Clone, Debug)]
pub struct SagaResultErr {
    /// name of a node whose action failed
    pub error_node_name: NodeName,
    /// details about the action failure
    pub error_source: ActionError,
    /// if an undo action also failed, details about that failure
    pub undo_failure: Option<(NodeName, UndoActionError)>,
}

/// Summarizes in-progress execution state of a saga
#[derive(Clone, Debug)]
pub struct SagaExecStatus {
    saga_id: SagaId,
    node_exec_states: BTreeMap<NodeIndex, NodeExecState>,
    dag: Arc<SagaDag>,
    sglog: SagaLog,
}

impl fmt::Display for SagaExecStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f)
    }
}

impl SagaExecStatus {
    pub fn log(&self) -> &SagaLog {
        &self.sglog
    }

    /// Generate an output order via the [`PrintOrderer`] and then write it
    /// to the Formatter.
    pub fn print(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let orderer = PrintOrderer::new(&self.dag);
        let output = orderer.print_order();
        self.write_header(f)?;
        for entry in output {
            match entry {
                PrintOrderEntry::Node { idx, indent_level } => {
                    self.print_node(f, idx, indent_level)?;
                }
                PrintOrderEntry::Parallel { indent_level } => {
                    Self::write_indented(
                        f,
                        indent_level,
                        "(parallel actions):\n",
                    )?;
                }
            }
        }

        Ok(())
    }

    fn print_node(
        &self,
        f: &mut fmt::Formatter<'_>,
        idx: NodeIndex,
        indent_level: usize,
    ) -> fmt::Result {
        let node = self.dag.get(idx).unwrap();
        let label = Self::mklabel(&node);
        let state = &self.node_exec_states[&idx];
        let msg = format!("{}: {}\n", state, label);
        Self::write_indented(f, indent_level, &msg)?;
        Ok(())
    }

    fn write_indented(
        out: &mut fmt::Formatter<'_>,
        indent_level: usize,
        msg: &str,
    ) -> fmt::Result {
        write!(
            out,
            "{:width$}+-- {}",
            "",
            msg,
            width = Self::big_indent(indent_level)
        )
    }

    fn mklabel(node: &InternalNode) -> String {
        if let Some(name) = node.node_name() {
            format!("{} (produces {:?})", node.label(), name)
        } else {
            node.label()
        }
    }

    fn big_indent(indent_level: usize) -> usize {
        indent_level * 8
    }

    fn write_header(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            out,
            "{:width$}+ saga execution: {}\n",
            "",
            self.saga_id,
            width = 0
        )
    }
}

// An entry in the output Vec of the [`SagaExecStatus::print_order`] method.
#[derive(Debug, PartialEq)]
enum PrintOrderEntry {
    // Print a message that parallelization has started
    // The nodes themselves are ordered and indented properly
    // so this is just to help with labeling and testing
    Parallel { indent_level: usize },
    Node { idx: NodeIndex, indent_level: usize },
}

impl PrintOrderEntry {
    #[allow(unused)]
    fn is_node(&self) -> bool {
        if let PrintOrderEntry::Node { .. } = *self {
            true
        } else {
            false
        }
    }
}

// A stack entry contains all parallel nodes that must run during a
// given indent level.
#[derive(Debug, PartialEq)]
enum StackEntry {
    Parallel(Vec<NodeIndex>),
    Subsaga,
}

// Structure the Dag for printing as text
//
// See [`PrintOrderer::print_order`] for details.
struct PrintOrderer<'a> {
    dag: &'a SagaDag,
    output: Vec<PrintOrderEntry>,
    stack: Vec<StackEntry>,
    idx: NodeIndex,
    indent_level: usize,
}

impl<'a> PrintOrderer<'a> {
    pub fn new(dag: &'a SagaDag) -> PrintOrderer<'a> {
        let idx = dag.start_node;
        PrintOrderer {
            dag,
            output: Vec::new(),
            stack: Vec::new(),
            idx,
            indent_level: 0,
        }
    }

    fn output_current_node(&mut self) {
        self.output.push(PrintOrderEntry::Node {
            idx: self.idx,
            indent_level: self.indent_level,
        });
    }

    fn output_parallel(&mut self) {
        self.output.push(PrintOrderEntry::Parallel {
            indent_level: self.indent_level,
        });
    }

    // Generate a print order of indexes along with their indent_level
    //
    // We want to walk the DAG in topological order and print indents in the
    // following scenarios:
    //     * `Parallel` nodes have been reached
    //     * A `SubsagaStart` node has been reached
    //
    // We de-indent when:
    //     * the last parallel node at an indent_level is run
    //     * A `SubsagaEnd` node has been reached
    //
    // Importantly we must allow arbitrary nesting of subsagas, which
    // themselves may contain parallel nodes. However, due to the way
    // we constrain the DAGs with the [`DagBuilder`], we do not have to
    // worry about parallel nodes spawning othert parallel nodes directly.
    // In other words, all parallel nodes must complete before the
    // next set of parallel nodes are run. This is because each call to
    // [`DagBuilder::append_parallel`], results in a set of nodes known as
    // `last_nodes` that must complete before any new nodes are added to the
    // graph with `[DagBuilder::append]` or `[DagBuilder::append_parallel]`.
    // Graphically, each `last_node` has an outgoing edge to any
    // nodes added in the next call to [`DagBuilder::append`] or
    // [`DagBuilder::append_parallel`]. Thus when the last parallel node
    // at a given level is done being printed we can descend the graph.
    //
    // The only way to have one parallel node lead to other nodes in
    // its parallel branch that the other parallel nodes at its level
    // don't also lead to is for the parallel node itself to be an
    // [`InternalNode::SubsagaStart`].
    fn print_order(mut self) -> Vec<PrintOrderEntry> {
        // Start walking the graph
        //
        // * Whenever a subsaga starts we want to print its children before any
        //   parallel nodes.
        // * Whenever a subsaga ends, we check to see if there are any parallel
        //   nodes before we look for children.
        // * Whenever there is a simple node, we check to see if there are
        //   parallel nodes before we look for children.
        //
        // This results in
        while self.idx != self.dag.end_node {
            let node = self.dag.get(self.idx).unwrap();

            if let &InternalNode::SubsagaStart { .. } = node {
                // Add the current node to the output before indenting
                self.output_current_node();
                self.indent_level += 1;
                self.stack.push(StackEntry::Subsaga);
                self.descend();
            } else if let &InternalNode::SubsagaEnd { .. } = node {
                self.indent_level -= 1;
                self.stack.pop();

                // Add the current node to the output after de-indenting
                self.output_current_node();

                if !self.next_parallel_node() {
                    self.descend();
                }
            } else {
                // Simple node
                // Add the current node to the output
                self.output_current_node();

                if !self.next_parallel_node() {
                    self.descend();
                }
            }
        }

        // Output the end node
        self.output_current_node();

        assert!(self.stack.is_empty());
        return self.output;
    }

    /// Descend down the graph
    fn descend(&mut self) {
        let mut children: Vec<NodeIndex> =
            self.dag.graph.neighbors_directed(self.idx, Outgoing).collect();

        if children.len() == 0 {
            // We are done
            assert!(self.stack.is_empty());
            assert_eq!(self.dag.end_node, self.idx);
            return;
        }

        if children.len() == 1 {
            self.idx = children[0];
        } else {
            self.output_parallel();
            // Pick the first child to print
            self.idx = children.pop().unwrap();
            self.indent_level += 1;
            // These nodes must run in parallel. Put the remainder on the stack.
            self.stack.push(StackEntry::Parallel(children));
        }
    }

    // Set the next parallel node if it exists.
    //
    // Return true if there is a parallel node.
    //
    // Side effects:
    //   * If there is a parallel node we remove it from the nodes in the top of
    //     the stack.
    //   * If this is the last parallel node, we pop the top of the stack and
    //     reduce the indent level.
    fn next_parallel_node(&mut self) -> bool {
        if let Some(StackEntry::Parallel(nodes)) = self.stack.last_mut() {
            if let Some(next_idx) = nodes.pop() {
                self.idx = next_idx;
                return true;
            } else {
                // This is the last parallel node
                self.indent_level -= 1;
                self.stack.pop();
            }
        }
        false
    }
}

/// Return true if all neighbors of `node_id` in the given `direction`  
/// return true for the predicate `test`.
fn neighbors_all<F>(
    graph: &Graph<InternalNode, ()>,
    node_id: &NodeIndex,
    direction: Direction,
    test: F,
) -> bool
where
    F: Fn(&NodeIndex) -> bool,
{
    for p in graph.neighbors_directed(*node_id, direction) {
        if !test(&p) {
            return false;
        }
    }

    return true;
}

/// Returns true if the parent node's load status is valid for the given child
/// node's load status.
fn recovery_validate_parent(
    parent_status: &SagaNodeLoadStatus,
    child_status: &SagaNodeLoadStatus,
) -> bool {
    match child_status {
        // If the child node has started, finished successfully, or even started
        // undoing, the only allowed status for the parent node is "done".  The
        // states prior to "done" are ruled out because we execute nodes in
        // dependency order.  "failed" is ruled out because we do not execute
        // nodes whose parents failed.  The undoing states are ruled out because
        // we unwind in reverse-dependency order, so we cannot have started
        // undoing the parent if the child node has not finished undoing.  (A
        // subtle but important implementation detail is that we do not undo a
        // node that has not started execution.  If we did, then the "undo
        // started" load state could be associated with a parent that failed.)
        SagaNodeLoadStatus::Started
        | SagaNodeLoadStatus::Succeeded(_)
        | SagaNodeLoadStatus::UndoStarted(_) => {
            matches!(parent_status, SagaNodeLoadStatus::Succeeded(_))
        }

        // If the child node has failed, this looks just like the previous case,
        // except that the parent node could be UndoStarted or UndoFinished.
        // That's possible because we don't undo a failed node, so after undoing
        // the parents, the log state would still show "failed".
        SagaNodeLoadStatus::Failed(_) => {
            matches!(
                parent_status,
                SagaNodeLoadStatus::Succeeded(_)
                    | SagaNodeLoadStatus::UndoStarted(_)
                    | SagaNodeLoadStatus::UndoFinished
                    | SagaNodeLoadStatus::UndoFailed(_)
            )
        }

        // If we've finished undoing the child node, then the parent must be
        // either "done" or one of the undoing states.
        SagaNodeLoadStatus::UndoFinished => matches!(
            parent_status,
            SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::UndoStarted(_)
                | SagaNodeLoadStatus::UndoFinished
                | SagaNodeLoadStatus::UndoFailed(_)
        ),

        // If we've failed to undo the child node, then the parent must be
        // "done".
        SagaNodeLoadStatus::UndoFailed(_) => {
            matches!(parent_status, SagaNodeLoadStatus::Succeeded(_))
        }

        // If a node has never started, the only illegal states for a parent are
        // those associated with undoing, since the child must be undone first.
        SagaNodeLoadStatus::NeverStarted => matches!(
            parent_status,
            SagaNodeLoadStatus::NeverStarted
                | SagaNodeLoadStatus::Started
                | SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::Failed(_)
        ),
    }
}

/// Action's handle to the saga subsystem
// Any APIs that are useful for actions should hang off this object.  It should
// have enough state to know which node is invoking the API.
pub struct ActionContext<UserType: SagaType> {
    ancestor_tree: Arc<BTreeMap<NodeName, Arc<serde_json::Value>>>,
    node_id: NodeIndex,
    dag: Arc<SagaDag>,
    user_context: Arc<UserType::ExecContextType>,
    saga_params: Arc<serde_json::Value>,
}

impl<UserType: SagaType> ActionContext<UserType> {
    /// Retrieves a piece of data stored by a previous (ancestor) node in the
    /// current saga.  The data is identified by `name`, the name of the
    /// ancestor node.
    ///
    /// # Panics
    ///
    /// This function panics if there was no data previously stored with name
    /// `name` (which means there was no ancestor node with that name).
    pub fn lookup<T: ActionData + 'static>(
        &self,
        name: &str,
    ) -> Result<T, ActionError> {
        // TODO: Remove this unnecessary allocation via `Borrow/Cow`
        let key = name.to_string();
        let item = self
            .ancestor_tree
            .get(&NodeName::new(key))
            .unwrap_or_else(|| panic!("no ancestor called \"{}\"", name));
        // TODO-cleanup double-asterisk seems ridiculous
        serde_json::from_value((**item).clone())
            .with_context(|| format!("output from earlier node {:?}", name))
            .map_err(ActionError::new_deserialize)
    }

    /// Returns the saga parameters for the current action
    ///
    /// If this action is being run as a subsaga, this returns the saga
    /// parameters for the subsaga.  This way actions don't have to care whether
    /// they're running in a saga or not.
    pub fn saga_params<T: ActionData + 'static>(
        &self,
    ) -> Result<T, ActionError> {
        serde_json::from_value((*self.saga_params).clone())
            .with_context(|| {
                let as_str = serde_json::to_string(&self.saga_params)
                    .unwrap_or_else(|_| format!("{:?}", self.saga_params));
                format!("saga params ({})", as_str)
            })
            .map_err(ActionError::new_deserialize)
    }

    /// Returns the human-readable label for the current saga node
    pub fn node_label(&self) -> String {
        self.dag.get(self.node_id).unwrap().label()
    }

    /// Returns the consumer-provided context for the current saga
    pub fn user_data(&self) -> &UserType::ExecContextType {
        &self.user_context
    }
}

/// Converts a NodeIndex (used by the graph representation to identify a node)
/// to a [`SagaNodeId`] (used elsewhere in this module to identify a node)
impl From<NodeIndex> for SagaNodeId {
    fn from(node_id: NodeIndex) -> SagaNodeId {
        // We (must) verify elsewhere that node indexes fit within a u32.
        SagaNodeId::from(u32::try_from(node_id.index()).unwrap())
    }
}

/// Wrapper for SagaLog.record_now() that maps internal node indexes to
/// stable node ids.
// TODO Consider how we do map internal node indexes to stable node ids.
// TODO clean up this interface
async fn record_now(
    live_state: &mut SagaExecLiveState,
    node: NodeIndex,
    event_type: SagaNodeEventType,
) {
    let saga_id = live_state.saga_id;
    let node_id = node.into();

    // The only possible failure here today is attempting to record an event
    // that's illegal for the current node state.  That's a bug in this
    // program.
    let event = SagaNodeEvent { saga_id, node_id, event_type };
    live_state.sglog.record(&event).unwrap();
    live_state.sec_hdl.record(event).await;
}

/// Consumer's handle for querying and controlling the execution of a single
/// saga
pub trait SagaExecManager: fmt::Debug + Send + Sync {
    /// Run the saga to completion.
    fn run(&self) -> BoxFuture<'_, ()>;
    /// Return the result of the saga
    ///
    /// The returned [`SagaResult`] has interfaces for querying saga outputs and
    /// error information.
    ///
    /// # Panics
    ///
    /// If the saga has not finished when this function is called.
    fn result(&self) -> SagaResult;

    /// Returns fine-grained information about saga execution
    fn status(&self) -> BoxFuture<'_, SagaExecStatus>;

    /// Replaces the action at the specified node with one that just generates
    /// an error
    ///
    /// See [`Dag::get_index()`] to get the node_id for a node.
    fn inject_error(&self, node_id: NodeIndex) -> BoxFuture<'_, ()>;

    /// Replaces the undo action at the specified node with one that just
    /// generates an error
    ///
    /// See [`Dag::get_index()`] to get the node_id for a node.
    fn inject_error_undo(&self, node_id: NodeIndex) -> BoxFuture<'_, ()>;

    /// Replaces the action at the specified node with one that calls both the
    /// action (and undo action, if called) twice.
    ///
    /// See [`Dag::get_index()`] to get the node_id for a node.
    fn inject_repeat(
        &self,
        node_id: NodeIndex,
        repeat: RepeatInjected,
    ) -> BoxFuture<'_, ()>;
}

impl<T> SagaExecManager for SagaExecutor<T>
where
    T: SagaType + fmt::Debug,
{
    fn run(&self) -> BoxFuture<'_, ()> {
        self.run().boxed()
    }

    fn result(&self) -> SagaResult {
        self.result()
    }

    fn status(&self) -> BoxFuture<'_, SagaExecStatus> {
        self.status()
    }

    fn inject_error(&self, node_id: NodeIndex) -> BoxFuture<'_, ()> {
        self.inject_error(node_id).boxed()
    }

    fn inject_error_undo(&self, node_id: NodeIndex) -> BoxFuture<'_, ()> {
        self.inject_error_undo(node_id).boxed()
    }

    fn inject_repeat(
        &self,
        node_id: NodeIndex,
        repeat: RepeatInjected,
    ) -> BoxFuture<'_, ()> {
        self.inject_repeat(node_id, repeat).boxed()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{DagBuilder, Node, SagaDag, SagaName};
    use petgraph::graph::NodeIndex;
    use std::fmt::Write;

    // Return a constant node with a null value
    fn constant(name: &str) -> Node {
        Node::constant(name, serde_json::Value::Null)
    }

    // Assert that the names match the NodeNames of the constant nodes at the
    // given indexes.
    fn constant_names_match(
        names: &[&str],
        indexes: &[NodeIndex],
        dag: &SagaDag,
    ) -> bool {
        assert_eq!(names.len(), indexes.len());
        for i in 0..names.len() {
            if !constant_name_matches(names[i], indexes[i], dag) {
                return false;
            }
        }
        true
    }
    fn constant_name_matches(
        name: &str,
        idx: NodeIndex,
        dag: &SagaDag,
    ) -> bool {
        let node = dag.get(idx).unwrap();
        matches!(
             node,
             InternalNode::Constant { name: a, .. }
               if a == &NodeName::new(name)
        )
    }

    fn is_start_node(idx: NodeIndex, dag: &SagaDag) -> bool {
        if let InternalNode::Start { .. } = dag.get(idx).unwrap() {
            true
        } else {
            false
        }
    }

    fn is_end_node(idx: NodeIndex, dag: &SagaDag) -> bool {
        if let InternalNode::End = dag.get(idx).unwrap() {
            true
        } else {
            false
        }
    }

    // Used to see the output structure to help write tests
    fn print_for_testing(
        entries: &Vec<PrintOrderEntry>,
        dag: &SagaDag,
    ) -> String {
        let mut out = String::new();
        for entry in entries {
            match entry {
                PrintOrderEntry::Node { idx, indent_level } => {
                    let node = dag.get(*idx).unwrap();
                    write!(&mut out, "{}{:?}\n", spaces(*indent_level), node)
                        .unwrap();
                }
                PrintOrderEntry::Parallel { indent_level } => {
                    write!(
                        &mut out,
                        "{}{:?}\n",
                        spaces(*indent_level),
                        "Parallel: "
                    )
                    .unwrap();
                }
            }
        }
        out
    }

    fn spaces(indent_level: usize) -> String {
        let num_spaces = indent_level * 4;
        (0..num_spaces).fold(String::new(), |mut acc, _| {
            acc.push(' ');
            acc
        })
    }

    #[test]
    fn test_print_order_no_subsagas_no_parallel() {
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(constant("a"));
        builder.append(constant("b"));
        let dag = builder.build().unwrap();
        let saga_dag = SagaDag::new(dag, serde_json::Value::Null);
        let orderer = PrintOrderer::new(&saga_dag);
        let entries = orderer.print_order();

        // There are 4 entries (start + end + 2 constant nodes);
        assert_eq!(4, entries.len());

        let mut indexes = Vec::new();

        // There are no indents
        for entry in entries {
            match entry {
                PrintOrderEntry::Node { idx, indent_level } => {
                    indexes.push(idx);
                    assert_eq!(indent_level, 0);
                }
                _ => panic!("No parallel nodes should exist"),
            }
        }

        // Assert the node order is what is expected
        assert!(is_start_node(indexes[0], &saga_dag));
        assert!(constant_name_matches("a", indexes[1], &saga_dag));
        assert!(constant_name_matches("b", indexes[2], &saga_dag));
        assert!(is_end_node(indexes[3], &saga_dag));
    }

    #[test]
    fn test_print_order_parallel_nodes_no_subsagas() {
        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(constant("a"));
        builder.append_parallel(vec![constant("b"), constant("c")]);
        builder.append(constant("d"));
        let dag = builder.build().unwrap();
        let saga_dag = SagaDag::new(dag, serde_json::Value::Null);
        let orderer = PrintOrderer::new(&saga_dag);
        let entries = orderer.print_order();

        // start, end, 4 constant nodes, a parallel entry
        assert_eq!(7, entries.len());

        let mut actual_indexes = Vec::new();
        // Ensure the order is what we expect
        for i in 0..7 {
            match entries[i] {
                PrintOrderEntry::Node { idx, indent_level } => match i {
                    0 => {
                        assert_eq!(indent_level, 0);
                        assert!(is_start_node(idx, &saga_dag));
                    }
                    1 | 5 => {
                        // This is a constant node, so push its index
                        assert_eq!(indent_level, 0);
                        actual_indexes.push(idx);
                    }
                    3..=4 => {
                        // This is a constant node, so push its index
                        assert_eq!(indent_level, 1);
                        actual_indexes.push(idx);
                    }
                    6 => {
                        assert_eq!(indent_level, 0);
                        assert!(is_end_node(idx, &saga_dag));
                    }
                    _ => panic!("invalid entry"),
                },

                PrintOrderEntry::Parallel { indent_level } => {
                    // We print a parallel label before indenting
                    assert_eq!(2, i);
                    // The parallel entry itself is not indented
                    assert_eq!(indent_level, 0);
                }
            }
        }

        let expected_names = vec!["a", "b", "c", "d"];
        assert!(constant_names_match(
            &expected_names,
            &actual_indexes,
            &saga_dag
        ));
    }

    #[test]
    fn test_print_order_nested_parallel_nodes_and_subsagas() {
        let mut nested_subsaga =
            DagBuilder::new(SagaName::new("test-nested-subsaga"));
        nested_subsaga.append(constant("a"));
        nested_subsaga.append_parallel(vec![constant("b"), constant("c")]);
        nested_subsaga.append(constant("d"));
        let nested_subsaga_dag = nested_subsaga.build().unwrap();

        let mut subsaga = DagBuilder::new(SagaName::new("test-subsaga"));
        subsaga.append(constant("a"));
        subsaga.append_parallel(vec![constant("b"), constant("c")]);
        subsaga.append(constant("d"));
        subsaga.append(Node::subsaga("e", nested_subsaga_dag.clone(), "d"));
        subsaga.append_parallel(vec![
            constant("f"),
            Node::subsaga("g", nested_subsaga_dag, "e"),
            constant("h"),
        ]);
        subsaga.append(constant("i"));
        let subsaga_dag = subsaga.build().unwrap();

        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(constant("a"));
        builder.append_parallel(vec![constant("b"), constant("c")]);
        builder.append(constant("d"));
        builder.append(Node::subsaga("e", subsaga_dag, "d"));
        let dag = builder.build().unwrap();
        let saga_dag = SagaDag::new(dag, serde_json::Value::Null);
        let orderer = PrintOrderer::new(&saga_dag);
        let entries = orderer.print_order();

        // It's super tedious to test by asserting on each entry as in the
        // prior tests. Instead, we generate a string and compare it to expected
        // output. The output is generated by the test, so it won't
        // change, and we can use a different format for actual
        // `SagaExecStatus` output.
        let actual = print_for_testing(&entries, &saga_dag);
        let expected = "\
Start { params: Null }
Constant { name: \"a\", value: Null }
\"Parallel: \"
    Constant { name: \"b\", value: Null }
    Constant { name: \"c\", value: Null }
Constant { name: \"d\", value: Null }
SubsagaStart { saga_name: \"test-subsaga\", params_node_name: \"d\" }
    Constant { name: \"a\", value: Null }
    \"Parallel: \"
        Constant { name: \"b\", value: Null }
        Constant { name: \"c\", value: Null }
    Constant { name: \"d\", value: Null }
    SubsagaStart { saga_name: \"test-nested-subsaga\", params_node_name: \"d\" \
                        }
        Constant { name: \"a\", value: Null }
        \"Parallel: \"
            Constant { name: \"b\", value: Null }
            Constant { name: \"c\", value: Null }
        Constant { name: \"d\", value: Null }
    SubsagaEnd { name: \"e\" }
    \"Parallel: \"
        Constant { name: \"f\", value: Null }
        SubsagaStart { saga_name: \"test-nested-subsaga\", params_node_name: \
                        \"e\" }
            Constant { name: \"a\", value: Null }
            \"Parallel: \"
                Constant { name: \"b\", value: Null }
                Constant { name: \"c\", value: Null }
            Constant { name: \"d\", value: Null }
        SubsagaEnd { name: \"g\" }
        Constant { name: \"h\", value: Null }
    Constant { name: \"i\", value: Null }
SubsagaEnd { name: \"e\" }
End
";

        assert_eq!(actual, expected);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::{Dag, DagBuilder, Node, SagaDag, SagaName};
    use petgraph::graph::NodeIndex;
    use proptest::prelude::*;

    // The type we want to generate values of. Its an abstract description of
    // the nodes of a DAG.
    //
    // These "descriptions" map to the operations we are going to perform:
    //  * [`Dag::append`]
    //  * [`Dag::append_parallel`]
    //  * [`Dag::append_subsaga`]
    //
    // Specifically, `NodeDesc::Constant` results in a call to `Dag::append`
    // with a constant node, `NodeDesc::Parallel` results in a call to
    // `Dag::append_parallel`, and  `NodeDesc::Subsaga` results in a call to
    // `Dag::append_subsaga`.
    //
    // The `Parallel` and  `Subsaga` variants are recursive and as such the
    // specific inner nodes will be constructed as necessary, with a separate
    // `DagBuilder` for subsaga nodes as needed.
    //
    // There is one complication to this recursive definition, however: There
    // is really no such thing as a `Parallel` node. It's a pseudo node type
    // that results in a call to [`Dag::append_parallel`]  on the given saga
    // or subsaga. But [`Dag::append_parallel`] can only be called on vectors
    // of real [`Node`]s. Thus we can't have a recursive generation of parallel
    // nodes inside parallel nodes directly, because it's nonsensical to say
    // something like `builder.append_parallel(builder.append_parallel)`. We
    // run into this issue because we want to use the recursive generative
    // abilities of `proptest` in order to generate arbitrary nestings of
    // subsagas and constant nodes inside subsagas and the outer saga, with
    // liberal usage of [`Dag::append_parallel`] as appropriate. We could write
    // a bunch of different types and do a whole lot of mapping to generate
    // subsagas, limiting each one with code to some depth and ensuring we
    // only call [`Dag::append_parallel`] as needed, but in the end subsagas
    // are still recursive, and we really don't want to worry about generating
    // actual matching numbers of subsaga start and subsaga end nodes. So we
    // end up just describing a slightly broken recursive definition for what
    // our node structure looks like.
    //
    // The great thing about utilizing propptest for recursive generation,
    // is that proptest knows how to generate this structure and shrink it
    // **efficiently**. Moreover, it's easy to rectify our issue with directly
    // nested `NodeDesc::Parallel` variants. In `arb_nodedesc()` below, we
    // ensure through the use of `prop_map`, that whenever we see a `Parallel`
    // inner node we wrap it in a subsaga, rather than generating a new
    // parallel node. In other words, we ensure that `NodeDesc::Parallel` only
    // constains `NodeDesc::Constant` and `NodeDesc::Subsaga` variants.
    #[derive(Clone, Debug)]
    enum NodeDesc {
        Constant,
        Parallel(Vec<NodeDesc>),
        Subsaga(Vec<NodeDesc>),
    }

    impl NodeDesc {
        fn is_parallel(&self) -> bool {
            if let NodeDesc::Parallel(_) = *self {
                true
            } else {
                false
            }
        }
    }

    // Create an arbitrary `NodeDesc` using proptest generation
    fn arb_nodedesc() -> impl Strategy<Value = NodeDesc> {
        // Configuration for recursive generation
        // See https://altsysrq.github.io/proptest-book/proptest/tutorial/recursive.html
        let num_levels = 8;
        let max_size = 256;
        let items_per_collection = 10;
        let leaf = prop_oneof![Just(NodeDesc::Constant)];

        leaf.prop_recursive(
            num_levels,
            max_size,
            items_per_collection,
            |inner| {
                prop_oneof![
                    // Parallel nodes must contain at least 2 nodes
                    prop::collection::vec(inner.clone(), 2..10).prop_map(|v| {
                        // Ensure that Parallel nodes do not contain parallel
                        // nodes
                        if v.iter().any(|node_desc| node_desc.is_parallel()) {
                            NodeDesc::Subsaga(v)
                        } else {
                            NodeDesc::Parallel(v)
                        }
                    }),
                    // Subsagas must contain at least one node
                    prop::collection::vec(inner, 1..10)
                        .prop_map(NodeDesc::Subsaga)
                ]
            },
        )
    }

    // Create a Dag from the proptest generated `Vec<NodeDesc>`.
    //
    // Note that this method recurses in order to create subsagas.
    fn new_dag(nodes: &Vec<NodeDesc>, depth: usize) -> Dag {
        // The outermost dag that will become the SagaDag
        let name = SagaName::new(&format!("test-saga-{}", depth));
        let mut dag = DagBuilder::new(name);

        // For simplicity, we always just use "0" for the `params_node_name`
        // of subsagas. Every saga has one of these nodes, so it works fine.
        // We don't really care about the values because we are only testing
        // structure, not saga behavior.
        let params_node_name = "0";

        // Always append a node named "0", so our lookups work for subsaga
        // params
        dag.append(Node::constant(params_node_name, serde_json::Value::Null));

        // Node names are just numbers that we can increment and convert to
        // strings. Each DAG uses the same set, since they are
        // namespaced.
        let mut node_name = 1;

        for node in nodes {
            match node {
                NodeDesc::Constant => {
                    dag.append(Node::constant(
                        &node_name.to_string(),
                        serde_json::Value::Null,
                    ));
                    node_name += 1;
                }
                NodeDesc::Parallel(parallel_nodes) => {
                    let mut output = Vec::with_capacity(parallel_nodes.len());
                    for node in parallel_nodes {
                        match node {
                            NodeDesc::Constant => {
                                output.push(Node::constant(
                                    &node_name.to_string(),
                                    serde_json::Value::Null,
                                ));
                                node_name += 1;
                            }
                            NodeDesc::Subsaga(subsaga_nodes) => {
                                // Recurse
                                let subsaga_dag =
                                    new_dag(subsaga_nodes, depth + 1);
                                output.push(Node::subsaga(
                                    &node_name.to_string(),
                                    subsaga_dag,
                                    params_node_name,
                                ));
                                node_name += 1;
                            }
                            NodeDesc::Parallel(_) => panic!(
                                "Strategy Generation Error: Nested \
                                 `NodeDesc::Parallel` not allowed!"
                            ),
                        }
                    }
                    dag.append_parallel(output);
                }
                NodeDesc::Subsaga(subsaga_nodes) => {
                    let subsaga_dag = new_dag(subsaga_nodes, depth + 1);
                    dag.append(Node::subsaga(
                        &node_name.to_string(),
                        subsaga_dag,
                        params_node_name,
                    ));
                    node_name += 1;
                }
            }
        }

        // Always append a single constant node, just to ensure there is always
        // one saga output node.
        dag.append(Node::constant(
            &node_name.to_string(),
            serde_json::Value::Null,
        ));

        dag.build().unwrap()
    }

    // Is this stack frame an indent from a parallel node or a subsaga,
    #[derive(Debug, Clone, PartialEq)]
    enum IndentStackEntry {
        Parallel,
        Subsaga,
    }

    fn num_ancestors(dag: &SagaDag, idx: NodeIndex) -> usize {
        dag.graph.edges_directed(idx, Direction::Incoming).count()
    }

    // Pick a child of a node and see how many ancestors it has.
    //
    // If there is only one ancestor, it must be from the current node,
    // which indicates that the current node was not appended in parallel.
    fn num_ancestors_of_child(dag: &SagaDag, idx: NodeIndex) -> usize {
        let child = dag
            .graph
            .neighbors_directed(idx, Direction::Outgoing)
            .next()
            .unwrap();
        dag.graph.edges_directed(child, Direction::Incoming).count()
    }

    fn appended_in_parallel(dag: &SagaDag, idx: NodeIndex) -> bool {
        num_ancestors_of_child(dag, idx) > 1
    }

    // Indents must adhere to the following properties:
    //   * They must only increment or decrement by `1` at a time
    //   * Increments only come from Parallel print entries or SubsagaStart
    //     nodes
    //   * Decrements only come from Parallel print entries ending or SubsagaEnd
    //     nodes
    fn property_indents_are_correct(
        entries: &Vec<PrintOrderEntry>,
        dag: &SagaDag,
    ) -> Result<(), TestCaseError> {
        let mut indent_stack = Vec::new();
        for entry in entries {
            match entry {
                PrintOrderEntry::Node { idx, indent_level } => {
                    let node = dag.get(*idx).unwrap();
                    match node {
                        InternalNode::Start { .. } => {
                            // Start nodes should always be at indent 0
                            prop_assert_eq!(0, *indent_level);
                            prop_assert_eq!(indent_stack.len(), *indent_level);
                        }
                        InternalNode::End { .. } => {
                            // End nodes should always be at indent 0
                            prop_assert_eq!(0, *indent_level);
                            prop_assert_eq!(indent_stack.len(), *indent_level);
                        }
                        InternalNode::Action { .. } => {
                            panic!("No actions should exist!")
                        }
                        InternalNode::Constant { .. } => {
                            let parallel = appended_in_parallel(dag, *idx);
                            if *indent_level == 0 && indent_stack.is_empty() {
                                // We are at the top level, and not in a
                                // parallel node.
                                prop_assert!(!parallel);
                                continue;
                            }
                            if indent_stack.len() == *indent_level {
                                // This node is part of a subsaga or is a
                                // parallel node.
                                if let &IndentStackEntry::Subsaga =
                                    indent_stack.last().unwrap()
                                {
                                    prop_assert!(!parallel);
                                } else {
                                    prop_assert!(parallel);
                                }
                            } else {
                                // This is the first node after all the
                                // parallel nodes. We don't explicitly track
                                // this in the print order.
                                //
                                // It can only be a de-indent
                                prop_assert_eq!(
                                    indent_stack.len() - 1,
                                    *indent_level
                                );
                                // The last indent had to be a parallel indent
                                prop_assert_eq!(
                                    &IndentStackEntry::Parallel,
                                    indent_stack.last().unwrap()
                                );
                                // This node was not appended in parallel
                                prop_assert!(!parallel);

                                // This node has multiple ancestors, meaning
                                // that its parents were appended in parallel
                                prop_assert!(num_ancestors(dag, *idx) > 1);

                                // We need to pop the stack to get back in sync
                                // with the Dag
                                indent_stack.pop();
                            }
                        }
                        InternalNode::SubsagaStart { .. } => {
                            // We must compensate for not having an explicit
                            // end of parallel entry in the PrintOutput
                            if indent_stack.len() != *indent_level {
                                prop_assert_eq!(
                                    indent_stack.len() - 1,
                                    *indent_level
                                );

                                // This node has multiple ancestors, meaning
                                // that its parents were appended in parallel
                                prop_assert!(num_ancestors(dag, *idx) > 1);

                                prop_assert_eq!(
                                    &IndentStackEntry::Parallel,
                                    indent_stack.last().unwrap()
                                );
                                // The last parallel node has already been seen
                                // in the
                                // print output, because the indent_level has
                                // been reduced.
                                // Pop the stack to compensate.
                                indent_stack.pop();
                            }
                            indent_stack.push(IndentStackEntry::Subsaga);
                        }
                        InternalNode::SubsagaEnd { .. } => {
                            // SubsagaEnd nodes should always be at the
                            // de-indented and aligned with the outer indent
                            // level.
                            prop_assert!(!indent_stack.is_empty());
                            prop_assert_eq!(
                                indent_stack.len() - 1,
                                *indent_level
                            );
                            prop_assert_eq!(
                                indent_stack.last().unwrap(),
                                &IndentStackEntry::Subsaga
                            );
                            // We need to pop the stack to get back in sync with
                            // the Dag
                            indent_stack.pop();
                        }
                    }
                }
                PrintOrderEntry::Parallel { indent_level } => {
                    // Two append parallel calls can arrive in a row. Since
                    // we don't keep track of when parallel sections end
                    // explicitly, we have to check the indent_level to see if
                    // it matches the stack. This is similar to what we do in
                    // the `else` clause of `InternalNode::Constant`.
                    if indent_stack.len() == *indent_level {
                        if !indent_stack.is_empty() {
                            // If we are not at the top level, the innermost
                            // indent must be a subsaga
                            prop_assert_eq!(
                                &IndentStackEntry::Subsaga,
                                indent_stack.last().unwrap()
                            );
                        }
                    } else {
                        prop_assert_eq!(
                            &IndentStackEntry::Parallel,
                            indent_stack.last().unwrap()
                        );
                        prop_assert_eq!(indent_stack.len() - 1, *indent_level);
                        // The last parallel node has already been seen in the
                        // priint output, because the indent_level has been
                        // reduced. Pop the stack to compensate.
                        indent_stack.pop();
                    }
                    indent_stack.push(IndentStackEntry::Parallel);
                }
            }
        }
        Ok(())
    }

    proptest! {
        #[test]
        fn prints_correctly(nodes in prop::collection::vec(arb_nodedesc(), 1..10)) {
            //println!("{:#?}", nodes);
            let dag = new_dag(&nodes, 0);
            //println!("{:#?}", dag);

            let saga_dag = SagaDag::new(dag, serde_json::Value::Null);
            let orderer = PrintOrderer::new(&saga_dag);
            let entries = orderer.print_order();
            //println!("{:#?}", entries);

            // Property 1: The number of actual ordered node entries equals
            // the number of nodes in the dag
            let num_nodes = entries.iter().filter(|e| e.is_node()).count();
            prop_assert_eq!(num_nodes, saga_dag.graph.node_count());

            // Property 2: Indents relate to subsagas or parallel nodes
            property_indents_are_correct(&entries, &saga_dag)?;
        }
    }
}
