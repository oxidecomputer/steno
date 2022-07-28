//! Manages execution of a saga

use crate::dag::Node;
use crate::dag::NodeName;
use crate::rust_features::ExpectNone;
use crate::saga_action_error::ActionError;
use crate::saga_action_generic::Action;
use crate::saga_action_generic::ActionConstant;
use crate::saga_action_generic::ActionData;
use crate::saga_action_generic::ActionInjectError;
use crate::saga_log::SagaNodeEventType;
use crate::saga_log::SagaNodeLoadStatus;
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
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::FutureExt;
use futures::StreamExt;
use petgraph::algo::toposort;
use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/*
 * TODO-design Should we go even further and say that each node is its own
 * struct with incoming channels from parents (to notify when done), from
 * children (to notify when undone), and to each direction as well?  Then the
 * whole thing is a message passing exercise?
 */
struct SgnsDone(Arc<serde_json::Value>);
struct SgnsFailed(ActionError);
struct SgnsUndone(UndoMode);

struct SagaNode<S: SagaNodeStateType> {
    node_id: NodeIndex,
    state: S,
}

trait SagaNodeStateType {}
impl SagaNodeStateType for SgnsDone {}
impl SagaNodeStateType for SgnsFailed {}
impl SagaNodeStateType for SgnsUndone {}

/* TODO-design Is this right?  Is the trait supposed to be empty? */
trait SagaNodeRest<UserType: SagaType>: Send + Sync {
    fn propagate(
        &self,
        exec: &SagaExecutor<UserType>,
        live_state: &mut SagaExecLiveState<UserType>,
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
        live_state: &mut SagaExecLiveState<UserType>,
    ) {
        let graph = &exec.dag.graph;
        assert!(!live_state.node_errors.contains_key(&self.node_id));
        live_state
            .node_outputs
            .insert(self.node_id, Arc::clone(&self.state.0))
            .expect_none("node finished twice (storing output)");

        if self.node_id == exec.dag.end_node {
            /*
             * If we've completed the last node, the saga is done.
             */
            assert_eq!(live_state.exec_state, SagaCachedState::Running);
            assert_eq!(graph.node_count(), live_state.node_outputs.len());
            live_state.mark_saga_done();
            return;
        }

        if live_state.exec_state == SagaCachedState::Unwinding {
            /*
             * If the saga is currently unwinding, then this node finishing
             * doesn't unblock any other nodes.  However, it potentially
             * unblocks undoing itself.  We'll only proceed if all of our child
             * nodes are "undone" already.
             */
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_undone.contains_key(child)
            }) {
                live_state.queue_undo.push(self.node_id);
            }
            return;
        }

        /*
         * Under normal execution, this node's completion means it's time to
         * check dependent nodes to see if they're now runnable.
         */
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
        live_state: &mut SagaExecLiveState<UserType>,
    ) {
        let graph = &exec.dag.graph;
        assert!(!live_state.node_outputs.contains_key(&self.node_id));
        live_state
            .node_errors
            .insert(self.node_id, self.state.0.clone())
            .expect_none("node finished twice (storing error)");

        if live_state.exec_state == SagaCachedState::Unwinding {
            /*
             * This node failed while we're already unwinding.  We don't
             * need to kick off unwinding again.  We could in theory
             * immediately move this node to "undone" and unblock its
             * dependents, but for consistency with a simpler algorithm,
             * we'll wait for unwinding to propagate from the end node.
             * If all of our children are already undone, however, we
             * must go ahead and mark ourselves undone and propagate
             * that.
             */
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
            /*
             * Begin the unwinding process.  Start with the end node: mark
             * it trivially "undone" and propagate that.
             */
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
        live_state: &mut SagaExecLiveState<UserType>,
    ) {
        let graph = &exec.dag.graph;
        assert_eq!(live_state.exec_state, SagaCachedState::Unwinding);
        live_state
            .nodes_undone
            .insert(self.node_id, self.state.0)
            .expect_none("node already undone");

        if self.node_id == exec.dag.start_node {
            /*
             * If we've undone the start node, the saga is done.
             */
            live_state.mark_saga_done();
            return;
        }

        /*
         * During unwinding, a node's becoming undone means it's time to check
         * ancestor nodes to see if they're now undoable.
         */
        for parent in graph.neighbors_directed(self.node_id, Incoming) {
            if neighbors_all(graph, &parent, Outgoing, |child| {
                live_state.nodes_undone.contains_key(child)
            }) {
                /*
                 * We're ready to undo "parent".  We don't know whether it's
                 * finished running, on the todo queue, or currenting
                 * outstanding.  (It should not be on the undo queue!)
                 * TODO-design Here's an awful approach just intended to let us
                 * flesh out more of the rest of this to better understand how
                 * to manage state.
                 */
                match live_state.node_exec_state(parent) {
                    /*
                     * If the node never started or if it failed, we can
                     * just mark it undone without doing anything else.
                     */
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
                        /*
                         * If we're running an action for this task, there's
                         * nothing we can do right now, but we'll handle it when
                         * it finishes.  We could do better with queued (and
                         * there's a TODO-design in kick_off_ready() to do so),
                         * but this isn't wrong as-is.
                         */
                        continue;
                    }

                    NodeExecState::Done => {
                        /*
                         * We have to actually run the undo action.
                         */
                        live_state.queue_undo.push(parent);
                    }

                    NodeExecState::QueuedToUndo
                    | NodeExecState::UndoInProgress
                    | NodeExecState::Undone(_) => {
                        panic!(
                            "already undoing or undone node \
                            whose child was just now undone"
                        );
                    }
                }
            }
        }
    }
}

/**
 * Message sent from (tokio) task that executes an action to the executor
 * indicating that the action has completed
 */
struct TaskCompletion<UserType: SagaType> {
    /*
     * TODO-cleanup can this be removed? The node field is a SagaNode, which has
     * a node_id.
     */
    node_id: NodeIndex,
    node: Box<dyn SagaNodeRest<UserType>>,
}

/**
 * Context provided to the (tokio) task that executes an action
 */
struct TaskParams<UserType: SagaType> {
    dag: Arc<SagaDag>,
    user_context: Arc<UserType::ExecContextType>,

    /**
     * Handle to the saga's live state
     *
     * This is used only to update state for status purposes.  We want to avoid
     * any tight coupling between this task and the internal state.
     */
    live_state: Arc<Mutex<SagaExecLiveState<UserType>>>,

    /** id of the graph node whose action we're running */
    node_id: NodeIndex,
    /** channel over which to send completion message */
    done_tx: mpsc::Sender<TaskCompletion<UserType>>,
    /** Ancestor tree for this node.  See [`ActionContext`]. */
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<NodeName, Arc<serde_json::Value>>>,
    /** Saga parameters for the closest enclosing saga */
    saga_params: Arc<serde_json::Value>,
    /** The action itself that we're executing. */
    action: Arc<dyn Action<UserType>>,
}

/**
 * Executes a saga
 *
 * Call `SagaExecutor.run()` to get a Future.  You must `await` this Future to
 * actually execute the saga.
 */
/*
 * TODO Lots more could be said here, but the basic idea matches distributed
 * sagas.
 * This will be a good place to put things like concurrency limits, canarying,
 * etc.
 *
 * TODO Design note: SagaExecutor's constructors consume Arc<E> and store Arc<E>
 * to reference the user-provided context "E".  This makes it easy for us to
 * pass references to the various places that need it.  It would seem nice if
 * the constructor accepted "E" and stored that, since "E" is already Send +
 * Sync + 'static.  There are two challenges here: (1) There are a bunch of
 * other types that store a reference to E, including TaskParams and
 * ActionContext, the latter of which is exposed to the user.  These would have
 * to store &E, which would be okay, but they'd need to have annoying lifetime
 * parameters.  (2) child sagas (and so child saga executors) are a thing.
 * Since there's only one "E", the child would have to reference &E, which means
 * it would need a lifetime parameter on it _and_ that might mean it would have
 * to be a different type than SagaExecutor, even though they're otherwise the
 * same.
 */
#[derive(Debug)]
pub struct SagaExecutor<UserType: SagaType> {
    #[allow(dead_code)]
    log: slog::Logger,

    dag: Arc<SagaDag>,

    /** Channel for monitoring execution completion */
    finish_tx: broadcast::Sender<()>,

    /** Unique identifier for this saga (an execution of a saga template) */
    saga_id: SagaId,

    live_state: Arc<Mutex<SagaExecLiveState<UserType>>>,
    user_context: Arc<UserType::ExecContextType>,
}

#[derive(Debug)]
enum RecoveryDirection {
    Forward(bool),
    Unwind(bool),
}

impl<UserType: SagaType> SagaExecutor<UserType> {
    /** Create an executor to run the given saga. */
    pub fn new(
        log: slog::Logger,
        saga_id: SagaId,
        dag: Arc<SagaDag>,
        registry: Arc<ActionRegistry<UserType>>,
        user_context: Arc<UserType::ExecContextType>,
        sec_hdl: SecExecClient,
    ) -> SagaExecutor<UserType> {
        let sglog = SagaLog::new_empty(saga_id);

        /*
         * The only errors that can be returned from new_recover() are never
         * expected in this context.
         */
        SagaExecutor::new_recover(
            log,
            saga_id,
            dag,
            registry,
            user_context,
            sec_hdl,
            sglog,
        )
        .unwrap()
    }

    /**
     * Create an executor to run the given saga that may have already
     * started, using the given log events.
     */
    pub fn new_recover(
        log: slog::Logger,
        saga_id: SagaId,
        dag: Arc<SagaDag>,
        registry: Arc<ActionRegistry<UserType>>,
        user_context: Arc<UserType::ExecContextType>,
        sec_hdl: SecExecClient,
        sglog: SagaLog,
    ) -> Result<SagaExecutor<UserType>, anyhow::Error> {
        /*
         * During recovery, there's a fine line between operational errors and
         * programmer errors.  If we discover semantically invalid saga state,
         * that's an operational error that we must handle gracefully.  We use
         * lots of assertions to check invariants about our own process for
         * loading the state.  We panic if those are violated.  For example, if
         * we find that we've loaded the same node twice, that's a bug in this
         * code right here (which walks each node of the graph exactly once),
         * not a result of corrupted database state.
         */
        let forward = !sglog.unwinding();
        let mut live_state = SagaExecLiveState {
            action_registry: Arc::clone(&registry),
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
            sglog,
            injected_errors: BTreeSet::new(),
            sec_hdl,
            saga_id,
        };
        let mut loaded = BTreeSet::new();
        let graph = &dag.graph;

        /*
         * Iterate in the direction of current execution: for normal execution,
         * a standard topological sort.  For unwinding, reverse that.
         */
        let graph_nodes = {
            let mut nodes =
                toposort(&graph, None).expect("saga DAG had cycles");
            if !forward {
                nodes.reverse();
            }

            nodes
        };

        for node_id in graph_nodes {
            let node_status =
                live_state.sglog.load_status_for_node(node_id.into());

            /*
             * Validate this node's state against its parent nodes' states.  By
             * induction, this validates everything in the graph from the start
             * or end node to the current node.
             */
            for parent in graph.neighbors_directed(node_id, Incoming) {
                let parent_status =
                    live_state.sglog.load_status_for_node(parent.into());
                if !recovery_validate_parent(parent_status, node_status) {
                    return Err(anyhow!(
                        "recovery for saga {}: node {:?}: \
                        load status is \"{:?}\", which is illegal for \
                        parent load status \"{:?}\"",
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
                            /*
                             * We're recovering a node in the forward direction
                             * where all parents completed successfully.  Add it
                             * to the ready queue.
                             */
                            live_state.queue_todo.push(node_id);
                        }
                        RecoveryDirection::Unwind(true) => {
                            /*
                             * We're recovering a node in the reverse direction
                             * (unwinding) whose children have all been
                             * undone and which has never started.  Just mark
                             * it undone.
                             * TODO-design Does this suggest a better way to do
                             * this might be to simply load all the state that
                             * we have into the SagaExecLiveState and execute
                             * the saga as normal, but have normal execution
                             * check for cached values instead of running
                             * actions?  In a sense, this makes the recovery
                             * path look like the normal path rather than having
                             * the normal path look like the recovery path.  On
                             * the other hand, it seems kind of nasty to have to
                             * hold onto the recovery state for the duration.
                             * It doesn't make it a whole lot easier to test or
                             * have fewer code paths, in a real sense.  It moves
                             * those code paths to normal execution, but they're
                             * still bifurcated from the case where we didn't
                             * recover the saga.
                             */
                            live_state
                                .nodes_undone
                                .insert(node_id, UndoMode::ActionNeverRan);
                        }
                        _ => (),
                    }
                }
                SagaNodeLoadStatus::Started => {
                    /*
                     * Whether we're unwinding or not, we have to finish
                     * execution of this action.
                     */
                    live_state.queue_todo.push(node_id);
                }
                SagaNodeLoadStatus::Succeeded(output) => {
                    /*
                     * If the node has finished executing and not started
                     * undoing, and if we're unwinding and the children have
                     * all finished undoing, then it's time to undo this
                     * one.
                     */
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

                    /*
                     * If the node failed, and we're unwinding, and the children
                     * have all been undone, it's time to undo this one.
                     * But we just mark it undone -- we don't execute the
                     * undo action.
                     */
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state
                            .nodes_undone
                            .insert(node_id, UndoMode::ActionFailed);
                    }
                }
                SagaNodeLoadStatus::UndoStarted(output) => {
                    /*
                     * We know we're unwinding. (Otherwise, we should have
                     * failed validation earlier.)  Execute the undo action.
                     */
                    assert!(!forward);
                    live_state.queue_undo.push(node_id);

                    /*
                     * We still need to record the output because it's available
                     * to the undo action.
                     */
                    live_state
                        .node_outputs
                        .insert(node_id, Arc::clone(output))
                        .expect_none("recovered node twice (undo case)");
                }
                SagaNodeLoadStatus::UndoFinished => {
                    /*
                     * Again, we know we're unwinding.  We've also finished
                     * undoing this node.
                     */
                    assert!(!forward);
                    live_state
                        .nodes_undone
                        .insert(node_id, UndoMode::ActionUndone);
                }
            }

            /*
             * TODO-correctness is it appropriate to have side effects in an
             * assertion here?
             */
            assert!(loaded.insert(node_id));
        }

        /*
         * Check our done conditions.
         */
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
        })
    }

    /**
     * Builds the "ancestor tree" for a node whose dependencies have all
     * completed
     *
     * The ancestor tree for a node is a map whose keys are strings that
     * identify ancestor nodes in the graph and whose values represent the
     * outputs from those nodes.  This is used by [`ActionContext::lookup`].
     * See where we use this function in poll() for more details.
     */
    fn make_ancestor_tree(
        &self,
        tree: &mut BTreeMap<NodeName, Arc<serde_json::Value>>,
        live_state: &SagaExecLiveState<UserType>,
        node: NodeIndex,
        include_self: bool,
        ignore_depth: usize,
    ) {
        if include_self {
            self.make_ancestor_tree_node(tree, live_state, node, ignore_depth);
            return;
        }

        let ancestors = self.dag.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(
                tree,
                live_state,
                ancestor,
                ignore_depth,
            );
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<NodeName, Arc<serde_json::Value>>,
        live_state: &SagaExecLiveState<UserType>,
        node: NodeIndex,
        mut ignore_depth: usize,
    ) {
        /*
         * XXX-dap implementation notes
         *
         * What's `ignore_depth`?  Recall that the ancestor tree is used to
         * lookup outputs from previous nodes that are available to the current
         * node.  We want the behavior to be that for a given node, you cannot
         * see outputs from nodes in a subsaga on which you depended (because
         * that would tightly couple all possible subsagas -- they'd live in a
         * single namespace of node names).  But of course you _should_ be able
         * to see outputs from actions that preceded a subsaga that preceded
         * you.  To deal with this, we traverse the DAG backwards, and if we
         * find a SubsagaEnd node, then we ignore everything until we get to its
         * SubsagaStart.  To implement this, we have this `ignore_depth`: if
         * it's non-zero, we ignore any outputs we see.  We increment it on the
         * recursive call when we're in a `SubsagaEnd` node, and we decrement it
         * on the recursive call when we reach a `SubsagaStart` node.
         *
         * What about the output of a subsaga itself?  When a user appends a
         * subsaga to a Dag, they give it a "name" just like they would a node.
         * When a downstream node looks up the output of that name, they get the
         * output of that subsaga.  To implement this, we store the name in the
         * `SubsagaEnd` node.  When we find it in this traversal, if
         * ignore_depth == 0 (see above), then we add the "output of the saga".
         * This has never really been defined before, so we're defining it for
         * now to be the output of the sole ancestor of subsaga's end node.
         */
        let dag_node = self.dag.get(node).unwrap();
        let found_here = match dag_node {
            Node::Start { .. } | Node::End => None,
            Node::Constant { name, value } => {
                if ignore_depth == 0 {
                    Some((name.clone(), Arc::new(value.clone())))
                } else {
                    None
                }
            }
            Node::Action { name, .. } => {
                /*
                 * If we're in this function, it's because we're looking at the
                 * ancestor of a node that's currently "Running".  All such
                 * ancestors must be "Done".  If they had never reached "Done",
                 * then we should never have started working on the current
                 * node.  If they were "Done" but moved on to one of the undoing
                 * states, then that implies we've already finished undoing
                 * descendants, which would include the current node.
                 */
                if ignore_depth == 0 {
                    Some((name.clone(), live_state.node_output(node)))
                } else {
                    None
                }
            }
            Node::SubsagaEnd { name } => {
                /*
                 * If we find a subsaga end node, then we'll emit the output of
                 * the saga.  Plus, every node we find until we get to the next
                 * subsaga start node needs to be ignored.  (But we still
                 * continue the traversal, since there might be nodes from
                 * before the subsaga that we need to include.)
                 */
                ignore_depth = ignore_depth + 1;
                Some((name.clone(), live_state.node_output(node)))
            }

            Node::SubsagaStart { .. } => {
                if ignore_depth == 0 {
                    // We don't need to traverse any more.
                    return;
                }

                ignore_depth = ignore_depth - 1;
                None
            }
        };

        if let Some((name, output)) = found_here {
            tree.insert(name, output);
        }
        self.make_ancestor_tree(tree, live_state, node, false, ignore_depth);
    }

    /// Returns the saga parameters for the current node
    // If this node is not within a subsaga, then these will be the top-level
    // saga parameters.  We find these by walking ancestor nodes until we find
    // the Start node and we grab the parameters directly out of that node.
    //
    // If this node is contained within a subsaga, then these will be the
    // parameters of the _subsaga_.  We find these by walking ancestor nodes
    // until we find a SubsagaStart node, which will say what _other_ node's
    // output represents our parameters.  Then we grab that node's output.
    // However, if we encounter a SubsagaEnd node, then there's a nested
    // subsaga -- we need to skip _past_ the corresponding SubsagaStart.  We use
    // `ignore_depth` similar to the way `make_ancestor_tree()` does in order to
    // skip entire subsagas.
    //
    // (Note that we don't know which case we're in until we find the
    // terminating condition for one or the other.)
    fn saga_params_for(
        &self,
        live_state: &SagaExecLiveState<UserType>,
        node_index: NodeIndex,
        ignore_depth: usize,
    ) -> Arc<serde_json::Value> {
        let node = self.dag.get(node_index).unwrap();
        match node {
            Node::Start { params } => {
                assert_eq!(ignore_depth, 0);
                // XXX-dap don't clone
                Arc::new(params.clone())
            }

            Node::SubsagaStart { params_node_name, .. }
                if ignore_depth == 0 =>
            {
                // XXX-dap error conditions
                // XXX-dap this is way more expensive than we need!  Exactly one
                // of our ancestors will have the right node in its ancestor
                // tree but we don't know which one.
                let mut tree = BTreeMap::new();
                self.make_ancestor_tree(
                    &mut tree, live_state, node_index, false, 0,
                );
                Arc::clone(tree.get(params_node_name).unwrap())
            }

            Node::SubsagaStart { .. } => {
                // XXX-dap error conditions
                // It shouldn't matter which ancestor we pick.
                assert!(ignore_depth > 0);
                let ancestor = self
                    .dag
                    .graph
                    .neighbors_directed(node_index, Incoming)
                    .next()
                    .unwrap();
                self.saga_params_for(live_state, ancestor, ignore_depth - 1)
            }

            Node::End | Node::Action { .. } | Node::Constant { .. } => {
                // XXX-dap error conditions
                // It shouldn't matter which ancestor we pick.
                let ancestor = self
                    .dag
                    .graph
                    .neighbors_directed(node_index, Incoming)
                    .next()
                    .unwrap();
                self.saga_params_for(live_state, ancestor, ignore_depth)
            }

            Node::SubsagaEnd { .. } => {
                // XXX-dap error conditions
                // It shouldn't matter which ancestor we pick.
                let ancestor = self
                    .dag
                    .graph
                    .neighbors_directed(node_index, Incoming)
                    .next()
                    .unwrap();
                self.saga_params_for(live_state, ancestor, ignore_depth + 1)
            }
        }
    }

    /**
     * Simulates an error at a given node in the saga graph
     *
     * When execution reaches this node, instead of running the normal action
     * for this node, an error will be generated and processed as though the
     * action itself had produced the error.
     */
    pub async fn inject_error(&self, node_id: NodeIndex) {
        let mut live_state = self.live_state.lock().await;
        live_state.injected_errors.insert(node_id);
    }

    /**
     * Runs the saga
     *
     * This might be running a saga that has never been started before or
     * one that has been recovered from persistent state.
     */
    async fn run_saga(&self) {
        {
            /*
             * TODO-design Every SagaExec should be able to run_saga() exactly
             * once.  We don't really want to let you re-run it and get a new
             * message on finish_tx.  However, we _do_ want to handle this
             * particular case when we've recovered a "done" saga and the
             * consumer has run() it (once).
             */
            let live_state = self.live_state.lock().await;
            if live_state.exec_state == SagaCachedState::Done {
                self.finish_tx.send(()).expect("failed to send finish message");
                live_state.sec_hdl.saga_update(SagaCachedState::Done).await;
                return;
            }
        }

        /*
         * Allocate the channel used for node tasks to tell us when they've
         * completed.  In practice, each node can enqueue only two messages in
         * its lifetime: one for completion of the action, and one for
         * completion of the compensating action.  We bound this channel's size
         * at twice the graph node count for this worst case.
         */
        let (tx, mut rx) = mpsc::channel(2 * self.dag.graph.node_count());

        loop {
            self.kick_off_ready(&tx).await;

            /*
             * Process any messages available on our channel.
             * It shouldn't be possible to get None back here.  That would mean
             * that all of the consumers have closed their ends, but we still
             * have a consumer of our own in "tx".
             * TODO-robustness Can we assert that there are outstanding tasks
             * when we block on this channel?
             */
            let message = rx.next().await.expect("broken tx");
            let task = {
                let mut live_state = self.live_state.lock().await;
                live_state.node_task_done(message.node_id)
            };

            /*
             * This should really not take long, as there's nothing else this
             * task does after sending the message that we just received.  It's
             * good to wait here to make sure things are cleaned up.
             * TODO-robustness can we enforce that this won't take long?
             */
            task.await.expect("node task failed unexpectedly");

            let mut live_state = self.live_state.lock().await;
            let prev_state = live_state.exec_state;
            message.node.propagate(&self, &mut live_state);
            /*
             * TODO-cleanup This condition ought to be simplified.  We want to
             * update the saga state when we become Unwinding (which we do here)
             * and when we become Done (which we do below).  There may be a
             * better place to put this logic that's less ad hoc.
             */
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
        live_state.sec_hdl.saga_update(SagaCachedState::Done).await;
    }

    /*
     * Kick off any nodes that are ready to run.  (Right now, we kick off
     * everything, so it might seem unnecessary to store this vector in
     * "self" to begin with.  However, the intent is to add capacity limits,
     * in which case we may return without having scheduled everything, and
     * we want to track whatever's still ready to go.)
     * TODO revisit dance with the vec to satisfy borrow rules
     * TODO implement unwinding
     */
    async fn kick_off_ready(
        &self,
        tx: &mpsc::Sender<TaskCompletion<UserType>>,
    ) {
        let mut live_state = self.live_state.lock().await;

        /*
         * TODO is it possible to deadlock with a concurrency limit given that
         * we always do "todo" before "undo"?
         */

        let todo_queue = live_state.queue_todo.clone();
        live_state.queue_todo = Vec::new();

        for node_id in todo_queue {
            /*
             * TODO-design It would be good to check whether the saga is
             * unwinding, and if so, whether this action has ever started
             * running before.  If not, then we can send this straight to
             * undoing without doing any more work here.  What we're
             * doing here should also be safe, though.  We run the action
             * regardless, and when we complete it, we'll undo it.
             */
            /*
             * TODO we could be much more efficient without copying this tree
             * each time.
             */
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                false,
                0,
            );

            let saga_params = self.saga_params_for(&live_state, node_id, 0);
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
            /*
             * TODO commonize with code above
             * TODO we could be much more efficient without copying this tree
             * each time.
             */
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                true,
                0,
            );

            let saga_params = self.saga_params_for(&live_state, node_id, 0);
            let sgaction = self.node_action(&live_state, node_id);
            let task_params = TaskParams {
                dag: Arc::clone(&self.dag),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                saga_params,
                action: sgaction,
                user_context: Arc::clone(&self.user_context),
            };

            let task = tokio::spawn(SagaExecutor::undo_node(task_params));
            live_state.node_task(node_id, task);
        }
    }

    // XXX-dap if the action registry were hanging off self instead of
    // live_state, we could just accept "self" here.
    // XXX-dap dag, action registry should not be in live_state
    fn node_action(
        &self,
        live_state: &SagaExecLiveState<UserType>,
        node_index: NodeIndex,
    ) -> Arc<dyn Action<UserType>> {
        let registry = &live_state.action_registry;
        let dag = &self.dag;
        match dag.get(node_index).unwrap() {
            Node::Action { action_name: action, .. } => {
                registry.get(action).expect("missing action for node")
            }

            Node::Constant { value, .. } => {
                Arc::new(ActionConstant::new(value.clone()))
            }

            Node::Start { .. } | Node::End | Node::SubsagaStart { .. } => {
                // These nodes are no-ops in terms of the action that happens at
                // the node itself.
                Arc::new(ActionConstant::new(serde_json::Value::Null))
            }

            Node::SubsagaEnd { .. } => {
                // We record the subsaga's output here, as the output of the
                // `SubsagaEnd` node.  (We don't _have_ to do this -- we could
                // instead change the logic that _finds_ the saga's output to
                // look in the same place that we do here.  But this is a simple
                // and clear way to do this.)
                // XXX-dap What is the saga's output?  We have not previously
                // defined that.  For now, we'll take the first immediate
                // ancestor's output.  This is a little janky.  We should
                // probably not explode if there's more than one, and really we
                // shouldn't allow you to create that graph in the first place.
                // DagBuilder::build() could return an error in this case.
                // (There are other error cases it could check too, like adding
                // a Subsaga whose params node does not precede it.) Still, we
                // can't be sure a previous version didn't do that (or the
                // database state isn't corrupt) so we shouldn't blow up.
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

    /**
     * Body of a (tokio) task that executes an action.
     */
    async fn exec_node(task_params: TaskParams<UserType>) {
        let node_id = task_params.node_id;

        {
            /*
             * TODO-liveness We don't want to hold this lock across a call
             * to the database.  It's fair to say that if the database
             * hangs, the saga's corked anyway, but we should at least be
             * able to view its state, and we can't do that with this
             * design.
             */
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
                | SagaNodeLoadStatus::UndoFinished => {
                    panic!("starting node in bad state")
                }
            }
        }

        let exec_future = task_params.action.do_it(ActionContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            saga_params: Arc::clone(&task_params.saga_params),
            node_id,
            dag: Arc::clone(&task_params.dag),
            user_context: Arc::clone(&task_params.user_context),
        });
        let result = exec_future.await;
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

    /**
     * Body of a (tokio) task that executes a compensation action.
     */
    /*
     * TODO-cleanup This has a lot in common with exec_node(), but enough
     * different that it doesn't make sense to parametrize that one.  Still, it
     * sure would be nice to clean this up.
     */
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
                | SagaNodeLoadStatus::UndoFinished => {
                    panic!("undoing node in bad state")
                }
            }
        }

        let exec_future = task_params.action.undo_it(ActionContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            saga_params: Arc::clone(&task_params.saga_params),
            node_id,
            dag: Arc::clone(&task_params.dag),
            user_context: Arc::clone(&task_params.user_context),
        });
        /*
         * TODO-robustness We have to figure out what it means to fail here and
         * what we want to do about it.
         */
        exec_future.await.unwrap();
        let node = Box::new(SagaNode {
            node_id,
            state: SgnsUndone(UndoMode::ActionUndone),
        });
        SagaExecutor::finish_task(task_params, node).await;
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

    /*
     * TODO-design Today, callers that invoke run() maintain a handle to the
     * SagaExec so that they can control and check the status of execution.
     * But ideally, once a caller has invoked run(), they wouldn't be able to
     * call it again; and ideally, they wouldn't be able to get the result of
     * the saga until run() had finished.  One way we might do this is to
     * have run() consume the WfExec, return immediately an object that can be
     * used only for status and control, and provide a method on that object
     * that turns into the result.
     */
    /**
     * Runs the saga to completion asynchronously
     */
    pub fn run(&self) -> impl Future<Output = ()> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_saga().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    /**
     * Returns a [`SagaResult`] describing the result of the saga, including
     * data produced by its actions.
     *
     * # Panics
     *
     * If the saga has not yet completed.
     */
    pub fn result(&self) -> SagaResult {
        /*
         * TODO-cleanup is there a way to make this safer?  If we could know
         * that there were no other references to the live_state (which should
         * be true, if we're done), then we could consume it, as well as "self",
         * and avoid several copies below.
         */
        let live_state = self
            .live_state
            .try_lock()
            .expect("attempted to get result while saga still running?");
        assert_eq!(live_state.exec_state, SagaCachedState::Done);

        if live_state.nodes_undone.contains_key(&self.dag.start_node) {
            assert!(live_state.nodes_undone.contains_key(&self.dag.end_node));

            /*
             * Choosing the first node_id in node_errors will find the
             * topologically-first node that failed.  This may not be the one
             * that actually triggered the saga to fail, but it could have done
             * so.  (That is, if there were another action that failed that
             * triggered the saga to fail, this one did not depend on it, so it
             * could as well have happened in the other order.)
             */
            let (error_node_id, error_source) =
                live_state.node_errors.iter().next().unwrap();
            // XXX-dap error condition
            let error_node_name = self
                .dag
                .get(*error_node_id)
                .unwrap()
                .node_name()
                .unwrap()
                .clone();
            return SagaResult {
                saga_id: self.saga_id,
                saga_log: live_state.sglog.clone(),
                kind: Err(SagaResultErr {
                    error_node_name,
                    error_source: error_source.clone(),
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

        SagaResult {
            saga_id: self.saga_id,
            saga_log: live_state.sglog.clone(),
            kind: Ok(SagaResultOk { node_outputs }),
        }
    }

    pub fn status(&self) -> BoxFuture<'_, SagaExecStatus> {
        async move {
            let live_state = self.live_state.lock().await;

            /*
             * First, determine the maximum depth of each node.  Use this to
             * figure out the list of nodes at each level of depth.  This
             * information is used to figure out where to print each node's
             * status vertically.
             */
            let mut max_depth_of_node: BTreeMap<NodeIndex, usize> =
                BTreeMap::new();
            max_depth_of_node.insert(self.dag.start_node, 0);
            let mut nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>> =
                BTreeMap::new();
            let mut node_exec_states = BTreeMap::new();

            let graph = &self.dag.graph;
            let topo_visitor = Topo::new(graph);
            for node in topo_visitor.iter(graph) {
                /* Record the current execution state for this node. */
                node_exec_states.insert(node, live_state.node_exec_state(node));

                /*
                 * Compute the node's depth.  This must be 0 (already stored
                 * above) for the root node and we don't care about doing this
                 * for the end node.
                 */
                if let Some(d) = max_depth_of_node.get(&node) {
                    assert_eq!(*d, 0);
                    assert_eq!(node, self.dag.start_node);
                    assert_eq!(max_depth_of_node.len(), 1);
                    continue;
                }

                /*
                 * We don't want to include the end node because we don't want
                 * to try to print it later.  This should always be the last
                 * iteration of the loop.
                 */
                if node == self.dag.end_node {
                    continue;
                }

                let mut max_parent_depth: Option<usize> = None;
                for p in graph.neighbors_directed(node, Incoming) {
                    let parent_depth = *max_depth_of_node.get(&p).unwrap();
                    match max_parent_depth {
                        Some(x) if x >= parent_depth => (),
                        _ => max_parent_depth = Some(parent_depth),
                    };
                }

                let depth = max_parent_depth.unwrap() + 1;
                max_depth_of_node.insert(node, depth);

                nodes_at_depth.entry(depth).or_insert_with(Vec::new).push(node);
            }

            SagaExecStatus {
                saga_id: self.saga_id,
                dag: Arc::clone(&self.dag),
                nodes_at_depth,
                node_exec_states,
                sglog: live_state.sglog.clone(),
            }
        }
        .boxed()
    }
}

/**
 * Encapsulates the (mutable) execution state of a saga
 */
/*
 * This is linked to a `SagaExecutor` and protected by a Mutex.  The state is
 * mainly modified by [`SagaExecutor::run_saga`].  We may add methods for
 * controlling the saga (e.g., pausing), which would modify this as well.
 * We also intend to add methods for viewing saga state, which will take the
 * lock to read state.
 *
 * If the view of a saga were just (1) that it's running, and maybe (2) a
 * set of outstanding actions, then we might take a pretty different approach
 * here.  We might create a read-only view object that's populated periodically
 * by the saga executor.  This still might be the way to go, but at the
 * moment we anticipate wanting pretty detailed debug information (like what
 * outputs were produced by what steps), so the view would essentially be a
 * whole copy of this object.
 * TODO This would be a good place for a debug log.
 */
#[derive(Debug)]
struct SagaExecLiveState<UserType: SagaType> {
    action_registry: Arc<ActionRegistry<UserType>>,

    /** Unique identifier for this saga (an execution of a saga template) */
    saga_id: SagaId,

    sec_hdl: SecExecClient,

    /** Overall execution state */
    exec_state: SagaCachedState,

    /** Queue of nodes that have not started but whose deps are satisfied */
    queue_todo: Vec<NodeIndex>,
    /** Queue of nodes whose undo action needs to be run. */
    queue_undo: Vec<NodeIndex>,

    /** Outstanding tokio tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,

    /** Outputs saved by completed actions. */
    node_outputs: BTreeMap<NodeIndex, Arc<serde_json::Value>>,
    /** Set of undone nodes. */
    nodes_undone: BTreeMap<NodeIndex, UndoMode>,
    /** Errors produced by failed actions. */
    node_errors: BTreeMap<NodeIndex, ActionError>,

    /** Persistent state */
    sglog: SagaLog,

    /** Injected errors */
    injected_errors: BTreeSet<NodeIndex>,
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
        })
    }
}

impl<UserType: SagaType> SagaExecLiveState<UserType> {
    /*
     * TODO-design The current implementation does not use explicit state.  In
     * most cases, this made things better than before because each hunk of code
     * was structured to accept only nodes in states that were valid.  But
     * there are a few cases where we need a bit more state than we're currently
     * keeping.  This function is used there.
     *
     * It's especially questionable to use load_status here -- or is that the
     * way we should go more generally?  See TODO-design in new_recover().
     */
    fn node_exec_state(&self, node_id: NodeIndex) -> NodeExecState {
        /*
         * This seems like overkill but it seems helpful to validate state.
         */
        let mut set: BTreeSet<NodeExecState> = BTreeSet::new();
        let load_status = self.sglog.load_status_for_node(node_id.into());
        if let Some(undo_mode) = self.nodes_undone.get(&node_id) {
            set.insert(NodeExecState::Undone(*undo_mode));
        } else if self.queue_undo.contains(&node_id) {
            set.insert(NodeExecState::QueuedToUndo);
        } else if let SagaNodeLoadStatus::Failed(_) = load_status {
            assert!(self.node_errors.contains_key(&node_id));
            set.insert(NodeExecState::Failed);
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
            let the_state = set.into_iter().last().unwrap();
            the_state
        }
    }

    fn mark_saga_done(&mut self) {
        assert!(self.queue_todo.is_empty());
        assert!(self.queue_undo.is_empty());
        assert!(
            self.exec_state == SagaCachedState::Running
                || self.exec_state == SagaCachedState::Unwinding
        );
        self.exec_state = SagaCachedState::Done;
    }

    fn node_task(&mut self, node_id: NodeIndex, task: JoinHandle<()>) {
        self.node_tasks.insert(node_id, task);
    }

    fn node_task_done(&mut self, node_id: NodeIndex) -> JoinHandle<()> {
        self.node_tasks
            .remove(&node_id)
            .expect("processing task completion with no task present")
    }

    fn node_output(&self, node_id: NodeIndex) -> Arc<serde_json::Value> {
        let output =
            self.node_outputs.get(&node_id).expect("node has no output");
        Arc::clone(output)
    }
}

/**
 * Summarizes the final state of a saga execution
 */
#[derive(Clone, Debug)]
pub struct SagaResult {
    pub saga_id: SagaId,
    pub saga_log: SagaLog,
    pub kind: Result<SagaResultOk, SagaResultErr>,
}

/**
 * Provides access to outputs from a saga that completed successfully
 */
#[derive(Clone, Debug)]
pub struct SagaResultOk {
    node_outputs: BTreeMap<NodeName, Arc<serde_json::Value>>,
}

impl SagaResultOk {
    /**
     * Returns the data produced by a node in the saga.
     *
     * # Panics
     *
     * If the saga has no node called `name`.
     */
    pub fn lookup_output<T: ActionData + 'static>(
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
            .map_err(ActionError::new_deserialize)
    }
}

/**
 * Provides access to failure details for a saga that failed
 *
 * When a saga fails, it's always one action's failure triggers failure of the
 * saga.  It's possible that other actions also failed, but only if they were
 * running concurrently.  This structure represents one of these errors, any of
 * which could have caused the saga to fail, depending on the order in which
 * they completed.
 */
/*
 * TODO-coverage We should test that sagas do the right thing when two actions
 * fail concurrently.
 *
 * We don't allow callers to access outputs from a saga that failed
 * because it's not obvious yet why this would be useful and it's too
 * easy to shoot yourself in the foot by not checking whether the saga
 * failed.  In practice, the enum that wraps this type ensures that the caller
 * has checked for failure, so it wouldn't be unreasonable to provide outputs
 * here.  (A strong case: there are cases where it's useful to get outputs even
 * while the saga is running, as might happen for a saga that generates a
 * database record whose id you want to return to a client without waiting for
 * the saga to complete.  It's silly to let you get this id while the saga is
 * running, but not after it's failed.)
 */
#[derive(Clone, Debug)]
pub struct SagaResultErr {
    pub error_node_name: NodeName,
    pub error_source: ActionError,
}

/**
 * Summarizes in-progress execution state of a saga
 */
#[derive(Clone, Debug)]
pub struct SagaExecStatus {
    saga_id: SagaId,
    nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>>,
    node_exec_states: BTreeMap<NodeIndex, NodeExecState>,
    dag: Arc<SagaDag>,
    sglog: SagaLog,
}

impl fmt::Display for SagaExecStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}

impl SagaExecStatus {
    pub fn log(&self) -> &SagaLog {
        &self.sglog
    }

    fn print(
        &self,
        out: &mut fmt::Formatter<'_>,
        indent_level: usize,
    ) -> fmt::Result {
        let big_indent = indent_level * 16;
        write!(
            out,
            "{:width$}+ saga execution: {}\n",
            "",
            self.saga_id,
            width = big_indent
        )?;
        for (d, nodes) in &self.nodes_at_depth {
            write!(
                out,
                "{:width$}+-- stage {:>2}: ",
                "",
                d,
                width = big_indent
            )?;
            let mklabel = |node| {
                let node = self.dag.get(node).unwrap();
                if let Some(name) = node.node_name() {
                    format!("{} (produces {:?})", node.label(), name)
                } else {
                    node.label()
                }
            };
            if nodes.len() == 1 {
                let node = nodes[0];
                let node_label = mklabel(nodes[0]);
                let node_state = &self.node_exec_states[&node];
                write!(out, "{}: {}\n", node_state, node_label)?;
            } else {
                write!(out, "+ (actions in parallel)\n")?;
                for node in nodes {
                    let node_label = mklabel(*node);
                    let node_state = &self.node_exec_states[&node];
                    // TODO(AJS) - fix this when subsaga nodes added
                    //let child_sagas = self.child_sagas.get(&node);
                    //let subsaga_char =
                    //    if child_sagas.is_some() { '+' } else { '-' };
                    let subsaga_char = '-';

                    write!(
                        out,
                        "{:width$}{:>14}+-{} {}: {}\n",
                        "",
                        "",
                        subsaga_char,
                        node_state,
                        node_label,
                        width = big_indent
                    )?;

                    //if let Some(sagas) = child_sagas {
                    //  for c in sagas {
                    //    c.print(out, indent_level + 1)?;
                    //}
                    //}
                }
            }
        }

        Ok(())
    }
}

/* TODO */
fn neighbors_all<F>(
    graph: &Graph<Node, ()>,
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

/**
 * Returns true if the parent node's load status is valid for the given child
 * node's load status.
 */
fn recovery_validate_parent(
    parent_status: &SagaNodeLoadStatus,
    child_status: &SagaNodeLoadStatus,
) -> bool {
    match child_status {
        /*
         * If the child node has started, finished successfully, or even started
         * undoing, the only allowed status for the parent node is "done".  The
         * states prior to "done" are ruled out because we execute nodes in
         * dependency order.  "failed" is ruled out because we do not execute
         * nodes whose parents failed.  The undoing states are ruled out because
         * we unwind in reverse-dependency order, so we cannot have started
         * undoing the parent if the child node has not finished undoing.  (A
         * subtle but important implementation detail is that we do not undo a
         * node that has not started execution.  If we did, then the "undo
         * started" load state could be associated with a parent that failed.)
         */
        SagaNodeLoadStatus::Started
        | SagaNodeLoadStatus::Succeeded(_)
        | SagaNodeLoadStatus::UndoStarted(_) => {
            matches!(parent_status, SagaNodeLoadStatus::Succeeded(_))
        }

        /*
         * If the child node has failed, this looks just like the previous case,
         * except that the parent node could be UndoStarted or UndoFinished.
         * That's possible because we don't undo a failed node, so after undoing
         * the parents, the log state would still show "failed".
         */
        SagaNodeLoadStatus::Failed(_) => {
            matches!(
                parent_status,
                SagaNodeLoadStatus::Succeeded(_)
                    | SagaNodeLoadStatus::UndoStarted(_)
                    | SagaNodeLoadStatus::UndoFinished
            )
        }

        /*
         * If we've finished undoing the child node, then the parent must be
         * either "done" or one of the undoing states.
         */
        SagaNodeLoadStatus::UndoFinished => matches!(
            parent_status,
            SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::UndoStarted(_)
                | SagaNodeLoadStatus::UndoFinished
        ),

        /*
         * If a node has never started, the only illegal states for a parent are
         * those associated with undoing, since the child must be undone first.
         */
        SagaNodeLoadStatus::NeverStarted => matches!(
            parent_status,
            SagaNodeLoadStatus::NeverStarted
                | SagaNodeLoadStatus::Started
                | SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::Failed(_)
        ),
    }
}

/**
 * Action's handle to the saga subsystem
 */
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
    /**
     * Retrieves a piece of data stored by a previous (ancestor) node in the
     * current saga.  The data is identified by `name`, the name of the ancestor
     * node.
     *
     * # Panics
     *
     * This function panics if there was no data previously stored with name
     * `name` (which means there was no ancestor node with that name).
     */
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
            .map_err(ActionError::new_deserialize)
    }

    /**
     * Returns the saga parameters for the current action
     *
     * If this action is being run as a subsaga, this returns the saga
     * parameters for the subsaga.  This way actions don't have to care whether
     * they're running in a saga or not.
     */
    pub fn saga_params<T: ActionData + 'static>(
        &self,
    ) -> Result<T, ActionError> {
        serde_json::from_value((*self.saga_params).clone())
            .map_err(ActionError::new_deserialize)
    }

    /**
     * Returns the human-readable label for the current saga node
     */
    pub fn node_label(&self) -> String {
        self.dag.get(self.node_id).unwrap().label()
    }

    /**
     * Returns the consumer-provided context for the current saga
     */
    pub fn user_data(&self) -> &UserType::ExecContextType {
        &self.user_context
    }
}

/**
 * Converts a NodeIndex (used by the graph representation to identify a node) to
 * a [`SagaNodeId`] (used elsewhere in this module to identify a node)
 */
impl From<NodeIndex> for SagaNodeId {
    fn from(node_id: NodeIndex) -> SagaNodeId {
        /*
         * We (must) verify elsewhere that node indexes fit within a u32.
         */
        SagaNodeId::from(u32::try_from(node_id.index()).unwrap())
    }
}

/**
 * Wrapper for SagaLog.record_now() that maps internal node indexes to
 * stable node ids.
 */
// TODO Consider how we do map internal node indexes to stable node ids.
// TODO clean up this interface
async fn record_now<T>(
    live_state: &mut SagaExecLiveState<T>,
    node: NodeIndex,
    event_type: SagaNodeEventType,
) where
    T: SagaType,
{
    let saga_id = live_state.saga_id;
    let node_id = node.into();

    /*
     * The only possible failure here today is attempting to record an event
     * that's illegal for the current node state.  That's a bug in this
     * program.
     */
    let event = SagaNodeEvent { saga_id, node_id, event_type };
    live_state.sglog.record(&event).unwrap();
    live_state.sec_hdl.record(event).await;
}

/**
 * Consumer's handle for querying and controlling the execution of a single saga
 */
pub trait SagaExecManager: fmt::Debug + Send + Sync {
    /** Run the saga to completion. */
    fn run(&self) -> BoxFuture<'_, ()>;
    /**
     * Return the result of the saga
     *
     * The returned [`SagaResult`] has interfaces for querying saga outputs and
     * error information.
     *
     * # Panics
     *
     * If the saga has not finished when this function is called.
     */
    fn result(&self) -> SagaResult;

    /**
     * Returns fine-grained information about saga execution
     */
    fn status(&self) -> BoxFuture<'_, SagaExecStatus>;

    /**
     * Replaces the action at the specified node with one that just generates an
     * error
     *
     * See [`Dag::get_index()`] to get the node_id for a node.
     */
    fn inject_error(&self, node_id: NodeIndex) -> BoxFuture<'_, ()>;
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
}
