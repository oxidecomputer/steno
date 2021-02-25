//! Manages execution of a saga

use crate::rust_features::ExpectNone;
use crate::saga_action_error::ActionError;
use crate::saga_action_generic::Action;
use crate::saga_action_generic::ActionData;
use crate::saga_action_generic::ActionInjectError;
use crate::saga_log::SagaNodeEventType;
use crate::saga_log::SagaNodeLoadStatus;
use crate::saga_template::SagaId;
use crate::saga_template::SagaTemplateMetadata;
use crate::SagaLog;
use crate::SagaTemplate;
use crate::SagaType;
use anyhow::anyhow;
use core::fmt::Debug;
use core::future::Future;
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
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/*
 * TODO-design Should we go even further and say that each node is its own
 * struct with incoming channels from parents (to notify when done), from
 * children (to notify when undone), and to each direction as well?  Then the
 * whole thing is a message passing exercise?
 */
struct SgnsDone(Arc<JsonValue>);
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
        let graph = &exec.saga_metadata.graph;
        assert!(!live_state.node_errors.contains_key(&self.node_id));
        live_state
            .node_outputs
            .insert(self.node_id, Arc::clone(&self.state.0))
            .expect_none("node finished twice (storing output)");

        if self.node_id == exec.saga_metadata.end_node {
            /*
             * If we've completed the last node, the saga is done.
             */
            assert_eq!(live_state.exec_state, SagaState::Running);
            assert_eq!(graph.node_count(), live_state.node_outputs.len());
            live_state.mark_saga_done();
            return;
        }

        if live_state.exec_state == SagaState::Unwinding {
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
        let graph = &exec.saga_metadata.graph;
        assert!(!live_state.node_outputs.contains_key(&self.node_id));
        live_state
            .node_errors
            .insert(self.node_id, self.state.0.clone())
            .expect_none("node finished twice (storing error)");

        if live_state.exec_state == SagaState::Unwinding {
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
            live_state.exec_state = SagaState::Unwinding;
            assert_ne!(self.node_id, exec.saga_metadata.end_node);
            let new_node = SagaNode {
                node_id: exec.saga_metadata.end_node,
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
        let graph = &exec.saga_metadata.graph;
        assert_eq!(live_state.exec_state, SagaState::Unwinding);
        live_state
            .nodes_undone
            .insert(self.node_id, self.state.0)
            .expect_none("node already undone");

        if self.node_id == exec.saga_metadata.start_node {
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
                match live_state.node_exec_state(&parent) {
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

                    NodeExecState::QueuedToUndo | NodeExecState::Undone(_) => {
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
 * Execution state for the saga overall
 */
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SagaState {
    Running,
    Unwinding,
    Done,
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
    saga_metadata: Arc<SagaTemplateMetadata>,
    user_context: Arc<UserType::ExecContextType>,
    user_saga_params: Arc<UserType::SagaParamsType>,

    /**
     * Handle to the saga's live state
     *
     * This is used only to update state for status purposes.  We want to avoid
     * any tight coupling between this task and the internal state.
     */
    live_state: Arc<Mutex<SagaExecLiveState<UserType>>>,

    // TODO-cleanup should not need a copy here.
    creator: String,

    /** id of the graph node whose action we're running */
    node_id: NodeIndex,
    /** channel over which to send completion message */
    done_tx: mpsc::Sender<TaskCompletion<UserType>>,
    /** Ancestor tree for this node.  See [`ActionContext`]. */
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
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
 * ActionContext, the latter of which is exposed to the user.  These would have to
 * store &E, which would be okay, but they'd need to have annoying lifetime
 * parameters.  (2) child sagas (and so child saga executors) are a thing.
 * Since there's only one "E", the child would have to reference &E, which means
 * it would need a lifetime parameter on it _and_ that might mean it would have
 * to be a different type than SagaExecutor, even though they're otherwise the
 * same.
 */
#[derive(Debug)]
pub struct SagaExecutor<UserType: SagaType> {
    // TODO This could probably be a reference instead.
    saga_template: Arc<SagaTemplate<UserType>>,
    saga_metadata: Arc<SagaTemplateMetadata>,

    creator: String,

    /** Channel for monitoring execution completion */
    finish_tx: broadcast::Sender<()>,

    /** Unique identifier for this saga (an execution of a saga template) */
    saga_id: SagaId,

    live_state: Arc<Mutex<SagaExecLiveState<UserType>>>,
    user_context: Arc<UserType::ExecContextType>,
    user_saga_params: Arc<UserType::SagaParamsType>,
}

#[derive(Debug)]
enum RecoveryDirection {
    Forward(bool),
    Unwind(bool),
}

impl<UserType: SagaType> SagaExecutor<UserType> {
    /** Create an executor to run the given saga. */
    pub fn new(
        saga_id: &SagaId,
        saga_template: Arc<SagaTemplate<UserType>>,
        creator: &str,
        user_context: Arc<UserType::ExecContextType>,
        user_saga_params: UserType::SagaParamsType,
    ) -> Result<SagaExecutor<UserType>, ActionError> {
        let serialized_params = serde_json::to_value(&user_saga_params)
            .map_err(ActionError::new_serialize)?;
        let sglog = SagaLog::new(creator, saga_id, serialized_params);

        /*
         * It would seem simpler to propagate the result from `new_recover()`,
         * but the reality is that the only errors that can be returned are not
         * expected in this context.
         */
        Ok(SagaExecutor::new_recover(
            saga_template,
            sglog,
            creator,
            user_context,
        )
        .unwrap())
    }

    /**
     * Create an executor to run the given saga that may have already
     * started, using the given log events.
     */
    pub fn new_recover(
        saga_template: Arc<SagaTemplate<UserType>>,
        sglog: SagaLog,
        creator: &str,
        user_context: Arc<UserType::ExecContextType>,
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
        let saga_id = sglog.saga_id;
        let forward = !sglog.unwinding;
        let user_saga_params =
            Arc::new(sglog.params_as::<UserType::SagaParamsType>()?);
        let mut live_state = SagaExecLiveState {
            saga_template: Arc::clone(&saga_template),
            saga_metadata: Arc::new(saga_template.metadata().clone()),
            exec_state: if forward {
                SagaState::Running
            } else {
                SagaState::Unwinding
            },
            queue_todo: Vec::new(),
            queue_undo: Vec::new(),
            node_tasks: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            nodes_undone: BTreeMap::new(),
            node_errors: BTreeMap::new(),
            child_sagas: BTreeMap::new(),
            sglog: sglog,
            injected_errors: BTreeSet::new(),
        };
        let mut loaded = BTreeSet::new();
        let saga_metadata = Arc::clone(&live_state.saga_metadata);
        let graph = &saga_metadata.graph;

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
                live_state.sglog.load_status_for_node(node_id.index() as u64);

            /*
             * Validate this node's state against its parent nodes' states.  By
             * induction, this validates everything in the graph from the start
             * or end node to the current node.
             */
            for parent in graph.neighbors_directed(node_id, Incoming) {
                let parent_status = live_state
                    .sglog
                    .load_status_for_node(parent.index() as u64);
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
        if live_state.node_outputs.contains_key(&saga_metadata.end_node)
            || live_state.nodes_undone.contains_key(&saga_metadata.start_node)
        {
            live_state.mark_saga_done();
        }

        let (finish_tx, _) = broadcast::channel(1);

        Ok(SagaExecutor {
            saga_template,
            saga_metadata,
            user_context,
            user_saga_params,
            creator: creator.to_owned(),
            saga_id,
            finish_tx,
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
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
        live_state: &SagaExecLiveState<UserType>,
        node: NodeIndex,
        include_self: bool,
    ) {
        if include_self {
            self.make_ancestor_tree_node(tree, live_state, node);
            return;
        }

        let ancestors =
            self.saga_metadata.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, live_state, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
        live_state: &SagaExecLiveState<UserType>,
        node: NodeIndex,
    ) {
        if node == self.saga_metadata.start_node {
            return;
        }

        /*
         * If we're in this function, it's because we're looking at the ancestor
         * of a node that's currently "Running".  All such ancestors must be
         * "Done".  If they had never reached "Done", then we should never have
         * started working on the current node.  If they were "Done" but moved
         * on to one of the undoing states, then that implies we've already
         * finished undoing descendants, which would include the current node.
         */
        let name = self.saga_metadata.node_name(&node).unwrap().to_owned();
        let output = live_state.node_output(node);
        tree.insert(name, output);
        self.make_ancestor_tree(tree, live_state, node, false);
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
            if live_state.exec_state == SagaState::Done {
                self.finish_tx.send(()).expect("failed to send finish message");
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
        let (tx, mut rx) =
            mpsc::channel(2 * self.saga_metadata.graph.node_count());

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
            message.node.propagate(&self, &mut live_state);
            if live_state.exec_state == SagaState::Done {
                break;
            }
        }

        let live_state = self.live_state.try_lock().unwrap();
        assert_eq!(live_state.exec_state, SagaState::Done);
        self.finish_tx.send(()).expect("failed to send finish message");
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
            );

            let sgaction = if live_state.injected_errors.contains(&node_id) {
                Arc::new(ActionInjectError {}) as Arc<dyn Action<UserType>>
            } else {
                Arc::clone(
                    live_state
                        .saga_template
                        .launchers
                        .get(&node_id)
                        .expect("missing action for node"),
                )
            };

            let task_params = TaskParams {
                saga_metadata: Arc::clone(&self.saga_metadata),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: sgaction,
                creator: self.creator.clone(),
                user_context: Arc::clone(&self.user_context),
                user_saga_params: Arc::clone(&self.user_saga_params),
            };

            let task = tokio::spawn(SagaExecutor::exec_node(task_params));
            live_state.node_task(node_id, task);
        }

        if live_state.exec_state == SagaState::Running {
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
            );

            let sgaction = live_state
                .saga_template
                .launchers
                .get(&node_id)
                .expect("missing action for node");

            let task_params = TaskParams {
                saga_metadata: Arc::clone(&self.saga_metadata),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: Arc::clone(sgaction),
                creator: self.creator.clone(),
                user_context: Arc::clone(&self.user_context),
                user_saga_params: Arc::clone(&self.user_saga_params),
            };

            let task = tokio::spawn(SagaExecutor::undo_node(task_params));
            live_state.node_task(node_id, task);
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
                live_state.sglog.load_status_for_node(node_id.index() as u64);
            match load_status {
                SagaNodeLoadStatus::NeverStarted => {
                    record_now(
                        &mut live_state.sglog,
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
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            saga_metadata: Arc::clone(&task_params.saga_metadata),
            creator: task_params.creator.clone(),
            user_context: Arc::clone(&task_params.user_context),
            user_saga_params: Arc::clone(&task_params.user_saga_params),
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
                live_state.sglog.load_status_for_node(node_id.index() as u64);
            match load_status {
                SagaNodeLoadStatus::Succeeded(_) => {
                    record_now(
                        &mut live_state.sglog,
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
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            saga_metadata: Arc::clone(&task_params.saga_metadata),
            creator: task_params.creator.clone(),
            user_context: Arc::clone(&task_params.user_context),
            user_saga_params: Arc::clone(&task_params.user_saga_params),
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
            record_now(&mut live_state.sglog, node_id, event_type).await;
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
        assert_eq!(live_state.exec_state, SagaState::Done);

        if live_state.nodes_undone.contains_key(&self.saga_metadata.start_node)
        {
            assert!(live_state
                .nodes_undone
                .contains_key(&self.saga_metadata.end_node));

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
            let error_node_name = self
                .saga_metadata
                .node_name(error_node_id)
                .unwrap()
                .to_string();
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
                let start_node = &self.saga_metadata.start_node;
                let end_node = &self.saga_metadata.end_node;
                if *node_id == *start_node || *node_id == *end_node {
                    None
                } else {
                    let node_name = self
                        .saga_metadata
                        .node_name(node_id)
                        .unwrap()
                        .to_owned();
                    Some((node_name, Arc::clone(node_output)))
                }
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
            max_depth_of_node.insert(self.saga_metadata.start_node, 0);
            let mut nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>> =
                BTreeMap::new();
            let mut node_exec_states = BTreeMap::new();
            let mut child_sagas = BTreeMap::new();

            let graph = &self.saga_metadata.graph;
            let topo_visitor = Topo::new(graph);
            for node in topo_visitor.iter(graph) {
                /* Record the current execution state for this node. */
                node_exec_states
                    .insert(node, live_state.node_exec_state(&node));

                /*
                 * If there's a child saga for this node, construct its status.
                 */
                if let Some(child_execs) = live_state.child_sagas.get(&node) {
                    /*
                     * TODO-correctness check lock order.  This seems okay
                     * because we always take locks from parent -> child and
                     * there cannot be cycles.
                     */
                    let mut statuses = Vec::new();
                    for c in child_execs {
                        statuses.push(c.status().await);
                    }
                    child_sagas
                        .insert(node, statuses)
                        .expect_none("duplicate child status");
                }

                /*
                 * Compute the node's depth.  This must be 0 (already stored
                 * above) for the root node and we don't care about doing this
                 * for the end node.
                 */
                if let Some(d) = max_depth_of_node.get(&node) {
                    assert_eq!(*d, 0);
                    assert_eq!(node, self.saga_metadata.start_node);
                    assert_eq!(max_depth_of_node.len(), 1);
                    continue;
                }

                /*
                 * We don't want to include the end node because we don't want
                 * to try to print it later.  This should always be the last
                 * iteration of the loop.
                 */
                if node == self.saga_metadata.end_node {
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
                saga_metadata: Arc::clone(&self.saga_metadata),
                nodes_at_depth,
                node_exec_states,
                child_sagas,
            }
        }
        .boxed()
    }
}

/*
 * This trait exists in order to erase the type parameter on SagaExecutor.  In
 * particular, SagaExecutor is necessarily parametrized by the user's SagaType,
 * which includes a bunch of other types that are specific to the user's
 * execution.  However, there are contexts where we don't care about that, and
 * we _do_ need to store a bunch of SagaExecutors that have, for example,
 * different SagaParamsType types.  Namely: a saga can have any number of child
 * sagas with different parameter types, and we want to keep a list of those so
 * that we can get their status.  We use this trait to refer to the child sagas.
 * TODO-cleanup Is there a better way to do this?
 */
trait SagaExecChild: Send + Sync + 'static {
    fn status(&self) -> BoxFuture<'_, SagaExecStatus>;
}
impl<UserType: SagaType> SagaExecChild for SagaExecutor<UserType> {
    fn status(&self) -> BoxFuture<'_, SagaExecStatus> {
        self.status()
    }
}
impl Debug for dyn SagaExecChild {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<SagaExecChild>")
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
    saga_template: Arc<SagaTemplate<UserType>>,
    saga_metadata: Arc<SagaTemplateMetadata>,

    /** Overall execution state */
    exec_state: SagaState,

    /** Queue of nodes that have not started but whose deps are satisfied */
    queue_todo: Vec<NodeIndex>,
    /** Queue of nodes whose undo action needs to be run. */
    queue_undo: Vec<NodeIndex>,

    /** Outstanding tokio tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,

    /** Outputs saved by completed actions. */
    node_outputs: BTreeMap<NodeIndex, Arc<JsonValue>>,
    /** Set of undone nodes. */
    nodes_undone: BTreeMap<NodeIndex, UndoMode>,
    /** Errors produced by failed actions. */
    node_errors: BTreeMap<NodeIndex, ActionError>,

    /** Child sagas created by a node (for status and control) */
    child_sagas: BTreeMap<NodeIndex, Vec<Arc<dyn SagaExecChild>>>,

    /** Persistent state */
    sglog: SagaLog,

    /** Injected errors */
    injected_errors: BTreeSet<NodeIndex>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NodeExecState {
    Blocked,
    QueuedToRun,
    TaskInProgress,
    Done,
    Failed,
    QueuedToUndo,
    Undone(UndoMode),
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
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
    fn node_exec_state(&self, node_id: &NodeIndex) -> NodeExecState {
        /*
         * This seems like overkill but it seems helpful to validate state.
         */
        let mut set: BTreeSet<NodeExecState> = BTreeSet::new();
        let load_status =
            self.sglog.load_status_for_node(node_id.index() as u64);
        if let Some(undo_mode) = self.nodes_undone.get(node_id) {
            set.insert(NodeExecState::Undone(*undo_mode));
        } else if self.queue_undo.contains(node_id) {
            set.insert(NodeExecState::QueuedToUndo);
        } else if let SagaNodeLoadStatus::Failed(_) = load_status {
            assert!(self.node_errors.contains_key(node_id));
            set.insert(NodeExecState::Failed);
        } else if self.node_outputs.contains_key(node_id) {
            set.insert(NodeExecState::Done);
        }

        if self.node_tasks.contains_key(node_id) {
            set.insert(NodeExecState::TaskInProgress);
        }
        if self.queue_todo.contains(node_id) {
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
            self.exec_state == SagaState::Running
                || self.exec_state == SagaState::Unwinding
        );
        self.exec_state = SagaState::Done;
    }

    fn node_task(&mut self, node_id: NodeIndex, task: JoinHandle<()>) {
        self.node_tasks.insert(node_id, task);
    }

    fn node_task_done(&mut self, node_id: NodeIndex) -> JoinHandle<()> {
        self.node_tasks
            .remove(&node_id)
            .expect("processing task completion with no task present")
    }

    fn node_output(&self, node_id: NodeIndex) -> Arc<JsonValue> {
        let output =
            self.node_outputs.get(&node_id).expect("node has no output");
        Arc::clone(output)
    }
}

/**
 * Summarizes the final state of a saga execution
 */
pub struct SagaResult {
    pub saga_id: SagaId,
    pub saga_log: SagaLog,
    pub kind: Result<SagaResultOk, SagaResultErr>,
}

/**
 * Provides access to outputs from a saga that completed successfully
 */
pub struct SagaResultOk {
    node_outputs: BTreeMap<String, Arc<JsonValue>>,
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
        let output_json = self.node_outputs.get(name).unwrap_or_else(|| {
            panic!("node with name \"{}\": not part of this saga", name)
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
pub struct SagaResultErr {
    pub error_node_name: String,
    pub error_source: ActionError,
}

/**
 * Summarizes in-progress execution state of a saga
 *
 * The only thing you can do with this currently is print it using `Display`.
 */
#[derive(Debug)]
pub struct SagaExecStatus {
    saga_id: SagaId,
    nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>>,
    node_exec_states: BTreeMap<NodeIndex, NodeExecState>,
    child_sagas: BTreeMap<NodeIndex, Vec<SagaExecStatus>>,
    saga_metadata: Arc<SagaTemplateMetadata>,
}

impl fmt::Display for SagaExecStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print(f, 0)
    }
}

impl SagaExecStatus {
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
            if nodes.len() == 1 {
                let node = nodes[0];
                let node_label = format!(
                    "{} (produces \"{}\")",
                    self.saga_metadata.node_label(&node).unwrap(),
                    self.saga_metadata.node_name(&node).unwrap()
                );
                let node_state = &self.node_exec_states[&node];
                write!(out, "{}: {}\n", node_state, node_label)?;
            } else {
                write!(out, "+ (actions in parallel)\n")?;
                for node in nodes {
                    let node_label = format!(
                        "{} (produces \"{}\")",
                        self.saga_metadata.node_label(&node).unwrap(),
                        self.saga_metadata.node_name(&node).unwrap()
                    );
                    let node_state = &self.node_exec_states[&node];
                    let child_sagas = self.child_sagas.get(&node);
                    let subsaga_char =
                        if child_sagas.is_some() { '+' } else { '-' };

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

                    if let Some(sagas) = child_sagas {
                        for c in sagas {
                            c.print(out, indent_level + 1)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/* TODO */
fn neighbors_all<F>(
    graph: &Graph<String, ()>,
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
 *
 * Any APIs that are useful for actions should hang off this object.  It should
 * have enough state to know which node is invoking the API.
 */
pub struct ActionContext<UserType: SagaType> {
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
    node_id: NodeIndex,
    saga_metadata: Arc<SagaTemplateMetadata>,
    live_state: Arc<Mutex<SagaExecLiveState<UserType>>>,
    /* TODO-cleanup should not need a copy here */
    creator: String,
    user_context: Arc<UserType::ExecContextType>,
    user_saga_params: Arc<UserType::SagaParamsType>,
}

impl<UserType: SagaType> ActionContext<UserType> {
    /**
     * Retrieves a piece of data stored by a previous (ancestor) node in the
     * current saga.  The data is identified by `name`.
     *
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
        let item = self
            .ancestor_tree
            .get(name)
            .unwrap_or_else(|| panic!("no ancestor called \"{}\"", name));
        // TODO-cleanup double-asterisk seems ridiculous
        serde_json::from_value((**item).clone())
            .map_err(ActionError::new_deserialize)
    }

    /**
     * Execute a new saga `sg` within this saga and wait for it to complete.
     *
     * `sg` is considered a "child" saga of the current saga, meaning that
     * control actions (like pause) on the current saga will affect the child
     * and status reporting of the current saga will show the status of the
     * child.
     */
    /*
     * TODO Is there some way to prevent people from instantiating their own
     * SagaExecutor by mistake instead?  Even better: if they do that, can we
     * detect that they're part of a saga already somehow and make the new
     * one a child saga?
     * TODO Would this be better done by having an ActionSaga that
     * executed a Saga as an action?  This way we would know when the
     * Saga was constructed what the whole graph looks like, instead of only
     * knowing about child sagas once we start executing the node that
     * creates them.
     * TODO We probably need to ensure that the child saga is running in the
     * same SEC.
     * TODO-design The only reason that we require ChildType::ExecContextType ==
     * UserType::ExecContextType is so that we can clone the user_context.  We
     * could just as well have the caller pass in a user context instead here,
     * and then they could use this to execute child sagas with different
     * context objects.  We don't anticipate needing this but there's no reason
     * we couldn't do it here, provided it's easy for users to clone the context
     * we give them (which actually may not be true today because they only get
     * a reference to their own context object).
     */
    pub async fn child_saga<ChildType>(
        &self,
        saga_id: &SagaId,
        sg: Arc<SagaTemplate<ChildType>>,
        initial_params: ChildType::SagaParamsType,
    ) -> Result<Arc<SagaExecutor<ChildType>>, ActionError>
    where
        ChildType: SagaType<ExecContextType = UserType::ExecContextType>,
    {
        let e = Arc::new(SagaExecutor::new(
            saga_id,
            sg,
            &self.creator,
            Arc::clone(&self.user_context),
            initial_params,
        )?);
        /* TODO-correctness Prove the lock ordering is okay here .*/
        self.live_state
            .lock()
            .await
            .child_sagas
            .entry(self.node_id)
            .or_insert_with(Vec::new)
            .push(Arc::clone(&e) as Arc<dyn SagaExecChild>);
        Ok(e)
    }

    /**
     * Returns the human-readable label for the current saga node
     */
    pub fn node_label(&self) -> &str {
        self.saga_metadata.node_label(&self.node_id).unwrap()
    }

    /**
     * Returns the consumer-provided context for the current saga
     */
    pub fn user_data(&self) -> &UserType::ExecContextType {
        &self.user_context
    }

    /**
     * Returns the consumer-provided parameters for the current saga
     */
    pub fn saga_params(&self) -> &UserType::SagaParamsType {
        &self.user_saga_params
    }
}

/**
 * Wrapper for SagaLog.record_now() that maps internal node indexes to
 * stable node ids.
 */
// TODO Consider how we do map internal node indexes to stable node ids.
// TODO clean up this interface
async fn record_now(
    sglog: &mut SagaLog,
    node: NodeIndex,
    event_type: SagaNodeEventType,
) {
    let node_id = node.index() as u64;

    /*
     * The only possible failure here today is attempting to record an event
     * that's illegal for the current node state.  That's a bug in this
     * program.
     */
    sglog.record_now(node_id, event_type).await.unwrap();
}
