//! Manages execution of a saga

use crate::saga_action::SagaAction;
use crate::saga_action::SagaActionInjectError;
use crate::saga_action::SagaActionOutput;
use crate::saga_action::SagaActionResult;
use crate::saga_action::SagaError;
use crate::saga_log::SagaNodeEventType;
use crate::saga_log::SagaNodeLoadStatus;
use crate::saga_template::SagaId;
use crate::SagaLog;
use crate::SagaTemplate;
use anyhow::anyhow;
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
use std::io;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/*
 * TODO-design Should we go even further and say that each node is its own
 * struct with incoming channels from parents (to notify when done), from
 * children (to notify when undone), and to each direction as well?  Then the
 * whole thing is a message passing exercise?
 */
struct SgnsDone(Arc<JsonValue>);
struct SgnsFailed(SagaError);
struct SgnsUndone(SagaUndoMode);

struct SagaNode<S: SagaNodeStateType> {
    node_id: NodeIndex,
    state: S,
}

trait SagaNodeStateType {}
impl SagaNodeStateType for SgnsDone {}
impl SagaNodeStateType for SgnsFailed {}
impl SagaNodeStateType for SgnsUndone {}

/* TODO-design Is this right?  Is the trait supposed to be empty? */
trait SagaNodeRest: Send + Sync {
    fn propagate(
        &self,
        exec: &SagaExecutor,
        live_state: &mut SagaExecLiveState,
    );
    fn log_event(&self) -> SagaNodeEventType;
}

impl SagaNodeRest for SagaNode<SgnsDone> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::Succeeded(Arc::clone(&self.state.0))
    }

    fn propagate(
        &self,
        exec: &SagaExecutor,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.saga_template.graph;
        live_state
            .node_outputs
            .insert(self.node_id, Arc::clone(&self.state.0))
            .expect_none("node finished twice (storing output)");

        if self.node_id == exec.saga_template.end_node {
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

impl SagaNodeRest for SagaNode<SgnsFailed> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::Failed
    }

    fn propagate(
        &self,
        exec: &SagaExecutor,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.saga_template.graph;

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
                    state: SgnsUndone(SagaUndoMode::ActionFailed),
                };
                new_node.propagate(exec, live_state);
            }
        } else {
            /*
             * Begin the unwinding process.  Start with the end node: mark
             * it trivially "undone" and propagate that.
             */
            live_state.exec_state = SagaState::Unwinding;
            assert_ne!(self.node_id, exec.saga_template.end_node);
            let new_node = SagaNode {
                node_id: exec.saga_template.end_node,
                state: SgnsUndone(SagaUndoMode::ActionNeverRan),
            };
            new_node.propagate(exec, live_state);
        }
    }
}

impl SagaNodeRest for SagaNode<SgnsUndone> {
    fn log_event(&self) -> SagaNodeEventType {
        SagaNodeEventType::UndoFinished
    }

    fn propagate(
        &self,
        exec: &SagaExecutor,
        live_state: &mut SagaExecLiveState,
    ) {
        let graph = &exec.saga_template.graph;
        assert_eq!(live_state.exec_state, SagaState::Unwinding);
        live_state
            .nodes_undone
            .insert(self.node_id, self.state.0)
            .expect_none("node already undone");

        if self.node_id == exec.saga_template.start_node {
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
                    SagaNodeExecState::Blocked => {
                        let new_node = SagaNode {
                            node_id: parent,
                            state: SgnsUndone(SagaUndoMode::ActionNeverRan),
                        };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    SagaNodeExecState::Failed => {
                        let new_node = SagaNode {
                            node_id: parent,
                            state: SgnsUndone(SagaUndoMode::ActionFailed),
                        };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    SagaNodeExecState::QueuedToRun
                    | SagaNodeExecState::TaskInProgress => {
                        /*
                         * If we're running an action for this task, there's
                         * nothing we can do right now, but we'll handle it when
                         * it finishes.  We could do better with queued (and
                         * there's a TODO-design in kick_off_ready() to do so),
                         * but this isn't wrong as-is.
                         */
                        continue;
                    }

                    SagaNodeExecState::Done => {
                        /*
                         * We have to actually run the undo action.
                         */
                        live_state.queue_undo.push(parent);
                    }

                    SagaNodeExecState::QueuedToUndo
                    | SagaNodeExecState::Undone(_) => {
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
struct TaskCompletion {
    /*
     * TODO-cleanup can this be removed? The node field is a SagaNode, which has
     * a node_id.
     */
    node_id: NodeIndex,
    node: Box<dyn SagaNodeRest>,
}

/**
 * Context provided to the (tokio) task that executes an action
 */
struct TaskParams {
    /** Handle to the saga itself, used for metadata like the label */
    saga_template: Arc<SagaTemplate>,

    /**
     * Handle to the saga's live state
     *
     * This is used only to update state for status purposes.  We want to avoid
     * any tight coupling between this task and the internal state.
     */
    live_state: Arc<Mutex<SagaExecLiveState>>,

    // TODO-cleanup should not need a copy here.
    creator: String,

    /** id of the graph node whose action we're running */
    node_id: NodeIndex,
    /** channel over which to send completion message */
    done_tx: mpsc::Sender<TaskCompletion>,
    /** Ancestor tree for this node.  See [`SagaContext`]. */
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
    /** The action itself that we're executing. */
    action: Arc<dyn SagaAction>,
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
 */
#[derive(Debug)]
pub struct SagaExecutor {
    // TODO This could probably be a reference instead.
    saga_template: Arc<SagaTemplate>,

    creator: String,

    /** Channel for monitoring execution completion */
    finish_tx: broadcast::Sender<()>,

    /** Unique identifier for this saga (an execution of a saga template) */
    saga_id: SagaId,

    live_state: Arc<Mutex<SagaExecLiveState>>,
}

#[derive(Debug)]
enum RecoveryDirection {
    Forward(bool),
    Unwind(bool),
}

impl SagaExecutor {
    /** Create an executor to run the given saga. */
    pub fn new(
        saga_template: Arc<SagaTemplate>,
        creator: &str,
    ) -> SagaExecutor {
        let saga_id = SagaId(Uuid::new_v4());
        let sglog = SagaLog::new(creator, saga_id);
        SagaExecutor::new_recover(saga_template, sglog, creator).unwrap()
    }

    /**
     * Create an executor to run the given saga that may have already
     * started, using the given log events.
     */
    pub fn new_recover(
        saga_template: Arc<SagaTemplate>,
        sglog: SagaLog,
        creator: &str,
    ) -> Result<SagaExecutor, SagaError> {
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
        let mut live_state = SagaExecLiveState {
            saga_template: Arc::clone(&saga_template),
            exec_state: if forward {
                SagaState::Running
            } else {
                SagaState::Done
            },
            queue_todo: Vec::new(),
            queue_undo: Vec::new(),
            node_tasks: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            nodes_undone: BTreeMap::new(),
            child_sagas: BTreeMap::new(),
            sglog: sglog,
            injected_errors: BTreeSet::new(),
        };
        let mut loaded = BTreeSet::new();

        /*
         * Iterate in the direction of current execution: for normal execution,
         * a standard topological sort.  For unwinding, reverse that.
         */
        let graph_nodes = {
            let mut nodes = toposort(&saga_template.graph, None)
                .expect("saga DAG had cycles");
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
            for parent in
                saga_template.graph.neighbors_directed(node_id, Incoming)
            {
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

            let graph = &saga_template.graph;
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
                                .insert(node_id, SagaUndoMode::ActionNeverRan);
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
                    live_state
                        .node_outputs
                        .insert(node_id, Arc::clone(output))
                        .expect_none("recovered node twice (success case)");
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state.queue_undo.push(node_id);
                    }
                }
                SagaNodeLoadStatus::Failed => {
                    /*
                     * If the node failed, and we're unwinding, and the children
                     * have all been undone, it's time to undo this one.
                     * But we just mark it undone -- we don't execute the
                     * undo action.
                     */
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state
                            .nodes_undone
                            .insert(node_id, SagaUndoMode::ActionFailed);
                    }
                }
                SagaNodeLoadStatus::UndoStarted => {
                    /*
                     * We know we're unwinding. (Otherwise, we should have
                     * failed validation earlier.)  Execute the undo action.
                     */
                    assert!(!forward);
                    live_state.queue_undo.push(node_id);
                }
                SagaNodeLoadStatus::UndoFinished => {
                    /*
                     * Again, we know we're unwinding.  We've also finished
                     * undoing this node.
                     */
                    assert!(!forward);
                    live_state
                        .nodes_undone
                        .insert(node_id, SagaUndoMode::ActionUndone);
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
        if live_state.node_outputs.contains_key(&saga_template.end_node)
            || live_state.nodes_undone.contains_key(&saga_template.start_node)
        {
            live_state.mark_saga_done();
        }

        let (finish_tx, _) = broadcast::channel(1);

        Ok(SagaExecutor {
            saga_template: saga_template,
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
     * outputs from those nodes.  This is used by [`SagaContext::lookup`].  See
     * where we use this function in poll() for more details.
     */
    fn make_ancestor_tree(
        &self,
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
        live_state: &SagaExecLiveState,
        node: NodeIndex,
        include_self: bool,
    ) {
        if include_self {
            self.make_ancestor_tree_node(tree, live_state, node);
            return;
        }

        let ancestors =
            self.saga_template.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, live_state, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
        live_state: &SagaExecLiveState,
        node: NodeIndex,
    ) {
        if node == self.saga_template.start_node {
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
        let name = self.saga_template.node_names[&node].to_string();
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
     * Wrapper for SagaLog.record_now() that maps internal node indexes to
     * stable node ids.
     */
    // TODO Consider how we do map internal node indexes to stable node ids.
    // TODO clean up this interface
    // TODO Decide what we want to do if this actually fails and handle it
    // properly.
    async fn record_now(
        sglog: &mut SagaLog,
        node: NodeIndex,
        event_type: SagaNodeEventType,
    ) {
        let node_id = node.index() as u64;
        sglog.record_now(node_id, event_type).await.unwrap();
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
            mpsc::channel(2 * self.saga_template.graph.node_count());

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
    async fn kick_off_ready(&self, tx: &mpsc::Sender<TaskCompletion>) {
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
                Arc::new(SagaActionInjectError {}) as Arc<dyn SagaAction>
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
                saga_template: Arc::clone(&self.saga_template),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: sgaction,
                creator: self.creator.clone(),
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
                saga_template: Arc::clone(&self.saga_template),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: Arc::clone(sgaction),
                creator: self.creator.clone(),
            };

            let task = tokio::spawn(SagaExecutor::undo_node(task_params));
            live_state.node_task(node_id, task);
        }
    }

    /**
     * Body of a (tokio) task that executes an action.
     */
    async fn exec_node(task_params: TaskParams) {
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
                    SagaExecutor::record_now(
                        &mut live_state.sglog,
                        node_id,
                        SagaNodeEventType::Started,
                    )
                    .await;
                }
                SagaNodeLoadStatus::Started => (),
                SagaNodeLoadStatus::Succeeded(_)
                | SagaNodeLoadStatus::Failed
                | SagaNodeLoadStatus::UndoStarted
                | SagaNodeLoadStatus::UndoFinished => {
                    panic!("starting node in bad state")
                }
            }
        }

        let exec_future = task_params.action.do_it(SagaContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            saga_template: Arc::clone(&task_params.saga_template),
            creator: task_params.creator.clone(),
        });
        let result = exec_future.await;
        let node: Box<dyn SagaNodeRest> = match result {
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
    async fn undo_node(task_params: TaskParams) {
        let node_id = task_params.node_id;

        {
            let mut live_state = task_params.live_state.lock().await;
            let load_status =
                live_state.sglog.load_status_for_node(node_id.index() as u64);
            match load_status {
                SagaNodeLoadStatus::Succeeded(_) => {
                    SagaExecutor::record_now(
                        &mut live_state.sglog,
                        node_id,
                        SagaNodeEventType::UndoStarted,
                    )
                    .await;
                }
                SagaNodeLoadStatus::UndoStarted => (),
                SagaNodeLoadStatus::NeverStarted
                | SagaNodeLoadStatus::Started
                | SagaNodeLoadStatus::Failed
                | SagaNodeLoadStatus::UndoFinished => {
                    panic!("undoing node in bad state")
                }
            }
        }

        let exec_future = task_params.action.undo_it(SagaContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            saga_template: Arc::clone(&task_params.saga_template),
            creator: task_params.creator.clone(),
        });
        /*
         * TODO-robustness We have to figure out what it means to fail here and
         * what we want to do about it.
         */
        exec_future.await.unwrap();
        let node = Box::new(SagaNode {
            node_id,
            state: SgnsUndone(SagaUndoMode::ActionUndone),
        });
        SagaExecutor::finish_task(task_params, node).await;
    }

    async fn finish_task(
        mut task_params: TaskParams,
        node: Box<dyn SagaNodeRest>,
    ) {
        let node_id = task_params.node_id;
        let event_type = node.log_event();

        {
            let mut live_state = task_params.live_state.lock().await;
            SagaExecutor::record_now(
                &mut live_state.sglog,
                node_id,
                event_type,
            )
            .await;
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
     * Returns a [`SagaExecResult`] describing the result of the saga, including
     * data produced by its actions.
     *
     * # Panics
     *
     * If the saga has not yet completed.
     */
    pub fn result(&self) -> SagaExecResult {
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

        let mut node_results = BTreeMap::new();
        for (node_id, output) in &live_state.node_outputs {
            if *node_id == self.saga_template.start_node
                || *node_id == self.saga_template.end_node
            {
                continue;
            }

            let node_name = &self.saga_template.node_names[node_id];
            node_results.insert(node_name.clone(), Ok(Arc::clone(output)));
        }

        SagaExecResult {
            saga_id: self.saga_id,
            sglog: live_state.sglog.clone(),
            node_results,
            succeeded: true,
        }
    }

    /*
     * TODO-cleanup It would be more idiomatic to return an object that impls
     * Display or Debug to do this.
     */
    // TODO-liveness does this writer need to be async?
    /**
     * Summarize the current execution status of the saga
     */
    pub fn print_status<'a, 'b, 'c>(
        &'a self,
        out: &'b mut (dyn io::Write + Send),
        indent_level: usize,
    ) -> BoxFuture<'c, io::Result<()>>
    where
        'a: 'c,
        'b: 'c,
    {
        /* TODO-cleanup There must be a better way to do this. */
        let mut max_depth_of_node: BTreeMap<NodeIndex, usize> = BTreeMap::new();
        max_depth_of_node.insert(self.saga_template.start_node, 0);

        let mut nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>> =
            BTreeMap::new();

        let graph = &self.saga_template.graph;
        let topo_visitor = Topo::new(graph);
        for node in topo_visitor.iter(graph) {
            if let Some(d) = max_depth_of_node.get(&node) {
                assert_eq!(*d, 0);
                assert_eq!(node, self.saga_template.start_node);
                assert_eq!(max_depth_of_node.len(), 1);
                continue;
            }

            if node == self.saga_template.end_node {
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

            nodes_at_depth.entry(depth).or_insert(Vec::new()).push(node);
        }

        let big_indent = indent_level * 16;

        async move {
            let live_state = self.live_state.lock().await;

            write!(
                out,
                "{:width$}+ saga execution: {}\n",
                "",
                self.saga_id,
                width = big_indent
            )?;
            for (d, nodes) in nodes_at_depth {
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
                        self.saga_template.node_labels[&node],
                        &self.saga_template.node_names[&node]
                    );
                    let node_state = live_state.node_exec_state(&node);
                    write!(out, "{}: {}\n", node_state, node_label)?;
                } else {
                    write!(out, "+ (actions in parallel)\n")?;
                    for node in nodes {
                        let node_label = format!(
                            "{} (produces \"{}\")",
                            self.saga_template.node_labels[&node],
                            &self.saga_template.node_names[&node]
                        );
                        let node_state = live_state.node_exec_state(&node);
                        let child_sagas = live_state.child_sagas.get(&node);
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
                                c.print_status(out, indent_level + 1).await?;
                            }
                        }
                    }
                }
            }

            Ok(())
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
struct SagaExecLiveState {
    saga_template: Arc<SagaTemplate>,
    /** Overall execution state */
    exec_state: SagaState,

    /** Queue of nodes that have not started but whose deps are satisfied */
    queue_todo: Vec<NodeIndex>,
    /** Queue of nodes whose undo action needs to be run. */
    queue_undo: Vec<NodeIndex>,

    /** Outstanding tokio tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,

    /** Outputs saved by completed actions. */
    // TODO may as well store errors too
    node_outputs: BTreeMap<NodeIndex, Arc<JsonValue>>,
    /** Set of undone nodes. */
    nodes_undone: BTreeMap<NodeIndex, SagaUndoMode>,

    /** Child sagas created by a node (for status and control) */
    child_sagas: BTreeMap<NodeIndex, Vec<Arc<SagaExecutor>>>,

    /** Persistent state */
    sglog: SagaLog,

    /** Injected errors */
    injected_errors: BTreeSet<NodeIndex>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SagaNodeExecState {
    Blocked,
    QueuedToRun,
    TaskInProgress,
    Done,
    Failed,
    QueuedToUndo,
    Undone(SagaUndoMode),
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum SagaUndoMode {
    ActionNeverRan,
    ActionUndone,
    ActionFailed,
}

impl fmt::Display for SagaNodeExecState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SagaNodeExecState::Blocked => "blocked",
            SagaNodeExecState::QueuedToRun => "queued-todo",
            SagaNodeExecState::TaskInProgress => "working",
            SagaNodeExecState::Done => "done",
            SagaNodeExecState::Failed => "failed",
            SagaNodeExecState::QueuedToUndo => "queued-undo",
            SagaNodeExecState::Undone(SagaUndoMode::ActionNeverRan) => {
                "abandoned"
            }
            SagaNodeExecState::Undone(SagaUndoMode::ActionUndone) => "undone",
            SagaNodeExecState::Undone(SagaUndoMode::ActionFailed) => "failed",
        })
    }
}

impl SagaExecLiveState {
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
    fn node_exec_state(&self, node_id: &NodeIndex) -> SagaNodeExecState {
        /*
         * This seems like overkill but it seems helpful to validate state.
         */
        let mut set: BTreeSet<SagaNodeExecState> = BTreeSet::new();
        let load_status =
            self.sglog.load_status_for_node(node_id.index() as u64);
        if let Some(undo_mode) = self.nodes_undone.get(node_id) {
            set.insert(SagaNodeExecState::Undone(*undo_mode));
        } else if self.node_outputs.contains_key(node_id) {
            set.insert(SagaNodeExecState::Done);
        } else if let SagaNodeLoadStatus::Failed = load_status {
            set.insert(SagaNodeExecState::Failed);
        }
        if self.node_tasks.contains_key(node_id) {
            set.insert(SagaNodeExecState::TaskInProgress);
        }
        if self.queue_todo.contains(node_id) {
            set.insert(SagaNodeExecState::QueuedToRun);
        }
        if self.queue_undo.contains(node_id) {
            set.insert(SagaNodeExecState::QueuedToUndo);
        }
        if set.is_empty() {
            if let SagaNodeLoadStatus::NeverStarted = load_status {
                SagaNodeExecState::Blocked
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
 * Summarizes the final state of a saga execution.
 */
pub struct SagaExecResult {
    pub saga_id: SagaId,
    pub sglog: SagaLog,
    node_results: BTreeMap<String, SagaActionResult>,
    succeeded: bool,
}

impl SagaExecResult {
    /**
     * Returns the data produced by a node in the saga, if the saga completed
     * successfully.  Otherwise, returns an error.
     *
     * # Panics
     *
     * If the saga has no node called `name`, or if the type produced by this
     * node does not match `T`.
     */
    pub fn lookup_output<T: SagaActionOutput + 'static>(
        &self,
        name: &str,
    ) -> Result<T, SagaError> {
        if !self.succeeded {
            return Err(anyhow!(
                "fetch output \"{}\" from saga execution \
                \"{}\": saga did not complete successfully",
                name,
                self.saga_id
            ));
        }

        let result = self.node_results.get(name).expect(&format!(
            "node with name \"{}\" is not part of this saga",
            name
        ));
        let item = result.as_ref().expect(&format!(
            "node with name \"{}\" failed and did not produce an output",
            name
        ));
        // TODO-cleanup double-asterisk seems ridiculous
        let parsed: T =
            serde_json::from_value((**item).clone()).expect(&format!(
                "requested wrong type for output of node with name \"{}\"",
                name
            ));
        Ok(parsed)
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
    match (child_status, parent_status) {
        /*
         * If the child node has started, finished successfully, finished with
         * an error, or even started undoing, the only allowed status for the
         * parent node is "done".  The states prior to "done" are ruled out
         * because we execute nodes in dependency order.  "failed" is ruled out
         * because we do not execute nodes whose parents failed.  The undoing
         * states are ruled out because we unwind in reverse-dependency order,
         * so we cannot have started undoing the parent if the child node has
         * not finished undoing.  (A subtle but important implementation
         * detail is that we do not undo a node that has not started
         * execution.  If we did, then the "undo started" load state could be
         * associated with a parent that failed.)
         */
        (
            SagaNodeLoadStatus::Started
            | SagaNodeLoadStatus::Succeeded(_)
            | SagaNodeLoadStatus::Failed
            | SagaNodeLoadStatus::UndoStarted,
            SagaNodeLoadStatus::Succeeded(_),
        ) => true,

        /*
         * If we've finished undoing the child node, then the parent must be
         * either "done" or one of the undoing states.
         */
        (
            SagaNodeLoadStatus::UndoFinished,
            SagaNodeLoadStatus::Succeeded(_)
            | SagaNodeLoadStatus::UndoStarted
            | SagaNodeLoadStatus::UndoFinished,
        ) => true,

        /*
         * If a node has never started, the only illegal states for a parent are
         * those associated with undoing, since the child must be undone first.
         */
        (
            SagaNodeLoadStatus::NeverStarted,
            SagaNodeLoadStatus::NeverStarted
            | SagaNodeLoadStatus::Started
            | SagaNodeLoadStatus::Succeeded(_)
            | SagaNodeLoadStatus::Failed,
        ) => true,
        _ => false,
    }
}

/**
 * Action's handle to the saga subsystem
 *
 * Any APIs that are useful for actions should hang off this object.  It should
 * have enough state to know which node is invoking the API.
 */
pub struct SagaContext {
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
    node_id: NodeIndex,
    saga_template: Arc<SagaTemplate>,
    live_state: Arc<Mutex<SagaExecLiveState>>,
    /* TODO-cleanup should not need a copy here */
    creator: String,
}

impl SagaContext {
    /**
     * Retrieves a piece of data stored by a previous (ancestor) node in the
     * current saga.  The data is identified by `name`.
     *
     *
     * # Panics
     *
     * This function panics if there was no data previously stored with name
     * `name` or if the type of that data was not `T`.  The assumption here is
     * that actions within a saga are tightly coupled, so the caller knows
     * exactly what the previous action stored.  We would enforce this at
     * compile time if we could.
     */
    pub fn lookup<T: SagaActionOutput + 'static>(&self, name: &str) -> T {
        let item = self
            .ancestor_tree
            .get(name)
            .expect(&format!("no ancestor called \"{}\"", name));
        // TODO-cleanup double-asterisk seems ridiculous
        let specific_item = serde_json::from_value((**item).clone())
            .expect(&format!("ancestor \"{}\" produced unexpected type", name));
        specific_item
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
     * TODO Would this be better done by having a SagaActionSaga that
     * executed a Saga as an action?  This way we would know when the
     * Saga was constructed what the whole graph looks like, instead of only
     * knowing about child sagas once we start executing the node that
     * creates them.
     * TODO We probably need to ensure that the child saga is running in the
     * same SEC.
     */
    pub async fn child_saga(&self, sg: Arc<SagaTemplate>) -> Arc<SagaExecutor> {
        let e = Arc::new(SagaExecutor::new(sg, &self.creator));
        /* TODO-correctness Prove the lock ordering is okay here .*/
        self.live_state
            .lock()
            .await
            .child_sagas
            .entry(self.node_id)
            .or_insert(Vec::new())
            .push(Arc::clone(&e));
        e
    }

    /**
     * Returns the human-readable label for the current saga node
     */
    pub fn node_label(&self) -> &str {
        self.saga_template.node_labels.get(&self.node_id).unwrap()
    }
}
