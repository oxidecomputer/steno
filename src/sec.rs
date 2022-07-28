/*!
 * Saga Execution Coordinator
 *
 * The Saga Execution Coordinator ("SEC") manages the execution of a group of
 * sagas, providing interfaces for running new sagas, recovering sagas that were
 * running in a previous lifetime, listing sagas, querying the state of a saga,
 * and providing some control over sagas (e.g., to inject errors).
 *
 * The implementation is grouped into
 *
 * * [`sec()`], a function to start up an SEC
 * * an `SecClient`, which Steno consumers use to interact with the SEC
 * * an `Sec`: a background task that owns the list of sagas and their overall
 *   runtime state.  (The detailed runtime state is owned by a separate
 *   `SagaExecutor` type internally.)
 * * a number of `SecExecClient` objects, which individual saga executors use to
 *   communicate back to the Sec (to communicate progress, record persistent
 *   state, etc.)
 *
 * The control flow of these components and their messages is shown in the
 * below diagram:
 *
 *  +---------+
 *  |  Saga   |
 *  |Consumer |--+
 *  +---------+  |
 *               |
 *           Saga API
 *               |
 *               |   +-------------+                  +-------------+
 *               |   |             |                  |             |
 *               |   |     SEC     |                  |     SEC     |
 *               +-->|   Client    |----------------->|   (task)    |
 *                   |             |   SecClientMsg   |             |
 *                   |             |                  |             |
 *                   +-------------+                  +-------------+
 *                                                           ^
 *                                                           |
 *                                                           |
 *                                                       SecExecMsg
 *                                                           |
 *                                                           |
 *                                                    +-------------+
 *                                                    |             |
 *                                                    | SagaExecutor|
 *                                                    |  (future)   |
 *                                                    |             |
 *                                                    |             |
 *                                                    +-------------+
 *
 * The Steno consumer is responsible for implementing an `SecStore` to store
 * persistent state.  There's an [`crate::InMemorySecStore`] to play around
 * with.
 */

#![allow(clippy::large_enum_variant)]

use crate::dag::SagaDag;
use crate::saga_exec::SagaExecManager;
use crate::saga_exec::SagaExecutor;
use crate::store::SagaCachedState;
use crate::store::SagaCreateParams;
use crate::store::SecStore;
use crate::ActionError;
use crate::ActionRegistry;
use crate::SagaExecStatus;
use crate::SagaId;
use crate::SagaLog;
use crate::SagaNodeEvent;
use crate::SagaResult;
use crate::SagaType;
use anyhow::anyhow;
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use petgraph::graph::NodeIndex;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/*
 * SEC client side (handle used by Steno consumers)
 */

/**
 * Maximum number of messages for the SEC that can be queued from the client
 *
 * This is very small.  These messages are commands, and the client always waits
 * for a response.  So it makes little difference to latency or throughput
 * whether the client waits up front for available buffer space or waits instead
 * on the response channel (with the request buffered in the queue).
 */
const SEC_CLIENT_MAXQ_MESSAGES: usize = 2;

/**
 * Maximum number of messages for the SEC that can be queued from SagaExecutors
 *
 * As with clients, we keep the queue small.  This may mean that SagaExecutors
 * get stuck behind the SEC, but that's preferable to bloat or more implicit
 * queueing delays.
 */
const SEC_EXEC_MAXQ_MESSAGES: usize = 2;

/**
 * Creates a new Saga Execution Coordinator
 */
pub fn sec(log: slog::Logger, sec_store: Arc<dyn SecStore>) -> SecClient {
    let (cmd_tx, cmd_rx) = mpsc::channel(SEC_CLIENT_MAXQ_MESSAGES);
    let (exec_tx, exec_rx) = mpsc::channel(SEC_EXEC_MAXQ_MESSAGES);

    /*
     * We spawn a new task rather than return a `Future` for the caller to
     * poll because we want to make sure the Sec can't be dropped unless
     * shutdown() has been invoked on the client.
     */
    let task = tokio::spawn(async move {
        let sec = Sec {
            log,
            sagas: BTreeMap::new(),
            sec_store,
            futures: FuturesUnordered::new(),
            cmd_rx,
            shutdown: false,
            exec_tx,
            exec_rx,
        };

        sec.run().await
    });
    SecClient { cmd_tx, task: Some(task), shutdown: false }
}

/**
 * Client handle for a Saga Execution Coordinator (SEC)
 *
 * This is the interface through which Steno consumers create new sagas, recover
 * sagas that were created in previous lifetimes, list sagas, and so on.
 */
#[derive(Debug)]
pub struct SecClient {
    cmd_tx: mpsc::Sender<SecClientMsg>,
    task: Option<tokio::task::JoinHandle<()>>,
    shutdown: bool,
}

impl SecClient {
    /**
     * Creates a new saga, which may later started with [`Self::saga_start`].
     *
     * This function asynchronously returns a `Future` that can be used to wait
     * for the saga to finish.  It's also safe to cancel (drop) this Future.
     */
    pub async fn saga_create<UserType>(
        &self,
        saga_id: SagaId,
        uctx: Arc<UserType::ExecContextType>,
        dag: Arc<SagaDag>,
        registry: Arc<ActionRegistry<UserType>>,
    ) -> Result<BoxFuture<'static, SagaResult>, anyhow::Error>
    where
        UserType: SagaType + fmt::Debug,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let template_params = Box::new(TemplateParamsForCreate {
            dag: dag.clone(),
            registry,
            uctx,
        }) as Box<dyn TemplateParams>;
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaCreate { ack_tx, saga_id, dag, template_params },
        )
        .await
    }

    /**
     * Resume a saga that was previously running
     *
     * This function asynchronously returns a `Future` that can be used to wait
     * for the saga to finish.  It's also safe to cancel (drop) this Future.
     */
    pub async fn saga_resume<UserType>(
        &self,
        saga_id: SagaId,
        uctx: Arc<UserType::ExecContextType>,
        dag: serde_json::Value,
        registry: Arc<ActionRegistry<UserType>>,
        log_events: Vec<SagaNodeEvent>,
    ) -> Result<BoxFuture<'static, SagaResult>, anyhow::Error>
    where
        UserType: SagaType + fmt::Debug,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let saga_log = SagaLog::new_recover(saga_id, log_events)
            .context("recovering log")?;
        let dag: Arc<SagaDag> = Arc::new(
            serde_json::from_value(dag)
                .map_err(ActionError::new_deserialize)?,
        );
        let template_params = Box::new(TemplateParamsForRecover {
            dag: dag.clone(),
            registry,
            uctx,
            saga_log,
        }) as Box<dyn TemplateParams>;
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaResume { ack_tx, saga_id, dag, template_params },
        )
        .await
    }

    /**
     * Start running (or resume running) a saga that was created with
     * [`SecClient::saga_create()`] or [`SecClient::saga_resume()`].
     */
    pub async fn saga_start(
        &self,
        saga_id: SagaId,
    ) -> Result<(), anyhow::Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_cmd(ack_rx, SecClientMsg::SagaStart { ack_tx, saga_id }).await
    }

    /**
     * List known sagas
     */
    pub async fn saga_list(
        &self,
        marker: Option<SagaId>,
        limit: NonZeroU32,
    ) -> Vec<SagaView> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_cmd(ack_rx, SecClientMsg::SagaList { ack_tx, marker, limit })
            .await
    }

    /**
     * Fetch information about one saga
     */
    pub async fn saga_get(&self, saga_id: SagaId) -> Result<SagaView, ()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_cmd(ack_rx, SecClientMsg::SagaGet { ack_tx, saga_id }).await
    }

    /**
     * Inject an error into one saga
     */
    pub async fn saga_inject_error(
        &self,
        saga_id: SagaId,
        node_id: NodeIndex,
    ) -> Result<(), anyhow::Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaInjectError { ack_tx, saga_id, node_id },
        )
        .await
    }

    /**
     * Shut down the SEC and wait for it to come to rest.
     */
    pub async fn shutdown(mut self) {
        self.shutdown = true;
        self.cmd_tx.send(SecClientMsg::Shutdown).await.unwrap_or_else(
            |error| panic!("failed to send message to SEC: {:#}", error),
        );
        self.task
            .take()
            .expect("missing task")
            .await
            .expect("failed to join on SEC task");
    }

    /**
     * Sends `msg` to the SEC and waits for a response on `ack_rx`
     *
     * The SEC is not expected to shut down until we issue the shutdown command,
     * which only happens when the consumer has given up ownership of this
     * object.  So we can assume that the SEC is still running and that these
     * channel operations will not fail.
     */
    async fn sec_cmd<R>(
        &self,
        ack_rx: oneshot::Receiver<R>,
        msg: SecClientMsg,
    ) -> R {
        self.cmd_tx.send(msg).await.unwrap_or_else(|error| {
            panic!("failed to send message to SEC: {:#}", error)
        });
        ack_rx.await.expect("failed to read SEC response")
    }
}

impl Drop for SecClient {
    fn drop(&mut self) {
        if !self.shutdown {
            /*
             * If we get here, there should be no outstanding requests on this
             * channel, in which case there must be buffer space and try_send()
             * ought not to fail for running out of space.  It may fail if the
             * other side is closed, but that should only happen if the SEC task
             * panicked.
             */
            let _ = self.cmd_tx.try_send(SecClientMsg::Shutdown);
        }
    }
}

/** External consumer's view of a saga */
#[derive(Clone, Debug, JsonSchema, Serialize)]
pub struct SagaView {
    pub id: SagaId,

    /* TODO-debugging impl an appropriate Serialize here */
    #[serde(skip)]
    pub state: SagaStateView,

    dag: serde_json::Value,
}

impl SagaView {
    fn from_saga(saga: &Saga) -> impl Future<Output = Self> {
        let id = saga.id;
        let dag = saga.dag.clone();
        let fut = SagaStateView::from_run_state(&saga.run_state);
        async move {
            let state = fut.await;
            SagaView { id, state, dag }
        }
    }

    /**
     * Returns an object that impl's serde's `Deserialize` and `Serialize`
     * traits
     *
     * This is mainly intended for tooling and demoing.  Production state
     * serialization happens via the [`SecStore`].
     */
    pub fn serialized(&self) -> SagaSerialized {
        SagaSerialized {
            saga_id: self.id,
            dag: self.dag.clone(),
            events: self.state.status().log().events().to_vec(),
        }
    }
}

/** State-specific parts of a consumer's view of a saga */
#[derive(Debug, Clone)]
pub enum SagaStateView {
    /** The saga is ready to start running */
    Ready {
        /** initial execution status */
        status: SagaExecStatus,
    },
    /** The saga is still running */
    Running {
        /** current execution status */
        status: SagaExecStatus,
    },
    /** The saga has finished running */
    Done {
        /** final execution status */
        status: SagaExecStatus,
        /** final result */
        result: SagaResult,
    },
}

impl SagaStateView {
    fn from_run_state(
        run_state: &SagaRunState,
    ) -> impl Future<Output = SagaStateView> {
        enum Which {
            Ready(Arc<dyn SagaExecManager>),
            Running(Arc<dyn SagaExecManager>),
            Done(SagaExecStatus, SagaResult),
        }

        let which = match run_state {
            SagaRunState::Ready { exec, .. } => Which::Ready(Arc::clone(exec)),
            SagaRunState::Running { exec, .. } => {
                Which::Running(Arc::clone(exec))
            }
            SagaRunState::Done { status, result } => {
                Which::Done(status.clone(), result.clone())
            }
        };

        async move {
            match which {
                Which::Ready(exec) => {
                    SagaStateView::Ready { status: exec.status().await }
                }
                Which::Running(exec) => {
                    SagaStateView::Running { status: exec.status().await }
                }
                Which::Done(status, result) => {
                    SagaStateView::Done { status, result }
                }
            }
        }
    }

    /** Returns the status summary for this saga */
    pub fn status(&self) -> &SagaExecStatus {
        match self {
            SagaStateView::Ready { status } => status,
            SagaStateView::Running { status } => status,
            SagaStateView::Done { status, .. } => status,
        }
    }
}

/*
 * SEC Client/Server interface
 */

/**
 * Message passed from the [`SecClient`] to the [`Sec`]
 */
/*
 * TODO-cleanup This might be cleaner using separate named structs for the
 * enums, similar to what we do for SecStep.
 */
enum SecClientMsg {
    /**
     * Creates a new saga
     *
     * The response includes a Future that can be used to wait for the saga to
     * finish.  The caller can ignore this.
     */
    SagaCreate {
        /** response channel */
        ack_tx: oneshot::Sender<
            Result<BoxFuture<'static, SagaResult>, anyhow::Error>,
        >,
        /** caller-defined id (must be unique) */
        saga_id: SagaId,

        /** user-type-specific parameters */
        template_params: Box<dyn TemplateParams>,

        /** The user created DAG */
        dag: Arc<SagaDag>,
    },

    /**
     * Resumes a saga from a previous lifetime (i.e., after a restart)
     *
     * The response includes a Future that can be used to wait for the saga to
     * finish.  The caller can ignore this.
     */
    SagaResume {
        /** response channel */
        ack_tx: oneshot::Sender<
            Result<BoxFuture<'static, SagaResult>, anyhow::Error>,
        >,
        /** unique id of the saga (from persistent state) */
        saga_id: SagaId,

        /** user-type-specific parameters */
        template_params: Box<dyn TemplateParams>,

        /** The user created DAG */
        dag: Arc<SagaDag>,
    },

    /** Start (or resume) running a saga */
    SagaStart {
        /** response channel */
        ack_tx: oneshot::Sender<Result<(), anyhow::Error>>,
        /** id of the saga to start running */
        saga_id: SagaId,
    },

    /** List sagas */
    SagaList {
        /** response channel */
        ack_tx: oneshot::Sender<Vec<SagaView>>,
        /** marker (where in the ID space to start listing from) */
        marker: Option<SagaId>,
        /** maximum number of sagas to return */
        limit: NonZeroU32,
    },

    /** Fetch information about one saga */
    SagaGet {
        /** response channel */
        ack_tx: oneshot::Sender<Result<SagaView, ()>>,
        /** id of saga to fetch information about */
        saga_id: SagaId,
    },

    /** Inject an error at a specific action in the saga */
    SagaInjectError {
        /** response channel */
        ack_tx: oneshot::Sender<Result<(), anyhow::Error>>,
        /** id of saga to fetch information about */
        saga_id: SagaId,
        /**
         * id of the node to inject the error (see
         * [`SagaTemplateMetadata::node_for_name`])
         */
        node_id: NodeIndex,
    },

    /** Shut down the SEC */
    Shutdown,
}

impl fmt::Debug for SecClientMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecClientMsg::")?;
        match self {
            SecClientMsg::SagaCreate {
                saga_id, template_params, dag, ..
            } => f
                .debug_struct("SagaCreate")
                .field("saga_id", saga_id)
                .field("template_params", template_params)
                .field("dag", dag)
                .finish(),
            SecClientMsg::SagaResume {
                saga_id, template_params, dag, ..
            } => f
                .debug_struct("SagaResume")
                .field("saga_id", saga_id)
                .field("template_params", template_params)
                .field("dag", dag)
                .finish(),
            SecClientMsg::SagaList { .. } => f.write_str("SagaList"),
            SecClientMsg::SagaGet { saga_id, .. } => {
                f.debug_struct("SagaGet").field("saga_id", saga_id).finish()
            }
            SecClientMsg::SagaInjectError { saga_id, node_id, .. } => f
                .debug_struct("SagaInjectError")
                .field("saga_id", saga_id)
                .field("node_Id", node_id)
                .finish(),
            SecClientMsg::Shutdown { .. } => f.write_str("Shutdown"),
            SecClientMsg::SagaStart { saga_id, .. } => {
                f.debug_struct("SagaStart").field("saga_id", saga_id).finish()
            }
        }
    }
}

/**
 * This trait erases the type parameters on a [`SagaTemplate`], user context,
 * and user parameters so that we can more easily pass it through a channel.
 * TODO(AJS) - rename since template no longer exists?
 */
trait TemplateParams: Send + fmt::Debug {
    fn into_exec(
        self: Box<Self>,
        log: slog::Logger,
        saga_id: SagaId,
        sec_hdl: SecExecClient,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error>;
}

/**
 * Stores a template and user context in a way where the
 * user-defined types can be erased with [`TemplateParams`]
 *
 * This version is for the "create" case, where we know the specific
 * [`SagaType`] for these values.  See [`SecClient::saga_create`].
 */
#[derive(Debug)]
struct TemplateParamsForCreate<UserType: SagaType + fmt::Debug> {
    dag: Arc<SagaDag>,
    registry: Arc<ActionRegistry<UserType>>,
    uctx: Arc<UserType::ExecContextType>,
}

impl<UserType> TemplateParams for TemplateParamsForCreate<UserType>
where
    UserType: SagaType + fmt::Debug,
{
    fn into_exec(
        self: Box<Self>,
        log: slog::Logger,
        saga_id: SagaId,
        sec_hdl: SecExecClient,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error> {
        Ok(Arc::new(SagaExecutor::new(
            log,
            saga_id,
            self.dag,
            self.registry,
            self.uctx,
            sec_hdl,
        )))
    }
}

/**
 * Stores a template, saga parameters, and user context in a way where the
 * user-defined types can be erased with [`TemplateParams]
 *
 * This version is for the "resume" case, where we know the specific context
 * type, but not the parameters or template type.  We also have a saga log in
 * this case.  See [`SecClient::saga_resume()`].
 */
#[derive(Debug)]
struct TemplateParamsForRecover<UserType: SagaType + fmt::Debug> {
    dag: Arc<SagaDag>,
    registry: Arc<ActionRegistry<UserType>>,
    uctx: Arc<UserType::ExecContextType>,
    saga_log: SagaLog,
}

impl<UserType> TemplateParams for TemplateParamsForRecover<UserType>
where
    UserType: SagaType + fmt::Debug,
{
    fn into_exec(
        self: Box<Self>,
        log: slog::Logger,
        saga_id: SagaId,
        sec_hdl: SecExecClient,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error> {
        Ok(Arc::new(SagaExecutor::new_recover(
            log,
            saga_id,
            self.dag,
            self.registry,
            self.uctx,
            sec_hdl,
            self.saga_log,
        )?))
    }
}

/*
 * SEC internal client side (handle used by SagaExecutor)
 */

/**
 * Handle used by `SagaExecutor` for sending messages back to the SEC
 */
/* TODO-cleanup This should be pub(crate).  See lib.rs. */
#[derive(Debug)]
pub struct SecExecClient {
    saga_id: SagaId,
    exec_tx: mpsc::Sender<SecExecMsg>,
}

impl SecExecClient {
    /** Write `event` to the saga log */
    pub async fn record(&self, event: SagaNodeEvent) {
        assert_eq!(event.saga_id, self.saga_id);
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_send(
            ack_rx,
            SecExecMsg::LogEvent(SagaLogEventData { event, ack_tx }),
        )
        .await
    }

    /**
     * Update the cached state for the saga
     */
    pub async fn saga_update(&self, update: SagaCachedState) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_send(
            ack_rx,
            SecExecMsg::UpdateCachedState(SagaUpdateCacheData {
                ack_tx,
                saga_id: self.saga_id,
                updated_state: update,
            }),
        )
        .await
    }

    pub async fn saga_get(&self, saga_id: SagaId) -> Result<SagaView, ()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_send(
            ack_rx,
            SecExecMsg::SagaGet(SagaGetData { ack_tx, saga_id }),
        )
        .await
    }

    async fn sec_send<T>(
        &self,
        ack_rx: oneshot::Receiver<T>,
        msg: SecExecMsg,
    ) -> T {
        /*
         * TODO-robustness How does shutdown interact (if this channel gets
         * closed)?
         */
        self.exec_tx.send(msg).await.unwrap();
        ack_rx.await.unwrap()
    }
}

/**
 * Message passed from the [`SecExecClient`] to the [`Sec`]
 */
#[derive(Debug)]
enum SecExecMsg {
    /** Fetch the status of a saga */
    SagaGet(SagaGetData),

    /** Record an event to the saga log */
    LogEvent(SagaLogEventData),

    /** Update the cached state of a saga */
    UpdateCachedState(SagaUpdateCacheData),
}

/** See [`SecExecMsg::SagaGet`] */
/* TODO-cleanup commonize with the client's SagaGet */
#[derive(Debug)]
struct SagaGetData {
    /** response channel */
    ack_tx: oneshot::Sender<Result<SagaView, ()>>,
    /** saga being updated */
    saga_id: SagaId,
}

/** See [`SecExecMsg::LogEvent`] */
#[derive(Debug)]
struct SagaLogEventData {
    /** response channel */
    ack_tx: oneshot::Sender<()>,
    /** event to be recorded to the saga log */
    event: SagaNodeEvent,
}

/** See [`SecExecMsg::UpdateCachedState`] */
#[derive(Debug)]
struct SagaUpdateCacheData {
    /** response channel */
    ack_tx: oneshot::Sender<()>,
    /** saga being updated */
    saga_id: SagaId,
    /** updated state */
    updated_state: SagaCachedState,
}

/*
 * SEC server side (background task)
 */

/**
 * The `Sec` (Saga Execution Coordinator) is responsible for tracking and
 * running sagas
 *
 * Steno consumers create this via [`sec()`].
 */
struct Sec {
    log: slog::Logger,
    sagas: BTreeMap<SagaId, Saga>,
    sec_store: Arc<dyn SecStore>,
    futures: FuturesUnordered<BoxFuture<'static, Option<SecStep>>>,
    cmd_rx: mpsc::Receiver<SecClientMsg>,
    exec_tx: mpsc::Sender<SecExecMsg>,
    exec_rx: mpsc::Receiver<SecExecMsg>,
    shutdown: bool,
}

impl Sec {
    /** Body of the SEC's task */
    async fn run(mut self) {
        /*
         * Until we're asked to shutdown, wait for any sagas to finish or for
         * messages to be received on the command channel.
         *
         * It's important to avoid waiting for any Futures to complete in the
         * body of this loop aside from those that we're explicitly selecting
         * on.  Bad things can happen if such a Future were to block on some
         * operation that requires the loop in order to proceed.  For example,
         * the Futures generated by cmd_saga_get() and cmd_saga_list() both
         * block on the SagaExecutor, which can in turn block on the Sec in
         * order to write log entries to the SecStore.  It's critical that we're
         * able to respond to requests from the SagaExecutor to write log
         * entries to the SecStore even when we're blocked on that SagaExecutor
         * to fetch its status.  As much as possible, any time the Sec needs to
         * do async work, that should be wrapped in a Future that's inserted
         * into `self.futures`.  That way we can poll on it with all the other
         * work we have to do.
         *
         * Another failure mode to consider is if writes to the SecStore hang.
         * We still want status requests from the SecClient to complete.  This
         * is another reason to avoid any sort of blocking in the main loop.
         */
        info!(&self.log, "SEC running");
        while !self.shutdown || !self.futures.is_empty() {
            tokio::select! {
                /*
                 * Carry out any of the asynchronous work that the SEC does and
                 * wait for any of it to finish.
                 *
                 * The guard against `self.futures.is_empty()` might look
                 * surprising.  One might expect that if we attempt to fetch the
                 * next completed Future from an empty FuturesUnordered, the
                 * implementation would wait until a Future is added to the set
                 * and then completes before returning.  Instead, the
                 * implementaiton immediately returns `None`, with the side
                 * effect of terminating the Stream altogether.  As a result, we
                 * want to avoid waiting on an empty FuturesUnordered.
                 */
                maybe_work_done = self.futures.next(),
                    if !self.futures.is_empty() => {
                    /* This stream will never end. */
                    let work_result = maybe_work_done.unwrap();
                    if let Some(next_step) = work_result {
                        self.dispatch_work(next_step);
                    }
                },

                /* Handle messages from the client (Steno consumer) */
                maybe_client_message = self.cmd_rx.recv() => {
                    /*
                     * If we're already shutdown, the only wakeup here must
                     * result from the channel closing because the client does
                     * not allow messages to be sent after shutdown.  Relatedly,
                     * if the channel is closing, we must have already received
                     * a shutdown message because the client's Drop handler
                     * ensures that one will be sent.
                     */
                    assert_eq!(self.shutdown, maybe_client_message.is_none());
                    if let Some(client_message) = maybe_client_message {
                        self.dispatch_client_message(client_message);
                    }
                },

                /* Handle messages from running SagaExecutors */
                maybe_exec_message = self.exec_rx.recv() => {
                    /* Ignore errors from a closing channel. */
                    if let Some(exec_message) = maybe_exec_message {
                        self.dispatch_exec_message(exec_message);
                    }
                }
            }
        }
    }

    fn client_respond<T>(
        log: &slog::Logger,
        ack_tx: oneshot::Sender<T>,
        value: T,
    ) {
        if ack_tx.send(value).is_err() {
            warn!(log, "unexpectedly failed to send response to SEC client");
        }
    }

    /*
     * Dispatch functions for miscellaneous async work
     */

    fn dispatch_work(&mut self, step: SecStep) {
        match step {
            SecStep::SagaInsert(insert_data) => self.saga_insert(insert_data),
            SecStep::SagaDone(done_data) => self.saga_finished(done_data),
        }
    }

    fn saga_insert(&mut self, rec: SagaInsertData) {
        let saga_id = rec.saga_id;
        let serialized_dag = rec.serialized_dag;
        let ack_tx = rec.ack_tx;
        let log = rec.log;
        let exec_tx = self.exec_tx.clone();
        let sec_hdl = SecExecClient { saga_id, exec_tx };

        /* Prepare a channel used to wait for the saga to finish. */
        let (done_tx, done_rx) = oneshot::channel();

        /* Create the executor to run this saga. */
        let maybe_exec =
            rec.template_params.into_exec(log.new(o!()), saga_id, sec_hdl);
        if let Err(e) = maybe_exec {
            Sec::client_respond(&log, ack_tx, Err(e));
            return;
        }
        let exec = maybe_exec.unwrap();
        let run_state = Saga {
            id: saga_id,
            log: log.new(o!()),
            dag: serialized_dag,
            run_state: SagaRunState::Ready {
                exec: Arc::clone(&exec),
                waiter: done_tx,
            },
        };
        if self.sagas.get(&saga_id).is_some() {
            return Sec::client_respond(
                &log,
                ack_tx,
                Err(anyhow!(
                    "saga_id {} cannot be inserted; already in use",
                    saga_id
                )),
            );
        }
        assert!(self.sagas.insert(saga_id, run_state).is_none());

        if rec.autostart {
            self.do_saga_start(saga_id).unwrap();
        }

        /* Return a Future that the consumer can use to wait for the saga. */
        Sec::client_respond(
            &log,
            ack_tx,
            Ok(async move {
                /*
                 * It should not be possible for the receive to fail because the
                 * other side will not be closed while the saga is still
                 * running.
                 */
                done_rx.await.unwrap_or_else(|_| {
                    panic!("failed to wait for saga to finish")
                })
            }
            .boxed()),
        );
    }

    fn cmd_saga_start(
        &mut self,
        ack_tx: oneshot::Sender<Result<(), anyhow::Error>>,
        saga_id: SagaId,
    ) {
        let result = self.do_saga_start(saga_id);
        Sec::client_respond(&self.log, ack_tx, result);
    }

    fn do_saga_start(&mut self, saga_id: SagaId) -> Result<(), anyhow::Error> {
        let saga = self.saga_remove(saga_id)?;
        let log = saga.log;
        let dag = saga.dag;
        let (exec, waiter) = match saga.run_state {
            SagaRunState::Ready { exec, waiter } => (exec, waiter),
            _ => {
                return Err(anyhow!(
                    "saga not in \"ready\" state: {:?}",
                    saga_id
                ))
            }
        };

        self.sagas.insert(
            saga_id,
            Saga {
                id: saga_id,
                log,
                dag,
                run_state: SagaRunState::Running {
                    exec: Arc::clone(&exec),
                    waiter,
                },
            },
        );

        self.futures.push(
            async move {
                exec.run().await;
                Some(SecStep::SagaDone(SagaDoneData {
                    saga_id,
                    result: exec.result(),
                    status: exec.status().await,
                }))
            }
            .boxed(),
        );

        Ok(())
    }

    fn saga_finished(&mut self, done_data: SagaDoneData) {
        let saga_id = done_data.saga_id;
        let saga = self.sagas.remove(&saga_id).unwrap();
        info!(&saga.log, "saga finished");
        if let SagaRunState::Running { waiter, .. } = saga.run_state {
            Sec::client_respond(&saga.log, waiter, done_data.result.clone());
            self.sagas.insert(
                saga_id,
                Saga {
                    id: saga_id,
                    log: saga.log,
                    run_state: SagaRunState::Done {
                        status: done_data.status,
                        result: done_data.result,
                    },
                    dag: saga.dag,
                },
            );
        } else {
            panic!(
                "saga future completion for unexpected state: {:?}",
                saga.run_state
            );
        }
    }

    /*
     * Dispatch functions for consumer client messages
     */

    fn dispatch_client_message(&mut self, message: SecClientMsg) {
        match message {
            SecClientMsg::SagaCreate {
                ack_tx,
                saga_id,
                template_params,
                dag,
            } => {
                self.cmd_saga_create(ack_tx, saga_id, template_params, dag);
            }
            SecClientMsg::SagaResume {
                ack_tx,
                saga_id,
                template_params,
                dag,
            } => {
                self.cmd_saga_resume(ack_tx, saga_id, template_params, dag);
            }
            SecClientMsg::SagaStart { ack_tx, saga_id } => {
                self.cmd_saga_start(ack_tx, saga_id);
            }
            SecClientMsg::SagaList { ack_tx, marker, limit } => {
                self.cmd_saga_list(ack_tx, marker, limit);
            }
            SecClientMsg::SagaGet { ack_tx, saga_id } => {
                self.cmd_saga_get(ack_tx, saga_id);
            }
            SecClientMsg::SagaInjectError { ack_tx, saga_id, node_id } => {
                self.cmd_saga_inject_error(ack_tx, saga_id, node_id);
            }
            SecClientMsg::Shutdown => self.cmd_shutdown(),
        }
    }

    fn cmd_saga_create(
        &mut self,
        ack_tx: oneshot::Sender<
            Result<BoxFuture<'static, SagaResult>, anyhow::Error>,
        >,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        dag: Arc<SagaDag>,
    ) {
        self.do_saga_create(ack_tx, saga_id, template_params, dag, false);
    }

    fn do_saga_create(
        &mut self,
        ack_tx: oneshot::Sender<
            Result<BoxFuture<'static, SagaResult>, anyhow::Error>,
        >,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        dag: Arc<SagaDag>,
        autostart: bool,
    ) {
        let log = self.log.new(o!(
            "saga_id" => saga_id.to_string(),
            "saga_name" => dag.saga_name.to_string(),
        ));
        /* TODO-log Figure out the way to log JSON objects to a JSON drain */
        // TODO(AJS) - Get rid of this unwrap?
        let serialized_dag = serde_json::to_value(&dag)
            .map_err(ActionError::new_serialize)
            .context("serializing new saga dag")
            .unwrap();
        info!(&log, "saga create";
             "dag" => serde_json::to_string(&serialized_dag).unwrap()
        );

        /*
         * Before doing anything else, create a persistent record for this saga.
         */
        let saga_create = SagaCreateParams {
            id: saga_id,
            dag: serialized_dag.clone(),
            state: SagaCachedState::Running,
        };
        let store = Arc::clone(&self.sec_store);
        let create_future = async move {
            let result = store
                .saga_create(saga_create)
                .await
                .context("creating saga record");
            if let Err(error) = result {
                Sec::client_respond(&log, ack_tx, Err(error));
                None
            } else {
                Some(SecStep::SagaInsert(SagaInsertData {
                    ack_tx,
                    log,
                    saga_id,
                    template_params,
                    serialized_dag,
                    autostart,
                }))
            }
        }
        .boxed();

        self.futures.push(create_future);
    }

    fn cmd_saga_resume(
        &mut self,
        ack_tx: oneshot::Sender<
            Result<BoxFuture<'static, SagaResult>, anyhow::Error>,
        >,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        dag: Arc<SagaDag>,
    ) {
        let log = self.log.new(o!(
            "saga_id" => saga_id.to_string(),
            "saga_name" => dag.saga_name.to_string(),
        ));
        /* TODO-log Figure out the way to log JSON objects to a JSON drain */
        // TODO(AJS) - Get rid of this unwrap?
        let serialized_dag = serde_json::to_value(&dag)
            .map_err(ActionError::new_serialize)
            .context("serializing new saga dag")
            .unwrap();
        info!(&log, "saga resume";
             "dag" => serde_json::to_string(&serialized_dag).unwrap()
        );
        self.saga_insert(SagaInsertData {
            ack_tx,
            log,
            saga_id,
            template_params,
            serialized_dag,
            autostart: false,
        })
    }

    fn cmd_saga_list(
        &self,
        ack_tx: oneshot::Sender<Vec<SagaView>>,
        marker: Option<SagaId>,
        limit: NonZeroU32,
    ) {
        trace!(&self.log, "saga_list");
        /* TODO-cleanup */
        let log = self.log.new(o!());

        /*
         * We always expect to be able to go from NonZeroU32 to usize.  This
         * would only not be true on systems with usize < 32 bits, which seems
         * an unlikely target for us.
         */
        let limit = usize::try_from(limit.get()).unwrap();
        let futures = match marker {
            None => self
                .sagas
                .values()
                .take(limit)
                .map(SagaView::from_saga)
                .collect::<Vec<_>>(),
            Some(marker_value) => {
                use std::ops::Bound;
                self.sagas
                    .range((Bound::Excluded(marker_value), Bound::Unbounded))
                    .take(limit)
                    .map(|(_, v)| SagaView::from_saga(v))
                    .collect::<Vec<_>>()
            }
        };

        self.futures.push(
            async move {
                let views = futures::stream::iter(futures)
                    .then(|f| f)
                    .collect::<Vec<SagaView>>()
                    .await;
                Sec::client_respond(&log, ack_tx, views);
                None
            }
            .boxed(),
        );
    }

    /*
     * TODO-cleanup We should define a useful error type for the SEC.  This
     * function can only produce a NotFound, and we use `()` just to
     * communicate that there's only one kind of error here (so that the caller
     * can produce an appropriate NotFound instead of a generic error).
     */
    fn cmd_saga_get(
        &self,
        ack_tx: oneshot::Sender<Result<SagaView, ()>>,
        saga_id: SagaId,
    ) {
        trace!(&self.log, "saga_get"; "saga_id" => %saga_id);
        let maybe_saga = self.saga_lookup(saga_id);
        if maybe_saga.is_err() {
            Sec::client_respond(&self.log, ack_tx, Err(()));
            return;
        }

        let fut = SagaView::from_saga(maybe_saga.unwrap());
        let log = self.log.new(o!());
        let the_fut = async move {
            let saga_view = fut.await;
            Sec::client_respond(&log, ack_tx, Ok(saga_view));
            None
        };
        self.futures.push(the_fut.boxed());
    }

    fn cmd_saga_inject_error(
        &self,
        ack_tx: oneshot::Sender<Result<(), anyhow::Error>>,
        saga_id: SagaId,
        node_id: NodeIndex,
    ) {
        trace!(
            &self.log,
            "saga_inject_error";
            "saga_id" => %saga_id,
            "node_id" => ?node_id,
        );
        let maybe_saga = self.saga_lookup(saga_id);
        if let Err(e) = maybe_saga {
            Sec::client_respond(&self.log, ack_tx, Err(e));
            return;
        }

        let saga = maybe_saga.unwrap();
        let exec = match &saga.run_state {
            SagaRunState::Ready { exec, .. } => Arc::clone(exec),
            SagaRunState::Running { exec, .. } => Arc::clone(exec),
            SagaRunState::Done { .. } => {
                Sec::client_respond(
                    &self.log,
                    ack_tx,
                    Err(anyhow!("saga is not running: {}", saga_id)),
                );
                return;
            }
        };
        let log = self.log.new(o!());
        let fut = async move {
            exec.inject_error(node_id).await;
            Sec::client_respond(&log, ack_tx, Ok(()));
            None
        }
        .boxed();
        self.futures.push(fut);
    }

    fn cmd_shutdown(&mut self) {
        /*
         * TODO We probably want to stop executing any sagas that are running at
         * this point.
         */
        info!(&self.log, "initiating shutdown");
        self.shutdown = true;
    }

    /*
     * Dispatch functions for SagaExecutor messages
     */

    fn dispatch_exec_message(&mut self, exec_message: SecExecMsg) {
        let log = self.log.new(o!());
        let store = Arc::clone(&self.sec_store);
        match exec_message {
            SecExecMsg::LogEvent(log_data) => {
                self.futures
                    .push(Sec::executor_log(log, store, log_data).boxed());
            }
            SecExecMsg::UpdateCachedState(update_data) => {
                self.futures.push(
                    Sec::executor_update(log, store, update_data).boxed(),
                );
            }
            SecExecMsg::SagaGet(get_data) => {
                self.executor_saga_get(get_data);
            }
        };
    }

    async fn executor_log(
        log: slog::Logger,
        store: Arc<dyn SecStore>,
        log_data: SagaLogEventData,
    ) -> Option<SecStep> {
        debug!(&log, "saga log event";
            "new_state" => ?log_data.event
        );
        let ack_tx = log_data.ack_tx;
        store.record_event(log_data.event).await;
        Sec::client_respond(&log, ack_tx, ());
        None
    }

    async fn executor_update(
        log: slog::Logger,
        store: Arc<dyn SecStore>,
        update_data: SagaUpdateCacheData,
    ) -> Option<SecStep> {
        info!(&log, "update for saga cached state";
            "saga_id" => update_data.saga_id.to_string(),
            "new_state" => ?update_data.updated_state
        );
        let ack_tx = update_data.ack_tx;
        store.saga_update(update_data.saga_id, update_data.updated_state).await;
        Sec::client_respond(&log, ack_tx, ());
        None
    }

    fn executor_saga_get(&self, get_data: SagaGetData) {
        self.cmd_saga_get(get_data.ack_tx, get_data.saga_id);
    }

    fn saga_lookup(&self, saga_id: SagaId) -> Result<&Saga, anyhow::Error> {
        self.sagas
            .get(&saga_id)
            .ok_or_else(|| anyhow!("no such saga: {:?}", saga_id))
    }

    fn saga_remove(&mut self, saga_id: SagaId) -> Result<Saga, anyhow::Error> {
        self.sagas
            .remove(&saga_id)
            .ok_or_else(|| anyhow!("no such saga: {:?}", saga_id))
    }
}

/**
 * Represents the internal state of a saga in the [`Sec`]
 */
struct Saga {
    id: SagaId,
    log: slog::Logger,
    dag: serde_json::Value,
    run_state: SagaRunState,
}

#[derive(Debug)]
pub enum SagaRunState {
    /** Saga is ready to be run */
    Ready {
        /** Handle to executor (for status, etc.) */
        exec: Arc<dyn SagaExecManager>,
        /** Notify when the saga is done */
        waiter: oneshot::Sender<SagaResult>,
    },
    /** Saga is currently running */
    Running {
        /** Handle to executor (for status, etc.) */
        exec: Arc<dyn SagaExecManager>,
        /** Notify when the saga is done */
        waiter: oneshot::Sender<SagaResult>,
    },
    /** Saga has finished */
    Done {
        /** Final execution status */
        status: SagaExecStatus,
        /** Overall saga result */
        result: SagaResult,
    },
}

/**
 * Describes the next step that an SEC needs to take in order to process a
 * command, execute a saga, or any other asynchronous work
 *
 * This provides a uniform interface that can be processed in the body of the
 * SEC loop.
 *
 * In some cases, it would seem clearer to write straight-line async code to
 * handle a complete client request.  However, that code would wind up borrowing
 * the Sec (sometimes mutably) for the duration of async work.  It's important
 * to avoid that here in order to avoid deadlock or blocking all operations in
 * pathological conditions (e.g., when writes to the database hang).
 */
enum SecStep {
    /** Start tracking a new saga, either as part of "create" or "resume"  */
    SagaInsert(SagaInsertData),

    /** A saga has just finished. */
    SagaDone(SagaDoneData),
}

/** Data associated with [`SecStep::SagaInsert`] */
/*
 * TODO-cleanup This could probably be commonized with a struct that makes up
 * the body of the CreateSaga message.
 */
struct SagaInsertData {
    log: slog::Logger,
    saga_id: SagaId,
    template_params: Box<dyn TemplateParams>,
    serialized_dag: serde_json::Value,
    ack_tx:
        oneshot::Sender<Result<BoxFuture<'static, SagaResult>, anyhow::Error>>,
    autostart: bool,
}

/** Data associated with [`SecStep::SagaDone`] */
struct SagaDoneData {
    saga_id: SagaId,
    result: SagaResult,
    status: SagaExecStatus,
}

/// Simple file-based serialization and deserialization of a whole saga,
/// intended only for testing and debugging
#[derive(Deserialize, Serialize)]
pub struct SagaSerialized {
    pub saga_id: SagaId,
    pub dag: serde_json::Value,
    pub events: Vec<SagaNodeEvent>,
}

impl TryFrom<SagaSerialized> for SagaLog {
    type Error = anyhow::Error;
    fn try_from(s: SagaSerialized) -> Result<SagaLog, anyhow::Error> {
        SagaLog::new_recover(s.saga_id, s.events)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        ActionContext, ActionError, ActionFunc, DagBuilder, Node, SagaId,
        SagaName,
    };
    use serde::{Deserialize, Serialize};
    use slog::Drain;
    use std::sync::Mutex;
    use uuid::Uuid;

    fn new_log() -> slog::Logger {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    }

    fn new_sec(log: &slog::Logger) -> SecClient {
        crate::sec(
            log.new(slog::o!()),
            Arc::new(crate::InMemorySecStore::new()),
        )
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TestParams;

    // This context object is a dynamically typed bucket of
    // information for use by the following tests.
    //
    // It can be used by tests to monitor:
    // - Frequency of saga node execution
    #[derive(Debug)]
    struct TestContext {
        counters: Mutex<BTreeMap<String, u32>>,
    }

    impl TestContext {
        fn new() -> Self {
            TestContext { counters: Mutex::new(BTreeMap::new()) }
        }

        // Identifies that a function `name` has been called.
        fn call(&self, name: &str) {
            let mut map = self.counters.lock().unwrap();
            if let Some(count) = map.get_mut(name) {
                *count += 1;
            } else {
                map.insert(name.to_string(), 1);
            }
        }

        // Returns the number of times `name` has been called.
        fn get_count(&self, name: &str) -> u32 {
            let map = self.counters.lock().unwrap();
            if let Some(count) = map.get(name) {
                *count
            } else {
                0
            }
        }
    }

    #[derive(Debug)]
    struct TestSaga;
    impl SagaType for TestSaga {
        type ExecContextType = TestContext;
    }

    fn make_test_one_node_saga() -> (Arc<ActionRegistry<TestSaga>>, Arc<SagaDag>)
    {
        async fn do_n1(
            ctx: ActionContext<TestSaga>,
        ) -> Result<i32, ActionError> {
            ctx.user_data().call("do_n1");
            Ok(1)
        }
        async fn undo_n1(
            ctx: ActionContext<TestSaga>,
        ) -> Result<(), anyhow::Error> {
            ctx.user_data().call("undo_n1");
            Ok(())
        }

        let mut registry = ActionRegistry::new();
        let action_n1 = ActionFunc::new_action("n1_out", do_n1, undo_n1);
        registry.register(Arc::clone(&action_n1));

        let mut builder = DagBuilder::new(SagaName::new("test-saga"));
        builder.append(Node::action("n1_out", "n1", &*action_n1));
        (
            Arc::new(registry),
            Arc::new(SagaDag::new(
                builder.build().unwrap(),
                serde_json::to_value(TestParams {}).unwrap(),
            )),
        )
    }

    // Tests the "normal flow" for a newly created saga: create + start.
    #[tokio::test]
    async fn test_saga_create_and_start_executes_saga() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let saga_future = sec
            .saga_create(saga_id, Arc::clone(&context), dag, registry)
            .await
            .expect("failed to create saga");

        sec.saga_start(saga_id).await.expect("failed to start saga running");
        let result = saga_future.await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(context.get_count("do_n1"), 1);
        assert_eq!(context.get_count("undo_n1"), 0);
    }

    // Tests error injection skips execution of the actions, and fails the saga.
    #[tokio::test]
    async fn test_saga_fails_after_error_injection() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let saga_future = sec
            .saga_create(saga_id, Arc::clone(&context), dag, registry)
            .await
            .expect("failed to create saga");

        sec.saga_inject_error(saga_id, NodeIndex::new(0))
            .await
            .expect("failed to inject error");

        sec.saga_start(saga_id).await.expect("failed to start saga running");
        let result = saga_future.await;
        result.kind.expect_err("should have failed; we injected an error!");
        assert_eq!(context.get_count("do_n1"), 0);
        assert_eq!(context.get_count("undo_n1"), 0);
    }

    // Tests that omitting "start" after creation doesn't execute the saga.
    #[tokio::test]
    async fn test_saga_create_without_start_does_not_run_saga() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let saga_future = sec
            .saga_create(saga_id, Arc::clone(&context), dag, registry)
            .await
            .expect("failed to create saga");

        tokio::select! {
            _ = saga_future => {
                panic!("saga_create future shouldn't complete without start");
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1)) => {},
        }
        assert_eq!(context.get_count("do_n1"), 0);
        assert_eq!(context.get_count("undo_n1"), 0);
    }

    // Tests that resume + start executes the saga. This is the normal flow
    // for sagas which have been recovered from durable storage across
    // a reboot.
    #[tokio::test]
    async fn test_saga_resume_and_start_executes_saga() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let saga_future = sec
            .saga_resume(
                saga_id,
                Arc::clone(&context),
                serde_json::to_value(Arc::try_unwrap(dag).unwrap()).unwrap(),
                registry,
                vec![],
            )
            .await
            .expect("failed to resume saga");

        sec.saga_start(saga_id).await.expect("failed to start saga running");
        let result = saga_future.await;
        let output = result.kind.unwrap();
        assert_eq!(output.lookup_output::<i32>("n1_out").unwrap(), 1);
        assert_eq!(context.get_count("do_n1"), 1);
        assert_eq!(context.get_count("undo_n1"), 0);
    }

    // Tests that at *most* one of create + resume should be used for a saga,
    // or else the saga executor throws an error.
    #[tokio::test]
    async fn test_saga_resuming_already_created_saga_fails() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let _ = sec
            .saga_create(
                saga_id,
                Arc::clone(&context),
                dag.clone(),
                registry.clone(),
            )
            .await
            .expect("failed to create saga");

        let err = sec
            .saga_resume(
                saga_id,
                Arc::clone(&context),
                serde_json::to_value((*dag).clone()).unwrap(),
                registry,
                vec![],
            )
            .await
            .err()
            .expect("Resuming the saga should fail");

        assert!(err.to_string().contains("cannot be inserted; already in use"));
    }

    // Tests that started sagas must have previously been created (or resumed).
    #[tokio::test]
    async fn test_saga_start_without_create_fails() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let err = sec
            .saga_start(saga_id)
            .await
            .err()
            .expect("Starting an uncreated saga should fail");

        assert!(err.to_string().contains("no such saga"));
    }

    // Tests that sagas can only be started once.
    #[tokio::test]
    async fn test_sagas_can_only_be_started_once() {
        // Test setup
        let log = new_log();
        let sec = new_sec(&log);
        let (registry, dag) = make_test_one_node_saga();

        // Saga Creation
        let saga_id = SagaId(Uuid::new_v4());
        let context = Arc::new(TestContext::new());
        let _ = sec
            .saga_create(saga_id, Arc::clone(&context), dag, registry)
            .await
            .expect("failed to create saga");

        let _ = sec.saga_start(saga_id).await.expect("failed to start saga");
        let err = sec
            .saga_start(saga_id)
            .await
            .err()
            .expect("Double starting a saga should fail");
        assert!(err.to_string().contains("saga not in \"ready\" state"));
    }
}
