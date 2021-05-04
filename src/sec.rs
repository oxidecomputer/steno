/*!
 * Interfaces for persistence of saga records and saga logs
 */
/* XXX TODO-doc This whole file */

use crate::saga_exec::SagaExecutor;
use crate::store::SagaCachedState;
use crate::store::SagaCreateParams;
use crate::store::SecStore;
use crate::ActionError;
use crate::SagaExecManager;
use crate::SagaExecStatus;
use crate::SagaId;
use crate::SagaLog;
use crate::SagaNodeEvent;
use crate::SagaResult;
use crate::SagaTemplate;
use crate::SagaTemplateGeneric;
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
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::future::Future;
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
    // TODO-cleanup The log and update channels can probably be combined for
    // clarity.  It doesn't make much functional difference.
    let (cmd_tx, cmd_rx) = mpsc::channel(SEC_CLIENT_MAXQ_MESSAGES);
    let (log_tx, log_rx) = mpsc::channel(SEC_EXEC_MAXQ_MESSAGES);
    let (update_tx, update_rx) = mpsc::channel(SEC_EXEC_MAXQ_MESSAGES);

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
            log_tx,
            log_rx,
            update_tx,
            update_rx,
        };

        sec.run().await
    });
    let client = SecClient { cmd_tx, task: Some(task), shutdown: false };
    client
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
     * Create a new saga and start it running
     *
     * This function asynchronously returns a `Future` that can be used to wait
     * for the saga to finish.  It's also safe to cancel (drop) this Future.
     */
    pub async fn saga_create<UserType>(
        &self,
        saga_id: SagaId,
        uctx: Arc<UserType::ExecContextType>,
        template: Arc<SagaTemplate<UserType>>,
        template_name: String,
        params: UserType::SagaParamsType,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error>
    where
        UserType: SagaType + fmt::Debug,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let serialized_params = serde_json::to_value(&params)
            .map_err(ActionError::new_serialize)
            .context("serializing new saga parameters")?;
        let template_params =
            Box::new(TemplateParamsForCreate { template, params, uctx })
                as Box<dyn TemplateParams>;
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaCreate {
                ack_tx,
                saga_id,
                template_params,
                template_name,
                serialized_params,
            },
        )
        .await
    }

    /**
     * Resume a saga that was previously running
     *
     * This function asynchronously returns a `Future` that can be used to wait
     * for the saga to finish.  It's also safe to cancel (drop) this Future.
     *
     * Unlike `saga_create`, this function is not parametrized by a [`SagaType`]
     * because the assumption is that the caller doesn't statically know what
     * that type is.  (The caller would usually have obtained a
     * [`SagaTemplateGeneric`] by looking it up in a table that contains various
     * different saga templates with varying SagaTypes.)
     */
    pub async fn saga_resume<T>(
        &self,
        saga_id: SagaId,
        uctx: Arc<T>,
        template: Arc<dyn SagaTemplateGeneric<T>>,
        template_name: String,
        params: JsonValue,
        log_events: Vec<SagaNodeEvent>,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error>
    where
        T: Send + Sync + fmt::Debug + 'static,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let saga_log = SagaLog::new_recover(saga_id, log_events)
            .context("recovering log")?;
        let template_params = Box::new(TemplateParamsForRecover {
            template,
            params: params.clone(),
            uctx,
            saga_log,
        }) as Box<dyn TemplateParams>;
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaResume {
                ack_tx,
                saga_id,
                template_params,
                template_name,
                serialized_params: params,
            },
        )
        .await
    }

    /**
     * List known sagas
     */
    pub async fn saga_list(&self) -> Vec<SagaView> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.sec_cmd(ack_rx, SecClientMsg::SagaList { ack_tx }).await
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

/* TODO-cleanup Is this necessary?  Correct? */
impl Drop for SecClient {
    fn drop(&mut self) {
        if !self.shutdown {
            /*
             * If we get here, there must be no outstanding requests on this
             * channel.  Thus, there must be buffer space, and try_send() ought
             * not to fail for running out of space.  It also ought not to fail
             * because the other side is closed either.  See shutdown() for
             * details.
             */
            self.cmd_tx.try_send(SecClientMsg::Shutdown).unwrap_or_else(
                |error| panic!("failed to send message to SEC: {:#}", error),
            );
        }
    }
}

/** External consumer's view of a saga */
#[derive(Debug, Clone)]
pub struct SagaView {
    pub id: SagaId,
    pub state: SagaStateView,

    params: JsonValue,
}

impl SagaView {
    fn from_saga(saga: &Saga) -> impl Future<Output = Self> {
        let id = saga.id;
        let params = saga.params.clone();
        let fut = SagaStateView::from_run_state(&saga.run_state);
        async move {
            let state = fut.await;
            SagaView { id, state, params }
        }
    }

    pub fn serialized(&self) -> SagaSerialized {
        SagaSerialized {
            saga_id: self.id,
            params: self.params.clone(),
            events: self.state.status().log().events().to_vec(),
        }
    }
}

/** State-specific parts of a consumer's view of a saga */
#[derive(Debug, Clone)]
pub enum SagaStateView {
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
            Running(Arc<dyn SagaExecManager>),
            Done(SagaExecStatus, SagaResult),
        }

        let which = match run_state {
            SagaRunState::Running { exec, .. } => {
                Which::Running(Arc::clone(&exec))
            }
            SagaRunState::Done { status, result } => {
                Which::Done(status.clone(), result.clone())
            }
        };

        async move {
            match which {
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
            SagaStateView::Running { status } => &status,
            SagaStateView::Done { status, .. } => &status,
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
        ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
        /** caller-defined id (must be unique) */
        saga_id: SagaId,
        /** user-type-specific parameters */
        template_params: Box<dyn TemplateParams>,
        /** name of the template used to create this saga */
        template_name: String,
        /** serialized saga parameters */
        serialized_params: JsonValue,
    },

    /**
     * Resumes a saga from a previous lifetime (i.e., after a restart)
     *
     * The response includes a Future that can be used to wait for the saga to
     * finish.  The caller can ignore this.
     */
    SagaResume {
        /** response channel */
        ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
        /** unique id of the saga (from persistent state) */
        saga_id: SagaId,
        /** user-type-specific parameters */
        template_params: Box<dyn TemplateParams>,
        /** name of the template used to resume this saga */
        template_name: String,
        /** serialized saga parameters */
        serialized_params: JsonValue,
    },

    /** List all sagas */
    SagaList {
        /** response channel */
        ack_tx: oneshot::Sender<Vec<SagaView>>,
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
                saga_id,
                template_params,
                template_name,
                ..
            } => f
                .debug_struct("SagaCreate")
                .field("saga_id", saga_id)
                .field("template_params", template_params)
                .field("template_name", template_name)
                .finish(),
            SecClientMsg::SagaResume {
                saga_id,
                template_params,
                template_name,
                ..
            } => f
                .debug_struct("SagaResume")
                .field("saga_id", saga_id)
                .field("template_name", template_name)
                .field("template_params", template_params)
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
        }
    }
}

/**
 * This trait erases the type parameters on a [`SagaTemplate`], user context,
 * and user parameters so that we can more easily pass it through a channel.
 */
trait TemplateParams: Send + fmt::Debug {
    fn into_exec(
        self: Box<Self>,
        log: slog::Logger,
        saga_id: SagaId,
        sec_hdl: SecSagaHdl,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error>;
}

/**
 * Stores a template, saga parameters, and user context in a way where the
 * user-defined types can be erased with [`TemplateParams`]
 *
 * This version is for the "create" case, where we know the specific
 * [`SagaType`] for these values.  See [`SecClient::saga_create`].
 */
#[derive(Debug)]
struct TemplateParamsForCreate<UserType: SagaType + fmt::Debug> {
    template: Arc<SagaTemplate<UserType>>,
    params: UserType::SagaParamsType,
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
        sec_hdl: SecSagaHdl,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error> {
        Ok(Arc::new(SagaExecutor::new(
            log,
            saga_id,
            self.template,
            self.uctx,
            self.params,
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
struct TemplateParamsForRecover<T: Send + Sync + fmt::Debug> {
    template: Arc<dyn SagaTemplateGeneric<T>>,
    params: JsonValue,
    uctx: Arc<T>,
    saga_log: SagaLog,
}

impl<T> TemplateParams for TemplateParamsForRecover<T>
where
    T: Send + Sync + fmt::Debug,
{
    fn into_exec(
        self: Box<Self>,
        log: slog::Logger,
        saga_id: SagaId,
        sec_hdl: SecSagaHdl,
    ) -> Result<Arc<dyn SagaExecManager>, anyhow::Error> {
        Ok(self.template.recover(
            log,
            saga_id,
            self.uctx,
            self.params,
            sec_hdl,
            self.saga_log,
        )?)
    }
}

/*
 * SEC server side (background task)
 */

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
    serialized_params: JsonValue,
    ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
}

/** Data associated with [`SecStep::SagaDone`] */
struct SagaDoneData {
    saga_id: SagaId,
    result: SagaResult,
    status: SagaExecStatus,
}

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
    log_tx: mpsc::Sender<SagaLogEventData>,
    log_rx: mpsc::Receiver<SagaLogEventData>,
    update_tx: mpsc::Sender<SagaUpdateCacheData>,
    update_rx: mpsc::Receiver<SagaUpdateCacheData>,
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
                 * One might expect that if we attempt to fetch the next
                 * completed Future from an empty FuturesUnordered, the
                 * implementation would wait until a Future is added to the set
                 * and then completes before returning.  Instead, we immediately
                 * get back None, which has the side effect of terminating the
                 * Stream.  As a result, we want to avoid waiting on an empty
                 * FuturesUnordered.
                 */
                maybe_result = self.futures.next(),
                    if !self.futures.is_empty() => {
                    /* This stream will never end. */
                    let future_result = maybe_result.unwrap();
                    if let Some(next_step) = future_result {
                        self.dispatch_work(next_step);
                    }
                },

                cmd_result = self.cmd_rx.recv() => {
                    self.dispatch_client_message(cmd_result);
                },

                log_result = self.log_rx.recv() => {
                    /* Ignore errors from a closing channel. */
                    if let Some(log_data) = log_result {
                        self.dispatch_exec_log(log_data);
                    }
                }

                update_result = self.update_rx.recv() => {
                    /* Ignore errors from a closing channel. */
                    if let Some(update_data) = update_result {
                        /* TODO-robustness this should be retried as needed? */
                        self.dispatch_exec_cache_update(update_data);
                    }
                },
            }
        }
    }

    fn dispatch_work(&mut self, step: SecStep) {
        match step {
            SecStep::SagaInsert(insert_data) => self.saga_insert(insert_data),
            SecStep::SagaDone(done_data) => self.saga_finished(done_data),
        }
    }

    fn client_respond<T>(
        log: &slog::Logger,
        ack_tx: oneshot::Sender<T>,
        value: T,
    ) {
        if let Err(_) = ack_tx.send(value) {
            warn!(log, "unexpectedly failed to send response to SEC client");
        }
    }

    fn saga_insert(&mut self, rec: SagaInsertData) {
        let saga_id = rec.saga_id;
        let serialized_params = rec.serialized_params;
        let ack_tx = rec.ack_tx;
        let log = rec.log;
        let log_channel = self.log_tx.clone();
        let update_channel = self.update_tx.clone();
        let sec_hdl = SecSagaHdl { saga_id, log_channel, update_channel };

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
            params: serialized_params,
            run_state: SagaRunState::Running {
                exec: Arc::clone(&exec),
                waiter: done_tx,
            },
        };
        assert!(self.sagas.insert(saga_id, run_state).is_none());
        let saga_future = async move {
            exec.run().await;
            Some(SecStep::SagaDone(SagaDoneData {
                saga_id,
                result: exec.result(),
                status: exec.status().await,
            }))
        }
        .boxed();
        self.futures.push(saga_future);

        /*
         * Return a Future that the consumer can use to wait for the saga to
         * finish.  It should not be possible for the receive to fail because
         * the other side will not be closed while the saga is still running.
         */
        Sec::client_respond(
            &log,
            ack_tx,
            Ok(async move {
                done_rx.await.unwrap_or_else(|_| {
                    panic!("failed to wait for saga to finish")
                })
            }
            .boxed()),
        );
    }

    fn saga_finished(&mut self, done_data: SagaDoneData) {
        let saga_id = done_data.saga_id;
        let saga = self.sagas.remove(&saga_id).unwrap();
        info!(&saga.log, "saga finished");
        if let SagaRunState::Running { waiter, .. } = saga.run_state {
            Sec::client_respond(&saga.log, waiter, ());
            self.sagas.insert(
                saga_id,
                Saga {
                    id: saga_id,
                    log: saga.log,
                    run_state: SagaRunState::Done {
                        status: done_data.status,
                        result: done_data.result,
                    },
                    params: saga.params,
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
     * Dispatch functions for SagaExecutor messages
     */

    fn dispatch_exec_log(&mut self, log_data: SagaLogEventData) {
        let log = self.log.new(o!());
        debug!(&log, "saga log event";
            "saga_id" => log_data.saga_id.to_string(),
            "new_state" => ?log_data.event
        );
        let store = Arc::clone(&self.sec_store);
        let future = async move {
            store.record_event(log_data.saga_id, &log_data.event).await;
            Sec::client_respond(&log, log_data.ack_tx, ());
            None
        }
        .boxed();
        self.futures.push(future);
    }

    fn dispatch_exec_cache_update(&mut self, update_data: SagaUpdateCacheData) {
        info!(&self.log, "update for saga cached state";
            "saga_id" => update_data.saga_id.to_string(),
            "new_state" => ?update_data.updated_state
        );
        let store = Arc::clone(&self.sec_store);
        self.futures.push(
            async move {
                store
                    .saga_update(
                        update_data.saga_id,
                        &update_data.updated_state,
                    )
                    .await;
                None
            }
            .boxed(),
        );
    }

    /*
     * Dispatch functions for consumer client messages
     */

    fn dispatch_client_message(&mut self, msg_result: Option<SecClientMsg>) {
        if msg_result.is_none() {
            /*
             * The client always sends a shutdown message as part of drop, so it
             * should not be possible for this channel to close before receiving
             * that message.
             */
            assert!(self.shutdown);
            return;
        }

        /*
         * It shouldn't be possible to receive any messages after shutdown
         * because shutdown is only sent by the client when the consumer has
         * given up its reference to the client.
         */
        assert!(!self.shutdown);
        match msg_result.unwrap() {
            SecClientMsg::SagaCreate {
                ack_tx,
                saga_id,
                template_params,
                template_name,
                serialized_params,
            } => {
                self.cmd_saga_create(
                    ack_tx,
                    saga_id,
                    template_params,
                    template_name,
                    serialized_params,
                );
            }
            SecClientMsg::SagaResume {
                ack_tx,
                saga_id,
                template_params,
                template_name,
                serialized_params,
            } => {
                self.cmd_saga_resume(
                    ack_tx,
                    saga_id,
                    template_params,
                    template_name,
                    serialized_params,
                );
            }
            SecClientMsg::SagaList { ack_tx } => {
                self.cmd_saga_list(ack_tx);
            }
            SecClientMsg::SagaGet { ack_tx, saga_id } => {
                self.cmd_saga_get(ack_tx, saga_id);
            }
            SecClientMsg::SagaInjectError { ack_tx, saga_id, node_id } => {
                self.cmd_saga_inject(ack_tx, saga_id, node_id);
            }
            SecClientMsg::Shutdown => self.cmd_shutdown(),
        }
    }

    fn cmd_saga_create(
        &mut self,
        ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        template_name: String,
        serialized_params: JsonValue,
    ) {
        let log = self.log.new(o!(
            "saga_id" => saga_id.to_string(),
            "template_name" => template_name.clone()
        ));
        /* TODO-log Figure out the way to log JSON objects to a JSON drain */
        info!(&log, "saga create";
            "params" => serde_json::to_string(&serialized_params).unwrap()
        );

        /*
         * Before doing anything else, create a persistent record for this saga.
         */
        let saga_create = SagaCreateParams {
            id: saga_id,
            template_name,
            saga_params: serialized_params.clone(),
        };
        let store = Arc::clone(&self.sec_store);
        let create_future = async move {
            let result = store
                .saga_create(&saga_create)
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
                    serialized_params,
                }))
            }
        }
        .boxed();

        self.futures.push(create_future);
    }

    fn cmd_saga_resume(
        &mut self,
        ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        template_name: String,
        serialized_params: JsonValue,
    ) {
        let log = self.log.new(o!(
            "saga_id" => saga_id.to_string(),
            "template_name" => template_name,
        ));
        /* TODO-log Figure out the way to log JSON objects to a JSON drain */
        info!(&log, "saga resume";
            "params" => serde_json::to_string(&serialized_params).unwrap()
        );
        self.saga_insert(SagaInsertData {
            ack_tx,
            log,
            saga_id,
            template_params,
            serialized_params,
        })
    }

    fn cmd_saga_list(&self, ack_tx: oneshot::Sender<Vec<SagaView>>) {
        trace!(&self.log, "saga_list");
        /* TODO-cleanup */
        let log = self.log.new(o!());
        let futures =
            self.sagas.values().map(SagaView::from_saga).collect::<Vec<_>>();
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

    fn cmd_saga_get(
        &self,
        ack_tx: oneshot::Sender<Result<SagaView, ()>>,
        saga_id: SagaId,
    ) {
        trace!(&self.log, "saga_get"; "saga_id" => %saga_id);
        let maybe_saga = self.sagas.get(&saga_id).ok_or(());
        if let Err(_) = maybe_saga {
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

    fn cmd_saga_inject(
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
        let maybe_saga = self
            .sagas
            .get(&saga_id)
            .ok_or_else(|| anyhow!("no such saga: {}", saga_id));

        if let Err(e) = maybe_saga {
            Sec::client_respond(&self.log, ack_tx, Err(e));
            return;
        }

        let saga = maybe_saga.unwrap();
        let exec = if let SagaRunState::Running { exec, .. } = &saga.run_state {
            Arc::clone(&exec)
        } else {
            Sec::client_respond(
                &self.log,
                ack_tx,
                Err(anyhow!("saga is not running: {}", saga_id)),
            );
            return;
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
}

struct Saga {
    id: SagaId,
    log: slog::Logger,
    params: JsonValue,
    run_state: SagaRunState,
}

#[derive(Debug)]
pub enum SagaRunState {
    /** Saga is currently running */
    Running {
        /** Handle to executor (for status, etc.) */
        exec: Arc<dyn SagaExecManager>,
        /** Notify when the saga is done */
        waiter: oneshot::Sender<()>,
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
 * Handle used by [`SagaExecutor`] for sending messages back to the SEC
 */
/* XXX TODO-cleanup This should be pub(crate).  See lib.rs. */
#[derive(Debug)]
pub struct SecSagaHdl {
    saga_id: SagaId,
    log_channel: mpsc::Sender<SagaLogEventData>,
    update_channel: mpsc::Sender<SagaUpdateCacheData>,
}

impl SecSagaHdl {
    /** Write `event` to the saga log */
    pub async fn record(&self, event: SagaNodeEvent) {
        /*
         * XXX How does shutdown interact here (if these channels get closed)?
         */
        let (ack_tx, ack_rx) = oneshot::channel();
        self.log_channel
            .send(SagaLogEventData { saga_id: self.saga_id, event, ack_tx })
            .await
            .unwrap();
        ack_rx.await.unwrap()
    }

    /**
     * Update the cached state for the saga
     *
     * This does not block on any write to persistent storage because that is
     * not required for correctness here.
     */
    pub async fn saga_update(&self, update: SagaCachedState) {
        /*
         * XXX How does shutdown interact here (if these channels get closed)?
         */
        self.update_channel
            .send(SagaUpdateCacheData {
                saga_id: self.saga_id,
                updated_state: update,
            })
            .await
            .unwrap();
    }
}

/**
 * Message from [`SagaExecutor`] to [`Sec`] (via [`SecSagaHdl`]) to record
 * an event to the saga log.
 */
#[derive(Debug)]
struct SagaLogEventData {
    /** saga being updated */
    saga_id: SagaId,
    /** event to be recorded to the saga log */
    event: SagaNodeEvent,
    /** response channel */
    ack_tx: oneshot::Sender<()>,
}

/**
 * Message from [`SagaExecutor`'] to [`Sec`] (via [`SecSagaHdl`]) to update the
 * cached state of a saga.
 */
#[derive(Debug)]
struct SagaUpdateCacheData {
    /** saga being updated */
    saga_id: SagaId,
    /** updated state */
    updated_state: SagaCachedState,
}

/*
 * Very simple file-based serialization and deserialization, intended only for
 * testing and debugging
 */

#[derive(Deserialize, Serialize)]
pub struct SagaSerialized {
    pub saga_id: SagaId,
    pub params: JsonValue,
    pub events: Vec<SagaNodeEvent>,
}

impl TryFrom<SagaSerialized> for SagaLog {
    type Error = anyhow::Error;
    fn try_from(s: SagaSerialized) -> Result<SagaLog, anyhow::Error> {
        SagaLog::new_recover(s.saga_id, s.events)
    }
}
