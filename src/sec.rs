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
 * Creates a new Saga Execution Coordinator
 */
pub fn sec(log: slog::Logger, sec_store: Arc<dyn SecStore>) -> SecClient {
    // XXX buffer sizes
    let (cmd_tx, cmd_rx) = mpsc::channel(8); // XXX buffer size
                                             // XXX combine log and update channels?
    let (log_tx, log_rx) = mpsc::channel(8);
    let (update_tx, update_rx) = mpsc::channel(8);

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
            saga_futures: FuturesUnordered::new(),
            client_futures: FuturesUnordered::new(),
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
        template_name: String, // XXX
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
     * Shut down the SEC and wait for it to do so.
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
    async fn from_saga(saga: &Saga) -> Self {
        SagaView {
            id: saga.id,
            state: SagaStateView::from_run_state(&saga.run_state).await,
            params: saga.params.clone(),
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
    async fn from_run_state(run_state: &SagaRunState) -> SagaStateView {
        match run_state {
            SagaRunState::Running { exec, .. } => {
                SagaStateView::Running { status: exec.status().await }
            }
            SagaRunState::Done { status, result } => SagaStateView::Done {
                status: status.clone(),
                result: result.clone(),
            },
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
 * Message passed from the SecClient to the Sec
 */
/* XXX TODO would this be cleaner using separate named structs for the enums? */
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
            SecClientMsg::SagaResume { saga_id, template_params, .. } => f
                .debug_struct("SagaResume")
                .field("saga_id", saga_id)
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
 * The `Sec` (Saga Execution Coordinator) is responsible for tracking and
 * running sagas
 *
 * Steno consumers create this via [`sec()`].
 */
struct Sec {
    log: slog::Logger,
    sagas: BTreeMap<SagaId, Saga>,
    sec_store: Arc<dyn SecStore>,
    saga_futures: FuturesUnordered<
        BoxFuture<'static, (SagaId, SagaExecStatus, SagaResult)>,
    >,
    client_futures: FuturesUnordered<BoxFuture<'static, ()>>,
    cmd_rx: mpsc::Receiver<SecClientMsg>,
    log_tx: mpsc::Sender<(SagaId, SecSagaHdlMsgLog)>,
    log_rx: mpsc::Receiver<(SagaId, SecSagaHdlMsgLog)>,
    update_tx: mpsc::Sender<(SagaId, SagaCachedState)>,
    update_rx: mpsc::Receiver<(SagaId, SagaCachedState)>,
    shutdown: bool,
}

impl Sec {
    /** Body of the SEC's task */
    async fn run(mut self) {
        /*
         * Until we're asked to shutdown, wait for any sagas to finish or for
         * messages to be received on the command channel.
         */
        info!(&self.log, "SEC running");
        while !self.shutdown || !self.saga_futures.is_empty() {
            tokio::select! {
                /*
                 * One might expect that if we attempt to fetch the next
                 * completed Future from an empty FuturesUnordered, we might
                 * wait until a Future is added to the set and then completes.
                 * Instead, we get back None, which has the side effect of
                 * terminating the Stream.  As a result, we want to avoid
                 * waiting on an empty FuturesUnordered.
                 */
                maybe_result = self.saga_futures.next(),
                    if !self.saga_futures.is_empty() => {
                    let (saga_id, status, result) = maybe_result.unwrap();
                    self.saga_finished(saga_id, status, result);
                },
                maybe_cmd_done = self.client_futures.next(),
                    if !self.client_futures.is_empty() => { },

                cmd_result = self.cmd_rx.recv() => {
                    self.client_futures.push(self.got_message(cmd_result).boxed())
                },
                log_result = self.log_rx.recv() => {
                    let (saga_id, logmsg) =
                        log_result.expect("bad SecHdl message");
                    self.sec_store.record_event(saga_id, &logmsg.event).await;
                    /*
                     * `send` can only fail if the other side of the channel
                     * has closed.  That's illegal because the other side
                     * should be waiting for our acknowledgement.
                     */
                    assert!(logmsg.ack_tx.send(()).is_ok());
                },
                update_result = self.update_rx.recv() => {
                    /*
                     * We may get an error when the channel is closing due to
                     * execution completion.  That's fine -- just ignore it.
                     */
                    if let Some((saga_id, cached_state)) = update_result {
                        /*
                         * TODO-robustness this should be retried as needed?
                         */
                        self
                            .sec_store
                            .saga_update(saga_id, &cached_state)
                            .await;
                    }
                },
            }
        }
    }

    async fn got_message(&mut self, msg_result: Option<SecClientMsg>) {
        /*
         * It shouldn't be possible to receive a message after processing a
         * shutdown request.  See the client's shutdown() method for details.
         */
        assert!(!self.shutdown);
        let clientmsg = msg_result.expect("error reading command");
        /*
         * TODO-robustness We probably don't want to allow these command
         * functions to be async.  (Or rather: if they are, we want to put them
         * into some other FuturesUnordered that we can poll on in this loop.)
         * In practice, saga_list and saga_get wind up blocking on a Mutex
         * that's held while we write saga log entries.  It's conceivable this
         * could deadlock (since writing the log entries requires that we
         * receive messages on a channel, which happens via the the poll on
         * saga_futures.next() above).  Worse, it means if writes to CockroachDB
         * hang, we won't even be able to list the in-memory sagas.  This is
         * also a problem for saga_create(), which calls out to the store as
         * well.  XXX
         */
        self.cmd_dispatch(clientmsg).await
    }

    fn saga_finished(
        &mut self,
        saga_id: SagaId,
        status: SagaExecStatus,
        result: SagaResult,
    ) {
        let saga = self.sagas.remove(&saga_id).unwrap();
        info!(&saga.log, "saga finished");
        if let SagaRunState::Running { waiter, .. } = saga.run_state {
            if let Err(_) = waiter.send(()) {
                warn!(&saga.log, "saga waiter stopped listening");
            }
            self.sagas.insert(
                saga_id,
                Saga {
                    id: saga_id,
                    log: saga.log,
                    run_state: SagaRunState::Done { status, result },
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

    async fn cmd_dispatch(&mut self, clientmsg: SecClientMsg) {
        match clientmsg {
            SecClientMsg::SagaCreate {
                ack_tx,
                saga_id,
                template_params,
                template_name,
                serialized_params,
            } => {
                ack_tx
                    .send(
                        self.cmd_saga_create(
                            saga_id,
                            template_params,
                            template_name,
                            serialized_params,
                        )
                        .await,
                    )
                    .unwrap_or_else(|_| {
                        panic!("failed to send response to client")
                    });
            }
            SecClientMsg::SagaResume {
                ack_tx,
                saga_id,
                template_params,
                serialized_params,
            } => {
                ack_tx
                    .send(self.cmd_saga_resume(
                        saga_id,
                        template_params,
                        serialized_params,
                    ))
                    .unwrap_or_else(|_| {
                        panic!("failed to send response to client")
                    });
            }
            SecClientMsg::SagaList { ack_tx } => {
                let fut = self.cmd_saga_list();
                ack_tx.send(fut.await).unwrap_or_else(|_| {
                    panic!("failed to send response to client")
                });
            }
            SecClientMsg::SagaGet { ack_tx, saga_id } => {
                let fut = self.cmd_saga_get(saga_id);
                ack_tx.send(fut.await).unwrap_or_else(|_| {
                    panic!("failed to send response to client")
                });
            }
            SecClientMsg::SagaInjectError { ack_tx, saga_id, node_id } => {
                let fut = self.cmd_saga_inject(saga_id, node_id);
                ack_tx.send(fut.await).unwrap_or_else(|_| {
                    panic!("failed to send response to client");
                });
            }
            SecClientMsg::Shutdown => self.cmd_shutdown(),
        }
    }

    async fn cmd_saga_create(
        &mut self,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        template_name: String,
        serialized_params: JsonValue,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error> {
        /*
         * TODO-log would like template name, maybe parameters in the log
         * Ditto in cmd_saga_resume() XXX
         */
        let log = self.log.new(o!("saga_id" => saga_id.to_string()));
        info!(&log, "saga create");

        /*
         * Before doing anything else, create a persistent record for this saga.
         */
        let saga_create = SagaCreateParams {
            id: saga_id,
            template_name,
            saga_params: serialized_params.clone(),
        };
        self.sec_store
            .saga_create(&saga_create)
            .await
            .context("creating saga record")?;

        self.saga_insert(log, saga_id, template_params, serialized_params)
    }

    fn cmd_saga_resume(
        &mut self,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        serialized_params: JsonValue,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error> {
        let log = self.log.new(o!("saga_id" => saga_id.to_string()));
        info!(&log, "saga resume");
        self.saga_insert(log, saga_id, template_params, serialized_params)
    }

    fn cmd_saga_list<'a>(&'a self) -> impl Future<Output = Vec<SagaView>> + 'a {
        trace!(&self.log, "saga_list");
        let vec = self.sagas.values();
        async {
            futures::stream::iter(vec).then(SagaView::from_saga).collect().await
        }
    }

    fn cmd_saga_get<'a>(
        &'a self,
        saga_id: SagaId,
    ) -> impl Future<Output = Result<SagaView, ()>> + 'a {
        trace!(&self.log, "saga_get"; "saga_id" => %saga_id);
        let maybe_saga = self.sagas.get(&saga_id).ok_or(());
        async move { Ok(SagaView::from_saga(maybe_saga?).await) }
    }

    fn cmd_saga_inject<'a>(
        &'a self,
        saga_id: SagaId,
        node_id: NodeIndex,
    ) -> impl Future<Output = Result<(), anyhow::Error>> + 'a {
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
        async move {
            let saga = maybe_saga?;
            if let SagaRunState::Running { exec, .. } = &saga.run_state {
                exec.inject_error(node_id).await;
                Ok(())
            } else {
                Err(anyhow!("saga is not running: {}", saga_id))
            }
        }
    }

    fn cmd_shutdown(&mut self) {
        /*
         * TODO We probably want to stop executing any sagas that are running at
         * this point.
         */
        info!(&self.log, "initiating shutdown");
        self.shutdown = true;
    }

    fn saga_insert(
        &mut self,
        log: slog::Logger,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        serialized_params: JsonValue,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error> {
        let log_channel = self.log_tx.clone();
        let update_channel = self.update_tx.clone();
        let sec_hdl = SecSagaHdl { saga_id, log_channel, update_channel };

        /* Prepare a channel used to wait for the saga to finish. */
        let (done_tx, done_rx) = oneshot::channel();

        /* Create the executor to run this saga. */
        let exec =
            template_params.into_exec(log.new(o!()), saga_id, sec_hdl)?;
        let run_state = Saga {
            id: saga_id,
            log,
            params: serialized_params,
            run_state: SagaRunState::Running {
                exec: Arc::clone(&exec),
                waiter: done_tx,
            },
        };
        assert!(self.sagas.insert(saga_id, run_state).is_none());
        let saga_future = async move {
            exec.run().await;
            (saga_id, exec.status().await, exec.result())
        }
        .boxed();
        self.saga_futures.push(saga_future);

        /*
         * Return a Future that the consumer can use to wait for the saga to
         * finish.  It should not be possible for the receive to fail because
         * the other side will not be closed while the saga is still running.
         */
        Ok(async move {
            done_rx
                .await
                .unwrap_or_else(|_| panic!("failed to wait for saga to finish"))
        }
        .boxed())
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
    log_channel: mpsc::Sender<(SagaId, SecSagaHdlMsgLog)>,
    update_channel: mpsc::Sender<(SagaId, SagaCachedState)>,
}

impl SecSagaHdl {
    /** Write `event` to the saga log */
    pub async fn record(&self, event: SagaNodeEvent) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.log_channel
            .send((self.saga_id, SecSagaHdlMsgLog { event, ack_tx }))
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
        self.update_channel.send((self.saga_id, update)).await.unwrap();
    }
}

/**
 * Message from [`SagaExecutor`] to [`Sec`] (via [`SecSagaHdl`]) to record
 * an event to the saga log.
 */
#[derive(Debug)]
struct SecSagaHdlMsgLog {
    /** event to be recorded to the saga log */
    event: SagaNodeEvent,
    /** response channel */
    ack_tx: oneshot::Sender<()>,
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
