/*!
 * Interfaces for persistence of saga records and saga logs
 */
/* XXX TODO-doc This whole file */
/*
 * XXX The latest SEC changes are pretty ugly.  Probably a better way to do this
 * would be to have one big FuturesUnordered with Futures that produce an enum
 * with variants describing what to do next.  Then the body of the SEC is a
 * select on that FuturesUnordered and the command channel.  (Maybe we could
 * even phrase the command channel in terms of this, but it's not clear if
 * that's worth it.)
 * XXX As part of this cleanup, we ought to commonize the places where we call
 * send().  We'll also want to review all the places where we panic and consider
 * whether those are valid even in drop cases (as when the SecClient is dropped
 * with sagas running).
 */

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
            create_futures: FuturesUnordered::new(),
            simple_futures: FuturesUnordered::new(),
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
    create_futures:
        FuturesUnordered<BoxFuture<'static, Option<SagaInsertRecord>>>,
    simple_futures: FuturesUnordered<BoxFuture<'static, ()>>,
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
                maybe_create_done = self.create_futures.next(),
                    if !self.create_futures.is_empty() => {
                    if let Some(insert_record) = maybe_create_done.unwrap() {
                        self.saga_insert(insert_record);
                    }
                },
                maybe_simple = self.simple_futures.next(),
                    if !self.simple_futures.is_empty() => {
                    /* Nothing to do. */
                },

                cmd_result = self.cmd_rx.recv() => {
                    self.got_message(cmd_result);
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

    fn got_message(&mut self, msg_result: Option<SecClientMsg>) {
        /*
         * It shouldn't be possible to receive a message after processing a
         * shutdown request.  See the client's shutdown() method for details.
         */
        let clientmsg = msg_result.expect("error reading command");
        assert!(!self.shutdown);

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
        match clientmsg {
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
                serialized_params,
            } => {
                self.cmd_saga_resume(
                    ack_tx,
                    saga_id,
                    template_params,
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
        let store = Arc::clone(&self.sec_store);
        let create_future = async move {
            let result = store
                .saga_create(&saga_create)
                .await
                .context("creating saga record");
            if let Err(error) = result {
                ack_tx.send(Err(error)).unwrap_or_else(|_| {
                    panic!("failed to send response to SEC client")
                });
                None
            } else {
                Some(SagaInsertRecord {
                    ack_tx,
                    log,
                    saga_id,
                    template_params,
                    serialized_params,
                })
            }
        }
        .boxed();

        self.create_futures.push(create_future);
    }

    fn cmd_saga_resume(
        &mut self,
        ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
        saga_id: SagaId,
        template_params: Box<dyn TemplateParams>,
        serialized_params: JsonValue,
    ) {
        let log = self.log.new(o!("saga_id" => saga_id.to_string()));
        info!(&log, "saga resume");
        self.saga_insert(SagaInsertRecord {
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
        let futures =
            self.sagas.values().map(SagaView::from_saga).collect::<Vec<_>>();
        self.simple_futures.push(
            async move {
                let views = futures::stream::iter(futures)
                    .then(|f| f)
                    .collect::<Vec<SagaView>>()
                    .await;
                ack_tx.send(views).unwrap_or_else(|_| {
                    panic!("failed to send response to client")
                });
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
        if let Err(e) = maybe_saga {
            ack_tx.send(Err(())).unwrap_or_else(|_| panic!()); // XXX panic
            return;
        }

        let fut = SagaView::from_saga(maybe_saga.unwrap());
        let the_fut = async move {
            let saga_view = fut.await;
            ack_tx.send(Ok(saga_view)).unwrap_or_else(|_| panic!()); // XXX
        };
        self.simple_futures.push(the_fut.boxed());
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
            ack_tx.send(Err(e)).unwrap_or_else(|_| {
                panic!("failed to send response to client")
            });
            return;
        }

        let saga = maybe_saga.unwrap();
        let exec = if let SagaRunState::Running { exec, .. } = &saga.run_state {
            Arc::clone(&exec)
        } else {
            ack_tx
                .send(Err(anyhow!("saga is not running: {}", saga_id)))
                .unwrap_or_else(|_| {
                    panic!("failed to send response to client")
                });
            return;
        };
        let fut = async move {
            exec.inject_error(node_id).await;
            ack_tx.send(Ok(())).unwrap_or_else(|_| {
                panic!("failed to send response to client")
            });
        }
        .boxed();
        self.simple_futures.push(fut);
    }

    fn cmd_shutdown(&mut self) {
        /*
         * TODO We probably want to stop executing any sagas that are running at
         * this point.
         */
        info!(&self.log, "initiating shutdown");
        self.shutdown = true;
    }

    fn saga_insert(&mut self, rec: SagaInsertRecord) {
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
            ack_tx.send(Err(e)).unwrap_or_else(|_| {
                panic!("failed to send response to SEC client")
            });
            return;
        }
        let exec = maybe_exec.unwrap();
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
        ack_tx
            .send(Ok(async move {
                done_rx.await.unwrap_or_else(|_| {
                    panic!("failed to wait for saga to finish")
                })
            }
            .boxed()))
            .unwrap_or_else(|_| {
                panic!("failed to send response to SEC client")
            });
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

/* XXX TODO-doc */
/*
 * XXX could probably be commonized with a struct that makes up the body of the
 * CreateSaga message
 */
struct SagaInsertRecord {
    log: slog::Logger,
    saga_id: SagaId,
    template_params: Box<dyn TemplateParams>,
    serialized_params: JsonValue,
    ack_tx: oneshot::Sender<Result<BoxFuture<'static, ()>, anyhow::Error>>,
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
