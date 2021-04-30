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
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use petgraph::graph::NodeIndex;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

/*
 * SEC client side (handle used by Steno consumers)
 */

/**
 * Creates a new Saga Execution Coordinator
 */
pub fn sec(log: slog::Logger, sec_store: Arc<dyn SecStore>) -> SecClient {
    let (cmd_tx, cmd_rx) = mpsc::channel(8); // XXX buffer size
    let sec = Sec {
        log,
        sagas: BTreeMap::new(),
        sec_store,
        saga_futures: FuturesUnordered::new(),
    };

    /*
     * We spawn a new task rather than return a `Future` for the caller to
     * poll because we want to make sure the Sec can't be dropped unless
     * shutdown() has been invoked on the client.
     */
    let task = tokio::spawn(sec.run());
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
        saga_log: SagaLog,
    ) -> Result<BoxFuture<'static, ()>, anyhow::Error>
    where
        T: Send + Sync + fmt::Debug + 'static,
    {
        let (ack_tx, ack_rx) = oneshot::channel();
        let template_params = Box::new(TemplateParamsForRecover {
            template,
            params,
            uctx,
            saga_log,
        }) as Box<dyn TemplateParams>;
        self.sec_cmd(
            ack_rx,
            SecClientMsg::SagaResume { ack_tx, saga_id, template_params },
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
            SecClientMsg::Shutdown { .. } => f.write_str("Shutdown"),
        }
    }
}

/**
 * This trait erases the type parameters on a [`SagaTemplate`], user context,
 * and user parameters so that we can more easily pass it through a channel.
 */
trait TemplateParams: fmt::Debug {
    fn into_exec(
        self,
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
        self,
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
        self,
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
    saga_futures: FuturesUnordered<BoxFuture<'static, SagaId>>,
}

pub struct Saga {
    log: slog::Logger,
    params: JsonValue,
    run_state: SagaRunState,
}

pub enum SagaRunState {
    Running { exec: Arc<dyn SagaExecManager> },
    Done { status: SagaExecStatus, result: SagaResult },
}

// XXX
// impl SagaView {
//     pub fn serialized(&self) -> SagaSerialized {
//         let status = self.state.status();
//         SagaSerialized {
//             saga_id: self.id,
//             params: self.params.clone(),
//             events: status.log().events().to_vec(),
//         }
//     }
// }

impl Sec {
    async fn run(self) {
        // XXX
        futures::future::pending().await
    }
}
//     // XXX Should this return a Future that waits on this saga?
//     pub async fn saga_create<UserType>(
//         &mut self,
//         uctx: Arc<UserType::ExecContextType>,
//         saga_id: SagaId,
//         template: Arc<SagaTemplate<UserType>>,
//         template_name: String, // XXX
//         params: UserType::SagaParamsType,
//     ) -> Result<(), anyhow::Error>
//     where
//         UserType: SagaType,
//     {
//         /* TODO-log would like template name, maybe parameters in the log */
//         let log = self.log.new(o!("id" => saga_id.to_string()));
//         info!(&log, "saga create");
//
//         /*
//          * Before doing anything else, create a persistent record for this saga.
//          */
//         let serialized_params = serde_json::to_value(&params)
//             .map_err(ActionError::new_serialize)
//             .context("serializing initial saga params")?;
//         let saga_create = SagaCreateParams {
//             id: saga_id,
//             template_name,
//             saga_params: serialized_params.clone(),
//         };
//         self.sec_store
//             .saga_create(&saga_create)
//             .await
//             .context("creating saga record")?;
//
//         /*
//          * Now create an executor to run this saga.
//          */
//         let (internal, log_rx, update_rx) = self.saga_handle(saga_id);
//         let exec = Arc::new(SagaExecutor::new(
//             log.new(o!()),
//             saga_id,
//             template,
//             uctx,
//             params,
//             internal,
//         ));
//         let run_state = Saga {
//             log,
//             params: serialized_params,
//             run_state: SagaRunState::Running { exec: Arc::clone(exec) },
//         };
//         assert!(self.sagas.insert(saga_id, run_state).is_none());
//         let saga_future = self.run_saga(saga_id, exec, log_rx, update_rx);
//         self.saga_futures.push(saga_future);
//         Ok(())
//     }
//
//     pub fn saga_resume<T>(
//         &mut self,
//         uctx: Arc<T>,
//         saga_id: SagaId,
//         template: Arc<dyn SagaTemplateGeneric<T>>,
//         params: JsonValue,
//         saga_log: SagaLog,
//     ) -> Result<(), anyhow::Error>
//     where
//         T: Send + Sync,
//     {
//         let log = self.log.new(o!("id" => saga_id.to_string()));
//         /* TODO-log would like template name, maybe parameters in the log */
//         info!(&self.log, "saga resume"; "id" => %saga_id);
//
//         let (internal, log_rx, update_rx) = self.saga_handle(saga_id);
//         let exec = Arc::new(template.recover(
//             log.new(o!()),
//             saga_id,
//             uctx,
//             params.clone(),
//             internal,
//             saga_log,
//         )?);
//         let run_state = Saga {
//             log,
//             params,
//             run_state: SagaRunState::Running { exec: Arc::clone(&exec) },
//         };
//         assert!(self.sagas.insert(saga_id, run_state).is_none());
//         let saga_future = self.run_saga(saga_id, exec, log_rx, update_rx);
//         self.saga_futures.push(saga_future);
//         Ok(())
//     }
//
//     pub async fn saga_inject_error(
//         &mut self,
//         saga_id: SagaId,
//         node_id: NodeIndex,
//     ) -> Result<(), anyhow::Error> {
//         let saga = self
//             .sagas
//             .get(&saga_id)
//             .ok_or_else(|| anyhow!("no saga with id \"{}\"", saga_id))?;
//         match &saga.run_state {
//             SagaRunState::Running { exec } => {
//                 exec.inject_error(node_id).await;
//                 Ok(())
//             }
//             SagaRunState::Done { .. } => Err(anyhow!("saga is already done")),
//         }
//     }
//
//     pub async fn saga_get_state(
//         &self,
//         saga_id: SagaId,
//     ) -> Result<SagaView, anyhow::Error> {
//         let saga = &self
//             .sagas
//             .get(&saga_id)
//             .ok_or_else(|| anyhow!("no saga with id \"{}\"", saga_id))?;
//         let state_view = match &saga.run_state {
//             SagaRunState::Running { exec } => {
//                 SagaStateView::Running { status: exec.status().await }
//             }
//             SagaRunState::Done { status, result } => SagaStateView::Done {
//                 status: status.clone(),
//                 result: result.clone(),
//             },
//         };
//         Ok(SagaView {
//             id: saga_id,
//             state: state_view,
//             params: saga.params.clone(),
//         })
//     }
//
//     fn saga_handle(
//         &self,
//         saga_id: SagaId,
//     ) -> (SecSagaHdl, mpsc::Receiver<SecMsgLog>, watch::Receiver<SagaCachedState>)
//     {
//         let (log_tx, log_rx) = mpsc::channel(1);
//         // XXX Is the initial value really Running?
//         let (update_tx, update_rx) = watch::channel(SagaCachedState::Running);
//         // XXX Need to actually listen on these channels at some point!
//         let si = SecSagaHdl { log_channel: log_tx, update_channel: update_tx };
//         (si, log_rx, update_rx)
//     }
//
//     async fn run_saga(
//         &self,
//         saga_id: SagaId,
//         exec: Arc<dyn SagaExecManager>,
//         log_rx: mpsc::Receiver<SecMsgLog>,
//         update_rx: watch::Receiver<SagaCachedState>,
//     ) -> SagaId {
//         let run_fut = exec.run().fuse();
//         let sec_store = Arc::clone(&self.sec_store);
//
//         loop {
//             select! {
//                 _ = run_fut => break,
//                 logmsg = log_rx.recv() => {
//                     sec_store.record_event(saga_id, &logmsg.event).await;
//                     /*
//                      * `send` can only fail if the other side of the channel has
//                      * closed.  That's illegal because the other side should be
//                      * waiting for our acknowledgement.
//                      */
//                     assert!(logmsg.ack_tx.send(()).is_ok());
//                 },
//                 maybe_state_update = update_rx.changed() => {
//                     /*
//                      * We may get an error when the channel is closing due to
//                      * execution completion.  That's fine -- just ignore it.
//                      */
//                     if let Ok(update) = maybe_state_update {
//                         /* TODO-robustness this should be retried as needed? */
//                         sec_store.saga_update(saga_id, update).await;
//                     }
//                 }
//             }
//         }
//     }
// }
//

/**
 * Handle used by [`SagaExecutor`] for sending messages back to the SEC
 */
/* XXX TODO-cleanup This should be pub(crate).  See lib.rs. */
#[derive(Debug)]
pub struct SecSagaHdl {
    log_channel: mpsc::Sender<SecSagaHdlMsgLog>,
    update_channel: watch::Sender<SagaCachedState>,
}

impl SecSagaHdl {
    /** Write `event` to the saga log. */
    pub async fn record(&self, event: SagaNodeEvent) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.log_channel
            .send(SecSagaHdlMsgLog { event, ack_tx })
            .await
            .unwrap();
        ack_rx.await.unwrap()
    }

    /**
     * Update the cached state for the saga.  This does not block on any
     * write to persistent storage because that is not required for
     * correctness here.
     */
    pub async fn saga_update(&self, update: SagaCachedState) {
        self.update_channel.send(update).unwrap();
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

// /*
//  * Very simple file-based serialization and deserialization, intended only for
//  * testing and debugging
//  */
// #[derive(Deserialize, Serialize)]
// pub struct SagaSerialized {
//     saga_id: SagaId,
//     params: JsonValue,
//     events: Vec<SagaNodeEvent>,
// }
//
// // XXX Should we combine these by having SagaLog impl Serialize/Deserialize?
// // XXX Maybe this is what saga_resume() should take, too?
// #[derive(Debug, Clone)]
// pub struct SagaRecovered {
//     pub saga_id: SagaId,
//     pub params: JsonValue,
//     pub log: SagaLog,
// }
//
// impl SagaRecovered {
//     /* XXX weirdly asymmetric with the way we write this out. */
//     pub fn read<R: std::io::Read>(
//         reader: R,
//     ) -> Result<SagaRecovered, anyhow::Error> {
//         let s: SagaSerialized = serde_json::from_reader(reader)
//             .with_context(|| "deserializing saga")?;
//         Ok(SagaRecovered {
//             saga_id: s.saga_id,
//             params: s.params,
//             log: SagaLog::new_recover(s.saga_id, s.events)?,
//         })
//     }
// }
