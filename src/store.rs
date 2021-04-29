/*!
 * Interfaces for persistence of saga records and saga logs
 */

use crate::saga_exec::SagaExecutor;
use crate::ActionError;
use crate::SagaExecManager;
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
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

/* XXX TODO-doc This whole file */

#[derive(Clone, Debug)]
pub struct SagaCreateParams {
    pub id: SagaId,
    pub template_name: String,
    pub saga_params: JsonValue,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SagaStoredState {
    Running,
    Unwinding,
    Done,
}

#[async_trait]
pub trait StoreBackend {
    async fn saga_create(
        &self,
        create_params: &SagaCreateParams,
    ) -> Result<(), anyhow::Error>;

    async fn record_event(&self, id: &SagaId, event: &SagaNodeEvent);

    async fn saga_update(
        &self,
        id: &SagaId,
        update: &SagaStoredState,
    ) -> Result<(), anyhow::Error>;
}

pub struct InMemoryStoreBackend {}

impl InMemoryStoreBackend {
    pub fn new() -> InMemoryStoreBackend {
        InMemoryStoreBackend {}
    }
}

#[async_trait]
impl StoreBackend for InMemoryStoreBackend {
    async fn saga_create(
        &self,
        _create_params: &SagaCreateParams,
    ) -> Result<(), anyhow::Error> {
        /* Nothing to do. */
        Ok(())
    }

    async fn record_event(&self, _id: &SagaId, _event: &SagaNodeEvent) {
        /* Nothing to do. */
    }

    async fn saga_update(
        &self,
        _id: &SagaId,
        _update: &SagaStoredState,
    ) -> Result<(), anyhow::Error> {
        /* Nothing to do. */
        Ok(())
    }
}

/*
 * XXX TODO-doc
 * owned by the consumer; any references from internal must be synchronized.
 * When we get to creating child sagas, we may want to use a channel for this.
 * Until then, we can potentially get away with handing out the "backend" Arc to
 * each Exec?  Or for future-proofing: wrap that in a struct that we hand to
 * each Exec.
 */
pub struct Store {
    log: slog::Logger,
    sagas: BTreeMap<SagaId, Saga>,
    backend: Arc<dyn StoreBackend>,
}

pub struct Saga {
    log: slog::Logger,
    run_state: SagaRunState,
}

pub enum SagaRunState {
    Running {
        exec: Arc<dyn SagaExecManager>,
        // future: Box<dyn Future<Output = ()>>, // XXX
    },
    Done {
        result: SagaResult,
    },
}

#[derive(Debug)]
pub enum SagaStateView {
    Running { status: crate::SagaExecStatus },
    Done,
}

impl Store {
    pub fn new(log: slog::Logger, backend: Arc<dyn StoreBackend>) -> Store {
        Store { log, sagas: BTreeMap::new(), backend }
    }

    // XXX Should this return a Future that waits on this saga?
    pub async fn saga_create<UserType>(
        &mut self,
        uctx: Arc<UserType::ExecContextType>,
        saga_id: SagaId,
        template: Arc<SagaTemplate<UserType>>,
        template_name: String, // XXX
        params: UserType::SagaParamsType,
    ) -> Result<(), anyhow::Error>
    where
        UserType: SagaType,
    {
        /* TODO-log would like template name, maybe parameters in the log */
        // XXX Run these futures
        let log = self.log.new(o!("id" => saga_id.to_string()));
        info!(&log, "saga create");

        /*
         * Before doing anything else, create a persistent record for this saga.
         */
        let serialized_params = serde_json::to_value(&params)
            .map_err(ActionError::new_serialize)
            .context("serializing initial saga params")?;
        let saga_create = SagaCreateParams {
            id: saga_id,
            template_name,
            saga_params: serialized_params,
        };
        self.backend
            .saga_create(&saga_create)
            .await
            .context("creating saga record")?;

        /*
         * Now create an executor to run this saga.
         */
        let internal = self.store_internal(saga_id);
        let exec = SagaExecutor::new(
            log.new(o!()),
            saga_id,
            template,
            uctx,
            params,
            internal,
        );
        let run_state = Saga {
            log,
            run_state: SagaRunState::Running { exec: Arc::new(exec) },
        };
        assert!(self.sagas.insert(saga_id, run_state).is_none());
        Ok(())
    }

    pub fn saga_resume<T>(
        &mut self,
        uctx: Arc<T>,
        saga_id: SagaId,
        template: Arc<dyn SagaTemplateGeneric<T>>,
        params: JsonValue,
        saga_log: SagaLog,
    ) -> Result<(), anyhow::Error>
    where
        T: Send + Sync,
    {
        let log = self.log.new(o!("id" => saga_id.to_string()));
        /* TODO-log would like template name, maybe parameters in the log */
        info!(&self.log, "saga resume"; "id" => %saga_id);

        let internal = self.store_internal(saga_id);
        let exec = template.recover(
            log.new(o!()),
            saga_id,
            uctx,
            params,
            internal,
            saga_log,
        )?;
        // XXX Run these futures
        let run_state = Saga { log, run_state: SagaRunState::Running { exec } };
        assert!(self.sagas.insert(saga_id, run_state).is_none());
        Ok(())
    }

    pub async fn saga_get_state(
        &self,
        saga_id: SagaId,
    ) -> Result<SagaStateView, anyhow::Error> {
        let run_state = &self
            .sagas
            .get(&saga_id)
            .ok_or_else(|| anyhow!("no saga with id \"{}\"", saga_id))?
            .run_state;
        Ok(match run_state {
            SagaRunState::Running { exec } => {
                SagaStateView::Running { status: exec.status().await }
            }
            SagaRunState::Done { .. } => SagaStateView::Done,
        })
    }

    fn store_internal(&self, saga_id: SagaId) -> StoreInternal {
        let (log_tx, log_rx) = mpsc::channel(1);
        // XXX Is the initial value really Running?
        let (update_tx, update_rx) = watch::channel(SagaStoredState::Running);
        // XXX Need to actually listen on these channels at some point!
        StoreInternal {
            log_channel: log_tx,
            update_channel: update_tx,
        }
    }
}

#[derive(Debug)]
struct StoreLog {
    event: SagaNodeEvent,
    ack_tx: oneshot::Sender<()>,
}

/* XXX TODO-cleanup This should be pub(crate).  See lib.rs. */
#[derive(Debug)]
pub struct StoreInternal {
    log_channel: mpsc::Sender<StoreLog>,
    update_channel: watch::Sender<SagaStoredState>,
}

impl StoreInternal {
    pub async fn record(&self, event: SagaNodeEvent) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.log_channel.send(StoreLog { event, ack_tx }).await.unwrap();
        ack_rx.await.unwrap()
    }

    pub async fn saga_update(&self, update: SagaStoredState) {
        self.update_channel.send(update).unwrap();
    }
}
