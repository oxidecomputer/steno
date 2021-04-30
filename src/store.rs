/*!
 * [`SecStore`] trait, related types, and built-in implementations
 */

use crate::SagaId;
use crate::SagaNodeEvent;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;

/**
 * Interfaces implemented by the Steno consumer to storing saga state and saga
 * log state persistently
 *
 * Correct implementation of these interfaces is critical for crash recovery.
 */
#[async_trait]
pub trait SecStore: fmt::Debug + Send + Sync {
    /**
     * Create a record for a newly created saga
     *
     * Once this step has completed, the saga will be discovered and recovered
     * upon startup.  Until this step has completed, the saga has not finished
     * being created (since it won't be recovered on startup).
     */
    async fn saga_create(
        &self,
        create_params: &SagaCreateParams,
    ) -> Result<(), anyhow::Error>;

    /**
     * Write a record to a saga's persistent log
     */
    async fn record_event(&self, id: SagaId, event: &SagaNodeEvent);

    /**
     * Update the cached runtime state of the saga
     *
     * Steno invokes this function when the saga has reached one of the states
     * described by [`SagaCachedState`] (like "Done").  This allows consumers to
     * persistently record this information for easy access.  This step is not
     * strictly required for correctness, since the saga log contains all the
     * information needed to determine this state.  But by recording when a saga
     * has finished, for example, the consumer can avoid having to read the
     * saga's log altogether when it next starts up since there's no need to
     * recover the saga.
     */
    async fn saga_update(
        &self,
        id: SagaId,
        update: &SagaCachedState,
    ) -> Result<(), anyhow::Error>;
}

/**
 * Describes what the SecStore needs to store for a persistent saga record.
 */
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SagaCreateParams {
    pub id: SagaId,
    pub template_name: String,
    pub saga_params: serde_json::Value,
    /* XXX SagaCachedState must go here */
}

/**
 * Describes the cacheable state of the saga
 *
 * See [`SecStore::saga_update`].
 */
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SagaCachedState {
    Running,
    Unwinding,
    Done,
}

/**
 * Implementation of [`SecStore`] that doesn't store any state persistently
 *
 * Sagas created using this store will not be recovered after the program
 * crashes.
 */
#[derive(Debug)]
pub struct InMemorySecStore {}

impl InMemorySecStore {
    pub fn new() -> InMemorySecStore {
        InMemorySecStore {}
    }
}

#[async_trait]
impl SecStore for InMemorySecStore {
    async fn saga_create(
        &self,
        _create_params: &SagaCreateParams,
    ) -> Result<(), anyhow::Error> {
        /* Nothing to do. */
        Ok(())
    }

    async fn record_event(&self, _id: SagaId, _event: &SagaNodeEvent) {
        /* Nothing to do. */
    }

    async fn saga_update(
        &self,
        _id: SagaId,
        _update: &SagaCachedState,
    ) -> Result<(), anyhow::Error> {
        /* Nothing to do. */
        Ok(())
    }
}
