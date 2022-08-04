//! [`SecStore`] trait, related types, and built-in implementations

use crate::SagaId;
use crate::SagaName;
use crate::SagaNodeEvent;
use anyhow::Context;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;
use std::fmt;

/// Interfaces implemented by the Steno consumer to storing saga state and saga
/// log state persistently
///
/// Correct implementation of these interfaces is critical for crash recovery.
#[async_trait]
pub trait SecStore: fmt::Debug + Send + Sync {
    /// Create a record for a newly created saga
    ///
    /// Once this step has completed, the saga will be discovered and recovered
    /// upon startup.  Until this step has completed, the saga has not finished
    /// being created (since it won't be recovered on startup).
    async fn saga_create(
        &self,
        create_params: SagaCreateParams,
    ) -> Result<(), anyhow::Error>;

    /// Write a record to a saga's persistent log
    async fn record_event(&self, event: SagaNodeEvent);

    /// Update the cached runtime state of the saga
    ///
    /// Steno invokes this function when the saga has reached one of the states
    /// described by [`SagaCachedState`] (like "Done").  This allows consumers
    /// to persistently record this information for easy access.  This step
    /// is not strictly required for correctness, since the saga log
    /// contains all the information needed to determine this state.  But by
    /// recording when a saga has finished, for example, the consumer can
    /// avoid having to read the saga's log altogether when it next starts
    /// up since there's no need to recover the saga.
    async fn saga_update(&self, id: SagaId, update: SagaCachedState);
}

/// Describes what an impl of [`SecStore`] needs to store for a persistent saga
/// record.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SagaCreateParams {
    pub id: SagaId,
    // The saga name doesn't strictly speaking need to be a separate field here
    // because it's contained within `dag`.  However, the name is useful to the
    // consumer.  And they're not supposed to be picking apart `dag`.  So we
    // pull it out for them.
    pub name: SagaName,
    pub dag: serde_json::Value,
    pub state: SagaCachedState,
}

/// Describes the cacheable state of the saga
///
/// See [`SecStore::saga_update`].
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SagaCachedState {
    Running,
    Unwinding,
    Done,
}

impl fmt::Display for SagaCachedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", <&str>::from(self))
    }
}

impl TryFrom<&str> for SagaCachedState {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Round-tripping through serde is a little absurd, but has the benefit
        // of always staying in sync with the real definition.  (The initial
        // serialization is necessary to correctly handle any quotes or the like
        // in the input string.)
        let json = serde_json::to_string(value).unwrap();
        serde_json::from_str(&json).context("parsing saga state")
    }
}

impl<'a> From<&'a SagaCachedState> for &'a str {
    fn from(s: &'a SagaCachedState) -> &'a str {
        match s {
            SagaCachedState::Running => "running",
            SagaCachedState::Unwinding => "unwinding",
            SagaCachedState::Done => "done",
        }
    }
}

/// Implementation of [`SecStore`] that doesn't store any state persistently
///
/// Sagas created using this store will not be recovered after the program
/// crashes.
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
        _create_params: SagaCreateParams,
    ) -> Result<(), anyhow::Error> {
        // Nothing to do.
        Ok(())
    }

    async fn record_event(&self, _event: SagaNodeEvent) {
        // Nothing to do.
    }

    async fn saga_update(&self, _id: SagaId, _update: SagaCachedState) {
        // Nothing to do.
    }
}
