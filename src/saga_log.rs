//! Persistent state for sagas

use crate::saga_action_error::ActionError;
use crate::SagaId;
use anyhow::anyhow;
use anyhow::Context;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;

/// Unique identifier for a saga node
// We use a newtype for SagaNodeId for the usual reasons.  What about the
// underlying representation?  The Omicron consumer is going to store these in
// CockroachDB, which makes `i64` the most natural numeric type.  There's no
// need for signed values here, so we choose `u32` as large enough for our
// purposes, unsigned, and can be infallibly converted to an `i64`.
//
// TODO-cleanup figure out how to use custom_derive here?
#[derive(
    Deserialize,
    Clone,
    Copy,
    Eq,
    Ord,
    JsonSchema,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(transparent)]
pub struct SagaNodeId(u32);
NewtypeDebug! { () pub struct SagaNodeId(u32); }
NewtypeDisplay! { () pub struct SagaNodeId(u32); }
NewtypeFrom! { () pub struct SagaNodeId(u32); }

#[derive(Debug, Clone, Error)]
pub enum SagaLogError {
    #[error(
        "event type {event_type} is illegal with current load status \
         {current_status:?}"
    )]
    IllegalEventForState {
        current_status: SagaNodeLoadStatus,
        event_type: SagaNodeEventType,
    },
}

/// An entry in the saga log
#[derive(Clone, Deserialize, Serialize)]
pub struct SagaNodeEvent {
    /// id of the saga
    pub saga_id: SagaId,
    /// id of the saga node
    pub node_id: SagaNodeId,
    /// what's indicated by this event
    pub event_type: SagaNodeEventType,
}

impl fmt::Debug for SagaNodeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "N{:0>3} {}", self.node_id, self.event_type)
    }
}

/// Event types that may be found in the log for a particular action
///
/// (This is not a general-purpose debug log, but more like an intent log for
/// recovering the action's state in the event of an executor crash.  That
/// doesn't mean we can't put debugging information here, though.)
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaNodeEventType {
    /// The action has started running
    Started,
    /// The action completed successfully (with output data)
    Succeeded(Arc<serde_json::Value>),
    /// The action failed
    Failed(ActionError),
    /// The undo action has started running
    UndoStarted,
    /// The undo action has finished
    UndoFinished,
}

impl fmt::Display for SagaNodeEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl SagaNodeEventType {
    pub fn label(&self) -> &'static str {
        match self {
            SagaNodeEventType::Started => "started",
            SagaNodeEventType::Succeeded(_) => "succeeded",
            SagaNodeEventType::Failed(_) => "failed",
            SagaNodeEventType::UndoStarted => "undo_started",
            SagaNodeEventType::UndoFinished => "undo_finished",
        }
    }
}

/// Persistent status for a saga node
///
/// The events present in the log determine the _persistent status_ of the node.
/// You can think of this like a single summary of the state of this action,
/// based solely on the persistent state.  When recovering from a crash, the
/// saga executor uses this status to determine what to do next.  We also
/// maintain this for each SagaLog to identify illegal transitions at runtime.
///
/// A node's status is very nearly identified by the type of the last event
/// seen. It's cleaner to have a first-class summary here.
#[derive(Clone, Debug)]
pub enum SagaNodeLoadStatus {
    /// The action never started running
    NeverStarted,
    /// The action has started running
    Started,
    /// The action completed successfully (with output data)
    Succeeded(Arc<serde_json::Value>),
    /// The action failed
    Failed(ActionError),
    /// The undo action has started running (with output data from success)
    UndoStarted(Arc<serde_json::Value>),
    /// The undo action has finished
    UndoFinished,
}

impl SagaNodeLoadStatus {
    /// Returns the new status for a node after recording the given event.
    fn next_status(
        &self,
        event_type: &SagaNodeEventType,
    ) -> Result<SagaNodeLoadStatus, SagaLogError> {
        match (self, event_type) {
            (SagaNodeLoadStatus::NeverStarted, SagaNodeEventType::Started) => {
                Ok(SagaNodeLoadStatus::Started)
            }
            (
                SagaNodeLoadStatus::Started,
                SagaNodeEventType::Succeeded(out),
            ) => Ok(SagaNodeLoadStatus::Succeeded(Arc::clone(out))),
            (SagaNodeLoadStatus::Started, SagaNodeEventType::Failed(e)) => {
                Ok(SagaNodeLoadStatus::Failed(e.clone()))
            }
            (
                SagaNodeLoadStatus::Succeeded(out),
                SagaNodeEventType::UndoStarted,
            ) => Ok(SagaNodeLoadStatus::UndoStarted(Arc::clone(out))),
            (
                SagaNodeLoadStatus::UndoStarted(_),
                SagaNodeEventType::UndoFinished,
            ) => Ok(SagaNodeLoadStatus::UndoFinished),
            _ => Err(SagaLogError::IllegalEventForState {
                current_status: self.clone(),
                event_type: event_type.clone(),
            }),
        }
    }
}

/// Write to a saga's log
#[derive(Clone, Debug)]
pub struct SagaLog {
    saga_id: SagaId,
    unwinding: bool,
    events: Vec<SagaNodeEvent>,
    node_status: BTreeMap<SagaNodeId, SagaNodeLoadStatus>,
}

impl SagaLog {
    pub fn new_empty(saga_id: SagaId) -> SagaLog {
        SagaLog {
            saga_id,
            events: Vec::new(),
            node_status: BTreeMap::new(),
            unwinding: false,
        }
    }

    pub fn new_recover(
        saga_id: SagaId,
        mut events: Vec<SagaNodeEvent>,
    ) -> Result<SagaLog, anyhow::Error> {
        let mut log = Self::new_empty(saga_id);

        // Sort the events by the event type.  This ensures that if there's at
        // least one valid sequence of events, then we'll replay the events in a
        // valid sequence.  Thus, if we fail to replay below, then the log is
        // corrupted somehow.  (Remember, the wall timestamp is never used for
        // correctness.) For debugging purposes, this is a little disappointing:
        // most likely, the events are already in a valid order that reflects
        // when they actually happened.  However, there's nothing to guarantee
        // that unless we make it so, and our simple approach for doing so here
        // destroys the sequential order.  This should only really matter for a
        // person looking at the sequence of entries (as they appear in memory)
        // for debugging.
        events.sort_by_key(|f| match f.event_type {
            // TODO-cleanup Is there a better way to do this?  We want to sort
            // by the event type, where event types are compared by the order
            // they're defined in SagaEventType.  We could almost use derived
            // PartialOrd and PartialEq implementations for SagaEventType,
            // except that one variant has a payload that does _not_
            // necessarily implement PartialEq or PartialOrd.  It
            // seems like that means we have to implement this by
            // hand.
            SagaNodeEventType::Started => 1,
            SagaNodeEventType::Succeeded(_) => 2,
            SagaNodeEventType::Failed(_) => 3,
            SagaNodeEventType::UndoStarted => 4,
            SagaNodeEventType::UndoFinished => 5,
        });

        // Replay the events for this saga.
        for event in events {
            if event.saga_id != saga_id {
                return Err(anyhow!(
                    "found an event in the log for a different saga ({}) than \
                     requested ({})",
                    event.saga_id,
                    saga_id,
                ));
            }

            log.record(&event).with_context(|| "SagaLog::new_recover")?;
        }

        Ok(log)
    }

    pub fn record(
        &mut self,
        event: &SagaNodeEvent,
    ) -> Result<(), SagaLogError> {
        let current_status = self.load_status_for_node(event.node_id);
        let next_status = current_status.next_status(&event.event_type)?;

        match next_status {
            SagaNodeLoadStatus::Failed(_)
            | SagaNodeLoadStatus::UndoStarted(_)
            | SagaNodeLoadStatus::UndoFinished => {
                self.unwinding = true;
            }
            _ => (),
        };

        self.node_status.insert(event.node_id, next_status);
        self.events.push(event.clone());
        Ok(())
    }

    pub fn unwinding(&self) -> bool {
        self.unwinding
    }

    pub fn load_status_for_node(
        &self,
        node_id: SagaNodeId,
    ) -> &SagaNodeLoadStatus {
        self.node_status
            .get(&node_id)
            .unwrap_or(&SagaNodeLoadStatus::NeverStarted)
    }

    pub fn events(&self) -> &[SagaNodeEvent] {
        &self.events
    }

    pub fn pretty(&self) -> SagaLogPretty<'_> {
        SagaLogPretty { log: self }
    }
}

/// Handle for pretty-printing a SagaLog (using the `fmt::Debug` trait)
pub struct SagaLogPretty<'a> {
    log: &'a SagaLog,
}

impl<'a> fmt::Debug for SagaLogPretty<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SAGA LOG:\n")?;
        write!(f, "saga id:   {}\n", self.log.saga_id)?;
        write!(
            f,
            "direction: {}\n",
            if !self.log.unwinding { "forward" } else { "unwinding" }
        )?;
        write!(f, "events ({} total):\n", self.log.events.len())?;
        write!(f, "\n")?;
        for (i, event) in self.log.events.iter().enumerate() {
            write!(f, "{:0>3} {:?}\n", i + 1, event)?;
        }
        Ok(())
    }
}

// TODO-testing lots of automated tests are possible here, but let's see if the
// abstraction makes any sense first.
//
