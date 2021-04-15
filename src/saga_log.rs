//! Persistent state for sagas

use crate::saga_action_error::ActionError;
use crate::saga_template::SagaId;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

/* TODO-cleanup newtype for this? */
type SagaNodeId = u64; // XXX this should become u32 so it fits in i64

#[derive(Debug, Error)]
pub enum SagaLogError {
    #[error(
        "event type {event_type} is illegal with current \
        load status {current_status:?}"
    )]
    IllegalEventForState {
        current_status: SagaNodeLoadStatus,
        event_type: SagaNodeEventType,
    },
}

/**
 * Event types that may be found in the log for a particular action
 *
 * (This is not a general-purpose debug log, but more like an intent log for
 * recovering the action's state in the event of an executor crash.  That
 * doesn't mean we can't put debugging information here, though.)
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaNodeEventType {
    /** The action has started running */
    Started,
    /** The action completed successfully (with output data) */
    Succeeded(Arc<JsonValue>),
    /** The action failed */
    Failed(ActionError),
    /** The undo action has started running */
    UndoStarted,
    /** The undo action has finished */
    UndoFinished,
}

impl fmt::Display for SagaNodeEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SagaNodeEventType::Started => "started",
            SagaNodeEventType::Succeeded(_) => "succeeded",
            SagaNodeEventType::Failed(_) => "failed",
            SagaNodeEventType::UndoStarted => "undo started",
            SagaNodeEventType::UndoFinished => "undo finished",
        })
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

/**
 * Persistent status for a saga node
 *
 * The events present in the log determine the _persistent status_ of the node.
 * You can think of this like a single summary of the state of this action,
 * based solely on the persistent state.  When recovering from a crash, the
 * saga executor uses this status to determine what to do next.  We also
 * maintain this for each SagaLog to identify illegal transitions at runtime.
 *
 * A node's status is very nearly identified by the type of the last event seen.
 * It's cleaner to have a first-class summary here.
 */
#[derive(Clone, Debug)]
pub enum SagaNodeLoadStatus {
    /** The action never started running */
    NeverStarted,
    /** The action has started running */
    Started,
    /** The action completed successfully (with output data) */
    Succeeded(Arc<JsonValue>),
    /** The action failed */
    Failed(ActionError),
    /** The undo action has started running (with output data from success) */
    UndoStarted(Arc<JsonValue>),
    /** The undo action has finished */
    UndoFinished,
}

impl SagaNodeLoadStatus {
    /** Returns the new status for a node after recording the given event. */
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

/**
 * An entry in the saga log
 */
#[derive(Clone, Deserialize, Serialize)]
pub struct SagaNodeEvent {
    /** id of the saga */
    pub saga_id: SagaId,
    /** id of the saga node */
    pub node_id: SagaNodeId,
    /** what's indicated by this event */
    pub event_type: SagaNodeEventType,

    /* The following debugging fields are not used by the executor. */
    /** when this event was recorded (for debugging) */
    pub event_time: DateTime<Utc>,
    /** creator of this event (e.g., a hostname, for debugging) */
    pub creator: String,
}

impl fmt::Debug for SagaNodeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} N{:0>3} {}",
            self.event_time.to_rfc3339_opts(SecondsFormat::Millis, true),
            self.creator,
            self.node_id,
            self.event_type
        )
    }
}

/**
 * Write to a saga's log
 */
/*
 * TODO-cleanup This structure is used both for writing to the log and
 * recovering the log.  There are some similarities.  However, it might be
 * useful to enforce that you're only doing one of these at a time by having
 * these by separate types, with the recovery one converting into SagaLog when
 * you're done with recovery.
 */
#[derive(Clone)]
pub struct SagaLog {
    /* TODO-robustness include version here */
    pub saga_id: SagaId,
    pub unwinding: bool,
    creator: String,
    params: JsonValue,
    events: Vec<SagaNodeEvent>,
    node_status: BTreeMap<SagaNodeId, SagaNodeLoadStatus>,
    sink: Arc<dyn SagaLogSink>,
}

impl SagaLog {
    pub fn new(
        creator: &str,
        saga_id: &SagaId,
        params: JsonValue,
        sink: Arc<dyn SagaLogSink>,
    ) -> SagaLog {
        SagaLog {
            saga_id: *saga_id,
            creator: creator.to_string(),
            events: Vec::new(),
            params,
            node_status: BTreeMap::new(),
            unwinding: false,
            sink,
        }
    }

    pub async fn record_now(
        &mut self,
        node_id: SagaNodeId,
        event_type: SagaNodeEventType,
    ) -> Result<(), SagaLogError> {
        let event = SagaNodeEvent {
            saga_id: self.saga_id,
            node_id,
            event_time: Utc::now(),
            event_type,
            creator: self.creator.clone(),
        };

        Ok(self
            .record(event)
            .await
            .unwrap_or_else(|_| panic!("illegal event for node {}", node_id)))
    }

    async fn record(
        &mut self,
        event: SagaNodeEvent,
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
        self.sink.record(&event).await;
        self.events.push(event);
        Ok(())
    }

    pub fn load_status_for_node(
        &self,
        node_id: SagaNodeId,
    ) -> &SagaNodeLoadStatus {
        self.node_status
            .get(&node_id)
            .unwrap_or(&SagaNodeLoadStatus::NeverStarted)
    }

    pub fn events(&self) -> &Vec<SagaNodeEvent> {
        &self.events
    }

    pub fn params_as<T: DeserializeOwned>(&self) -> Result<T, anyhow::Error> {
        serde_json::from_value(self.params.clone())
            .context("deserializing initial saga parameters")
    }

    // TODO-blocking
    pub fn dump<W: Write>(&self, writer: W) -> Result<(), anyhow::Error> {
        /* TODO-cleanup can we avoid these clones? */
        let s = SagaLogSerialized {
            saga_id: self.saga_id,
            creator: self.creator.clone(),
            events: self.events.clone(),
            params: self.params.clone(),
        };

        serde_json::to_writer_pretty(writer, &s).with_context(|| {
            format!("serializing log for saga {}", self.saga_id)
        })
    }

    // TODO-blocking
    // TODO-design There's some confusion about the recovery process in general
    // and the "creator" field.  The recorded log has a creator, and each entry
    // has a creator.  The executor also has a creator, and the SagaLog has a
    // creator.  These can nearly all be different, but the executor and SagaLog
    // creator should match.  Part of the problem here is that the SagaLog only
    // has one "creator", but we really want it to have two: the one in the
    // recorded log, and the one that's used for new entries.
    pub fn load<R: Read>(
        creator: &str,
        reader: R,
    ) -> Result<SagaLog, anyhow::Error> {
        let mut s: SagaLogSerialized = serde_json::from_reader(reader)
            .with_context(|| "deserializing saga log")?;
        let mut sglog = SagaLog::new(
            &creator,
            &s.saga_id,
            s.params.clone(),
            Arc::new(NullSink),
        );

        /*
         * Sort the events by the event type.  This ensures that if there's at
         * least one valid sequence of events, then we'll replay the events in a
         * valid sequence.  Thus, if we fail to replay below, then the log is
         * corrupted somehow.  (Remember, the wall timestamp is never used for
         * correctness.) For debugging purposes, this is a little disappointing:
         * most likely, the events are already in a valid order that reflects
         * when they actually happened.  However, there's nothing to guarantee
         * that unless we make it so, and our simple approach for doing so here
         * destroys the sequential order.  This should only really matter for a
         * person looking at the sequence of entries (as they appear in memory)
         * for debugging.
         */
        s.events.sort_by_key(|f| match f.event_type {
            /*
             * TODO-cleanup Is there a better way to do this?  We want to sort
             * by the event type, where event types are compared by the order
             * they're defined in SagaEventType.  We could almost use derived
             * PartialOrd and PartialEq implementations for SagaEventType, except
             * that one variant has a payload that does _not_ necessarily
             * implement PartialEq or PartialOrd.  It seems like that means we
             * have to implement this by hand.
             */
            SagaNodeEventType::Started => 1,
            SagaNodeEventType::Succeeded(_) => 2,
            SagaNodeEventType::Failed(_) => 3,
            SagaNodeEventType::UndoStarted => 4,
            SagaNodeEventType::UndoFinished => 5,
        });

        /*
         * Replay the events for this saga.
         */
        for event in s.events {
            if event.saga_id != sglog.saga_id {
                return Err(anyhow!(
                    "found an event in the log for a \
                    different saga ({}) than the log's header ({})",
                    event.saga_id,
                    sglog.saga_id
                ));
            }

            todo!("need to fix this call to record");
            // sglog.record(event).with_context(|| "recovering saga log")?;
        }
        Ok(sglog)
    }
}

#[doc(hidden)]
#[derive(Deserialize, Serialize)]
pub struct SagaLogSerialized {
    /* TODO-robustness add version */
    pub saga_id: SagaId,
    pub creator: String,
    pub params: JsonValue,
    pub events: Vec<SagaNodeEvent>,
}

impl fmt::Debug for SagaLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SAGA LOG:\n")?;
        write!(f, "saga execution id: {}\n", self.saga_id)?;
        write!(f, "creator:           {}\n", self.creator)?;
        write!(f, "params (generic):  {}\n", self.params)?;
        write!(
            f,
            "direction:         {}\n",
            if !self.unwinding { "forward" } else { "unwinding" }
        )?;
        write!(f, "events ({} total):\n", self.events.len())?;
        write!(f, "\n")?;

        for (i, event) in self.events.iter().enumerate() {
            write!(f, "{:0>3} {:?}\n", i + 1, event)?;
        }

        Ok(())
    }
}

/*
 * XXX TODO here:
 * - what if record() fails?
 * - need a way to load, too
 * - need to fix the call to record() in load() above
 * - do we want to first-class the saga persistence part too?
 */
#[async_trait]
pub trait SagaLogSink: fmt::Debug + Send + Sync {
    async fn record(&self, event: &SagaNodeEvent);
}

#[derive(Debug)]
pub struct NullSink;
#[async_trait]
impl SagaLogSink for NullSink {
    async fn record(&self, _: &SagaNodeEvent) {}
}

//
// TODO-testing lots of automated tests are possible here, but let's see if the
// abstraction makes any sense first.
//
