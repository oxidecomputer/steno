//! Saga actions, core implementations, and related facilities
//!
//! This file contains a generic [`Action`] trait that is not intended to be
//! used by Steno consumers, though it is exposed.  Users are expected to
//! use [`crate::ActionFunc`] in saga_action_func.rs.

use crate::saga_action_error::ActionError;
use crate::saga_exec::ActionContext;
use crate::ActionName;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::Arc;

/**
 * Collection of consumer-provided types, effectively defining the type
 * signature of a saga
 *
 * This trait bundles a bunch of consumer-provided types that are used
 * throughout Steno to avoid a sprawl of type parameters and duplicated trait
 * bounds.
 */
pub trait SagaType: Debug + 'static {
    /**
     * Type for the consumer's context object
     *
     * When beginning execution of a saga with
     * [`crate::SecClient::saga_create()`] or resuming a previous execution with
     * [`crate::SecClient::saga_resume()`], consumers provide a context object
     * with this type.  This object is not persistent.  Rather, it provides
     * programming interfaces the consumer wants available from within actions.
     * For example, this could include HTTP clients that will be used by the
     * action to make requests to dependent services.  This object is made
     * available to actions via [`crate::ActionContext::user_data()`].  There's
     * one context for the life of each saga's execution.
     */
    type ExecContextType: Debug + Send + Sync + 'static;
}

/**
 * Data produced by the consumer that may need to be serialized to the saga log
 *
 * This type is used for saga parameters and the output data and errors from an
 * individual action.  It's essentially a synonym for `Debug + DeserializeOwned
 * + Serialize + Send + Sync`.  Consumers are not expected to impl this
 * directly.
 */
pub trait ActionData:
    Debug + DeserializeOwned + Serialize + Send + Sync + 'static
{
}
impl<T: Debug + DeserializeOwned + Serialize + Send + Sync + 'static> ActionData
    for T
{
}

/**
 * Result of a saga action
 *
 * In this generic Action interface, actions return a pretty generic
 * `serde_json::Value`.  This is something that we can store uniformly,
 * serialize to the log, and deserialize into a more specific type when the
 * consumer asks for that.  (By contrast, the `ActionFunc` impl is a little
 * fancier.  It allows consumers to return anything that _can_ be serialized.
 * That's why consumers should prefer that interface and not this one.)
 */
// TODO-cleanup can we drop this Arc?
pub type ActionResult = Result<Arc<serde_json::Value>, ActionError>;

/** Result of a saga undo action */
// TODO-design what should the error type here be?  Maybe something that can
// encompass "general framework error"?  This might put the saga into a "needs
// attention" state?
pub type UndoResult = Result<(), anyhow::Error>;

/**
 * Building blocks of sagas
 *
 * Each node in a saga graph is represented with some kind of `Action`,
 * which provides entry points to asynchronously execute an action and its
 * corresponding undo action.  A saga is essentially a directed acyclic graph of
 * these actions with dependencies between them.  Each action consumes an
 * [`ActionContext`] and asynchronously produces an [`ActionResult`].  The
 * primary implementor for most consumers is [`crate::ActionFunc`].
 *
 * Actions should be stateless.  Any state is supposed to be stored via the
 * framework.  So it should be easy to make Actions Send and Sync.  This is
 * important because we want to be able to have multiple references to the same
 * Action in multiple threads -- as might happen if the same action appeared
 * multiple times in the saga or in different sagas.
 */
pub trait Action<UserType: SagaType>: Debug + Send + Sync {
    /**
     * Executes the action for this saga node, whatever that is.  Actions
     * function like requests in distributed sagas: critically, they must be
     * idempotent. This means that multiple calls to the action have the
     * same result on the system as a single call, although the action is not
     * necessarily required to return the same result.
     *
     * As an example, generating a UUID to represent an object is a common saga
     * action: if called repeatedly, it may generate different results, but it
     * has no side effects on the rest of the system. Similarly, using a
     * generated UUID in a subsequent action to create an object may help ensure
     * that the side effects appear the same, regardless of how many times the
     * action has been invoked.
     *
     * Actions should be very careful in using interfaces outside of
     * [`ActionContext`] -- we want them to be as self-contained as possible to
     * ensure idempotence and to minimize versioning issues.
     *
     * On success, this function produces a serialized output.  This output will
     * be stored persistently, keyed by the _name_ of the current saga node.
     * Subsequent stages can access this data with [`ActionContext::lookup`].
     * This is the _only_ supported means of sharing state across actions within
     * a saga.
     */
    fn do_it(
        &self,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'_, ActionResult>;

    /**
     * Executes the undo action for this saga node, whatever that is.
     */
    fn undo_it(
        &self,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'_, UndoResult>;

    /**
     * Return the name of the action used as the key in the ActionRegistry
     */
    fn name(&self) -> ActionName;
}

/*
 * Special action implementations
 */

/// [`Action`] impl that emits a value known when the DAG is created
///
/// This is used to implement [`UserNode::Constant`].
#[derive(Debug)]
pub struct ActionConstant {
    value: serde_json::Value,
}

impl ActionConstant {
    pub fn new(value: serde_json::Value) -> ActionConstant {
        ActionConstant { value }
    }
}

impl<UserType> Action<UserType> for ActionConstant
where
    UserType: SagaType,
{
    fn do_it(&self, _: ActionContext<UserType>) -> BoxFuture<'_, ActionResult> {
        Box::pin(futures::future::ok(Arc::new(self.value.clone())))
    }

    fn undo_it(&self, _: ActionContext<UserType>) -> BoxFuture<'_, UndoResult> {
        Box::pin(futures::future::ok(()))
    }

    fn name(&self) -> ActionName {
        ActionName::new("ActionConstant")
    }
}

/** Simulates an error at a given spot in the saga graph */
#[derive(Debug)]
pub struct ActionInjectError {}

impl<UserType: SagaType> Action<UserType> for ActionInjectError {
    fn do_it(&self, _: ActionContext<UserType>) -> BoxFuture<'_, ActionResult> {
        // TODO-log
        Box::pin(futures::future::err(ActionError::InjectedError))
    }

    fn undo_it(&self, _: ActionContext<UserType>) -> BoxFuture<'_, UndoResult> {
        /* We should never undo an action that failed. */
        unimplemented!();
    }

    fn name(&self) -> ActionName {
        ActionName::new("InjectError")
    }
}
