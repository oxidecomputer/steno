//! Saga actions, core implementations, and related facilities
//!
//! This file contains a generic [`Action`] trait that is not intended to be
//! used by Steno consumers, though it is exposed.  Users are expected to
//! use [`crate::ActionFunc`] in saga_action_func.rs.

use crate::saga_action_error::ActionError;
use crate::saga_exec::ActionContext;
use core::fmt::Debug;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/**
 * Collection of consumer-provided types, effectively defining the type
 * signature of a saga
 *
 * This trait bundles a bunch of consumer-provided types that are used
 * throughout Steno to avoid a sprawl of type parameters and duplicated trait
 * bounds.
 */
pub trait SagaType: 'static {
    /**
     * Type for a saga's input parameters
     *
     * When consumers begin execution of a saga with
     * [`crate::SagaExecutor::new()`], they can specify parameters for the saga.
     * The collection of parameters has this type.  These parameters are
     * immediately recorded to the saga log.  They're subsequently made
     * available to the saga's actions via
     * [`crate::ActionContext::saga_params()`].
     */
    type SagaParamsType: ActionData;

    /**
     * Type for the consumer's context object
     *
     * When beginning execution of a saga with [`crate::SagaExecutor::new()`] or
     * resuming a previous execution with
     * [`crate::SagaExecutor::new_recover()`], consumers provide a context
     * object with this type.  This object is not persistent.  Rather, it
     * provides programming interfaces the consumer wants available from within
     * actions.  For example, this could include HTTP clients that will be used
     * by the action to make requests to dependent services.
     * This object is made available to actions via
     * [`crate::ActionContext::context()`].  There's one context for the life of
     * the `SagaExecutor`.
     */
    type ExecContextType: Send + Sync + 'static;
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
 * `JsonValue`.  This is something that we can store uniformly, serialize to the
 * log, and deserialize into a more specific type when the consumer asks for
 * that.  (By contrast, the `ActionFunc` impl is a little fancier.  It allows
 * consumers to return anything that _can_ be serialized.  That's why consumers
 * should prefer that interface and not this one.)
 */
// TODO-cleanup can we drop this Arc?
pub type ActionResult = Result<Arc<JsonValue>, ActionError>;

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
     * idempotent.  They should be very careful in using interfaces outside of
     * [`ActionContext`] -- we want them to be as self-contained as possible to
     * ensure idempotence and to minimize versioning issues.
     *
     * On success, this function produces a serialized output.  This output will
     * be stored persistently, keyed by the _name_ of the current saga node.
     * Subsequent stages can access this data with [`ActionContext::lookup`].
     * This is the _only_ supported means of sharing state across actions within
     * a saga.
     */
    fn do_it<'f>(
        &'f self,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'f, ActionResult>;

    /**
     * Executes the undo action for this saga node, whatever that is.
     */
    fn undo_it<'f>(
        &'f self,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'f, UndoResult>;
}

/*
 * Action implementations
 */

/** Represents the start node in a graph */
#[derive(Debug)]
pub struct ActionStartNode {}

impl<UserType> Action<UserType> for ActionStartNode
where
    UserType: SagaType,
{
    fn do_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, ActionResult> {
        // TODO-log
        Box::pin(futures::future::ok(Arc::new(JsonValue::Null)))
    }

    fn undo_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, UndoResult> {
        // TODO-log
        Box::pin(futures::future::ok(()))
    }
}

/** Represents the end node in a graph */
#[derive(Debug)]
pub struct ActionEndNode {}

impl<UserType: SagaType> Action<UserType> for ActionEndNode {
    fn do_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, ActionResult> {
        // TODO-log
        Box::pin(futures::future::ok(Arc::new(JsonValue::Null)))
    }

    fn undo_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, UndoResult> {
        /*
         * We should not run compensation actions for nodes that have not
         * started.  We should never start this node unless all other actions
         * have completed.  We should never unwind a saga unless some action
         * failed.  Thus, we should never undo the "end" node in a saga.
         */
        panic!("attempted to undo end node in saga");
    }
}

/** Simulates an error at a given spot in the saga graph */
#[derive(Debug)]
pub struct ActionInjectError {}

impl<UserType: SagaType> Action<UserType> for ActionInjectError {
    fn do_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, ActionResult> {
        // TODO-log
        Box::pin(futures::future::err(ActionError::InjectedError))
    }

    fn undo_it<'f>(
        &'f self,
        _: ActionContext<UserType>,
    ) -> BoxFuture<'f, UndoResult> {
        /* We should never undo an action that failed. */
        unimplemented!();
    }
}
