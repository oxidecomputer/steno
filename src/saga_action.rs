//! Definition of Action trait, core implementations, and related facilities

use crate::saga_exec::SagaContext;
use async_trait::async_trait;
use core::any::type_name;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use thiserror::Error;

/*
 * Result, output, and error types used for actions
 */

/*
 * Error types
 * TODO This needs more thought and better documentation, particularly on why
 * this works this way.
 *
 * Here's the basic idea: a user-provided saga action can fail for a number of
 * reasons and the returned error should provide enough information for a
 * consumer of the saga to understand what happened.  That is, if the
 * user-provided action failed for a specific reason, and that causes the whole
 * saga to fail, then someone waiting on the result of the saga should know what
 * that specific reason was.  This means we want the user-provided saga action
 * to be able to return a user-provided error type.
 *
 * It's also possible for the saga action to fail due to a problem in the
 * framework.  For example, if an error was injected, the action won't be run at
 * all, and we don't have a user-provided error type to put in its place.  Or
 * maybe the user action succeeded, but we failed to serialize the output.  In
 * these cases, the framework itself needs to return an error.
 *
 * How can we return either a framework error or a user error?  We provide
 * `SagaActionError`, an enum describing the ways a saga action can fail.  One
 * of those variants, `ActionFailed`, indicates that we ran the action and _it_
 * returned a user-provided error.  Just like with normal outputs, we require
 * that error objects be serializable and we store the generic JsonValue
 * serialized form.  Later, if the user wants the specific error back, we
 * deserialize it.  This is important to ensure that this sequence (generate
 * user-specific error, then later access it) works even when there's a crash in
 * the middle of it.
 *
 * Recall that the interface for SagaAction is more generic than for
 * SagaActionFunc.  SagaAction actions produce a Result that's ultimately either
 * a JsonValue representing an output or a JsonValue representing an error.
 * SagaActionFunc allows consumers to produce Result<O, E>, where `O` and `E`
 * both impl `SagaActionOutput` (which essentially means that they're serde
 * serializable, deserializable, Send, and Sync).
 */

/**
 * An error that can be produced by a saga action
 */
#[derive(Clone, Debug, Deserialize, Error, Serialize)]
pub enum SagaActionError {
    #[error("action failed")]
    ActionFailed { source_error: SagaActionUserError },
    #[error("failed to serialize action's result")]
    SerializeFailed { message: String },
    #[error("error injected")]
    InjectedError,
}

impl SagaActionError {
    /**
     * Return a SagaActionError for a given user-provided serializable error
     * type
     */
    pub fn action_failed<E: SagaActionOutput + 'static>(
        user_error: E,
    ) -> SagaActionError {
        match serde_json::to_value(user_error) {
            Ok(source_error) => SagaActionError::ActionFailed {
                source_error: SagaActionUserError(source_error),
            },
            Err(serialize_error) => SagaActionError::SerializeFailed {
                message: serialize_error.to_string(),
            },
        }
    }
}

/**
 * Wrapper around SagaActionError::ActionFailed to ensure that downcasting is
 * only possible for the ActionFailed variant.
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SagaActionUserError(JsonValue);

impl SagaActionUserError {
    /**
     * Reinterpret the SagaActionError::ActionFailed source error as an object
     * of type `T`.
     */
    pub fn action_failure_downcast<T: SagaActionOutput + 'static>(&self) -> T {
        serde_json::from_value(self.0.clone()).expect(
            "failed to deserialize action failure error as requested type",
        )
    }
}

/** Result of a saga action */
// TODO-cleanup can we drop this Arc?
pub type SagaActionResult = Result<Arc<JsonValue>, SagaActionError>;
/** Result of a saga undo action */
// TODO-design what should the error type here be?  Maybe something that can
// encompass "general framework error"?  This might put the saga into a "needs
// attention" state?
pub type SagaUndoResult = Result<(), anyhow::Error>;

/**
 * Result of a function that implements a saga action
 */
/*
 * This differs from [`SagaActionResult`] because [`SagaActionResult`] returns a
 * pretty generic type.  The function-oriented interface allows you to return
 * more specific types as long as they implement the [`SagaActionOutput`] trait.
 *
 * TODO-design There's no reason that SagaActionResult couldn't also look like
 * this.  We have this mechanism to allow `SagaActionFunc` functions to return
 * specific types while storing the generic thing inside the framework.  We do
 * this translation in the impl of `SagaActionFunc`.  Instead, we could create
 * another layer above `SagaAction` that does this.  This gets complicated and
 * doesn't seem especially useful yet.
 */
pub type SagaFuncResult<T, E> = Result<T, E>;

/**
 * Success return type for functions that are used as saga actions
 *
 * This trait exists as a name for `Debug + DeserializeOwned + Serialize + Send
 * + Sync`.  Consumers are not expected to impl this directly.  
 */
pub trait SagaActionOutput:
    Debug + DeserializeOwned + Serialize + Send + Sync
{
}
impl<T: Debug + DeserializeOwned + Serialize + Send + Sync> SagaActionOutput
    for T
{
}

/**
 * Synonym for `Send + Sync + 'static`
 */
pub trait ExecContext: Send + Sync + 'static {}
impl<ExecContextType: Send + Sync + 'static> ExecContext for ExecContextType {}

/*
 * Generic Action interface
 */

/**
 * Building blocks of sagas
 *
 * Each node in a saga graph is represented with some kind of `SagaAction`,
 * which provides entry points to asynchronously execute an action and its
 * corresponding undo action.  A saga is essentially a directed acyclic graph of
 * these actions with dependencies between them.  Each action consumes a
 * [`SagaContext`] and asynchronously produces a [`SagaActionResult`].  The
 * primary implementor for most consumers is [`SagaActionFunc`].
 */
/*
 * We currently don't expose the `SagaAction` trait directly to users, but we
 * easily could if that proved useful.  We may want to think more carefully
 * about the `SagaActionResult` type if we do that.
 *
 * The intent is that SagaActions are stateless -- any state is supposed to be
 * stored via the saga framework itself.  As a result, it should be easy to make
 * these Send and Sync.  This is important because we want to be able to have
 * multiple references to the same SagaAction in multiple threads -- as might
 * happen if the same action appeared multiple times in the saga or in different
 * sagas.
 */
#[async_trait]
pub trait SagaAction<ExecContextType>: Debug + Send + Sync
where
    ExecContextType: ExecContext,
{
    /**
     * Executes the action for this saga node, whatever that is.  Actions
     * function like requests in distributed sagas: critically, they must be
     * idempotent.  They should be very careful in using interfaces outside of
     * [`SagaContext`] -- we want them to be as self-contained as possible to
     * ensure idempotence and to minimize versioning issues.
     *
     * On success, this function produces a `SagaActionOutput`.  This output will
     * be stored persistently, keyed by the _name_ of the current saga node.
     * Subsequent stages can access this data with [`SagaContext::lookup`].  This
     * is the _only_ supported means of sharing state across actions within a
     * saga.
     */
    async fn do_it(
        &self,
        sgctx: SagaContext<ExecContextType>,
    ) -> SagaActionResult;

    /**
     * Executes the compensation action for this saga node, whatever that is.
     */
    async fn undo_it(
        &self,
        sgctx: SagaContext<ExecContextType>,
    ) -> SagaUndoResult;
}

/*
 * SagaAction implementations
 */

/** Represents the start node in a graph */
#[derive(Debug)]
pub struct SagaActionStartNode {}

#[async_trait]
impl<ExecContextType: ExecContext> SagaAction<ExecContextType>
    for SagaActionStartNode
{
    async fn do_it(&self, _: SagaContext<ExecContextType>) -> SagaActionResult {
        eprintln!("<action for \"start\" node>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: SagaContext<ExecContextType>) -> SagaUndoResult {
        eprintln!("<undo for \"start\" node (saga is nearly done unwinding)>");
        Ok(())
    }
}

/** Represents the end node in a graph */
#[derive(Debug)]
pub struct SagaActionEndNode {}

#[async_trait]
impl<ExecContextType: ExecContext> SagaAction<ExecContextType>
    for SagaActionEndNode
{
    async fn do_it(&self, _: SagaContext<ExecContextType>) -> SagaActionResult {
        eprintln!("<action for \"end\" node: saga is nearly done>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: SagaContext<ExecContextType>) -> SagaUndoResult {
        /*
         * We should not run compensation actions for nodes that have not
         * started.  We should never start this node unless all other actions
         * have completed.  We should never unwind a saga unless some action
         * failed.  Thus, we should never undo the "end" node in a saga.
         */
        panic!("attempted to undo end node in saga");
    }
}

/** Simulates an error at a given spot in the graph */
#[derive(Debug)]
pub struct SagaActionInjectError {}

#[async_trait]
impl<ExecContextType: ExecContext> SagaAction<ExecContextType>
    for SagaActionInjectError
{
    async fn do_it(&self, _: SagaContext<ExecContextType>) -> SagaActionResult {
        Err(SagaActionError::InjectedError)
    }

    async fn undo_it(&self, _: SagaContext<ExecContextType>) -> SagaUndoResult {
        /* We should never undo an action that failed. */
        unimplemented!();
    }
}

/**
 * Implementation for `SagaAction` using simple functions for the action and
 * undo action
 */
/*
 * The type parameters here look pretty complicated, but it's simpler than it
 * looks.  `SagaActionFunc` wraps two asynchronous functions.  Both consume a
 * `SagaContext`.  On success, the action function produces a type that impls
 * `SagaActionOutput` and the undo function produces nothing.  Because they're
 * asynchronous and because the first function can produce any type that impls
 * `SagaActionOutput`, we get this explosion of type parameters and trait
 * bounds.
 */
pub struct SagaActionFunc<
    ExecContextType,
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
    UndoFutType,
    UndoFuncType,
> where
    ExecContextType: ExecContext,
    ActionFuncType: Fn(SagaContext<ExecContextType>) -> ActionFutType
        + Send
        + Sync
        + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput, SagaActionError>>
        + Send
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType:
        Fn(SagaContext<ExecContextType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + 'static,
{
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
    /*
     * The PhantomData type parameter below deserves some explanation.  First:
     * this struct needs to store the above fields of type ActionFuncType and
     * UndoFuncType.  These are async functions (i.e., they produce futures), so
     * we need additional type parameters and trait bounds to describe the
     * futures that they produce.  But we don't actually use these futures in
     * the struct.  Consumers implicitly specify them when they specify the
     * corresponding function type parameters.  This is a typical case for using
     * PhantomData to reference these type parameters without really using them.
     *
     * Like many future types, ActionFutType and UndoFutType will be Send, but
     * not necessarily Sync.  (We don't want to impose Sync on the caller
     * because many useful futures are not Sync -- like
     * hyper::client::ResponseFuture, for example.)  As a result, the obvious
     * choice of `PhantomData<(ActionFutType, UndoFutType)>` won't be Sync, and
     * then this struct (SagaActionFunc) won't be Sync -- and that's bad.  See
     * the comment on the SagaAction trait for why this must be Sync.
     *
     * On the other hand, the type `PhantomData<fn() -> (ActionFutType,
     * UndoFutType)>` is Sync and also satisfies our need to reference these
     * type parameters in the struct's contents.
     */
    phantom: PhantomData<fn() -> (ExecContextType, ActionFutType, UndoFutType)>,
}

impl<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
    SagaActionFunc<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ExecContextType: ExecContext,
    ActionFuncType: Fn(SagaContext<ExecContextType>) -> ActionFutType
        + Send
        + Sync
        + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput, SagaActionError>>
        + Send
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType:
        Fn(SagaContext<ExecContextType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + 'static,
{
    /**
     * Construct a `SagaAction` from a pair of functions, using `action_func`
     * for the action and `undo_func` for the undo action
     *
     * We return the result as a `Arc<dyn SagaAction>` so that it can be used
     * directly where `SagaAction`s are expected.  (The struct `SagaActionFunc`
     * has no interfaces of its own so there's generally no need to have the
     * specific type.)
     */
    pub fn new_action(
        action_func: ActionFuncType,
        undo_func: UndoFuncType,
    ) -> Arc<dyn SagaAction<ExecContextType>> {
        Arc::new(SagaActionFunc {
            action_func,
            undo_func,
            phantom: PhantomData,
        })
    }
}

/*
 * TODO-cleanup why can't new_action_noop_undo live in the SagaAction namespace?
 */

async fn undo_noop<ExecContextType: ExecContext>(
    sgctx: SagaContext<ExecContextType>,
) -> SagaUndoResult {
    eprintln!("<noop undo for node: \"{}\">", sgctx.node_label());
    Ok(())
}

/**
 * Given a function `f`, return a `SagaActionFunc` that uses `f` as the action
 * and provides a no-op undo function (which does nothing and always succeeds).
 */
pub fn new_action_noop_undo<
    ExecContextType,
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
>(
    f: ActionFuncType,
) -> Arc<dyn SagaAction<ExecContextType>>
where
    ExecContextType: ExecContext,
    ActionFuncType: Fn(SagaContext<ExecContextType>) -> ActionFutType
        + Send
        + Sync
        + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput, SagaActionError>>
        + Send
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
{
    SagaActionFunc::new_action(f, undo_noop)
}

#[async_trait]
impl<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > SagaAction<ExecContextType>
    for SagaActionFunc<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ExecContextType: ExecContext,
    ActionFuncType: Fn(SagaContext<ExecContextType>) -> ActionFutType
        + Send
        + Sync
        + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput, SagaActionError>>
        + Send
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType:
        Fn(SagaContext<ExecContextType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + 'static,
{
    async fn do_it(
        &self,
        sgctx: SagaContext<ExecContextType>,
    ) -> SagaActionResult {
        let fut = { (self.action_func)(sgctx) };
        /*
         * Execute the caller's function and translate its type into the generic
         * JsonValue that the framework uses to store action outputs.
         */
        fut.await
            .and_then(|func_output| {
                serde_json::to_value(func_output).map_err(|e| {
                    SagaActionError::SerializeFailed { message: e.to_string() }
                })
            })
            .map(Arc::new)
    }

    async fn undo_it(
        &self,
        sgctx: SagaContext<ExecContextType>,
    ) -> SagaUndoResult {
        let fut = { (self.undo_func)(sgctx) };
        fut.await
    }
}

impl<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Debug
    for SagaActionFunc<
        ExecContextType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ExecContextType: ExecContext,
    ActionFuncType: Fn(SagaContext<ExecContextType>) -> ActionFutType
        + Send
        + Sync
        + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput, SagaActionError>>
        + Send
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType:
        Fn(SagaContext<ExecContextType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
         * The type name for a function includes its name, so it's a handy
         * summary for debugging.
         */
        f.write_str(&type_name::<ActionFuncType>())
    }
}
