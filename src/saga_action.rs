//! Definition of Action trait, core implementations, and related facilities

use crate::saga_exec::SagaContext;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use core::any::type_name;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/*
 * Result, output, and error types used for actions
 */

/** Error produced by a saga action or a saga itself */
pub type SagaError = anyhow::Error;

/** Result of a saga action */
// TODO-cleanup can we drop this Arc?
pub type SagaActionResult = Result<Arc<JsonValue>, SagaError>;
/** Result of a saga undo action */
pub type SagaUndoResult = Result<(), SagaError>;

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
pub type SagaFuncResult<T> = Result<T, SagaError>;

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
 */
#[async_trait]
pub trait SagaAction: Debug + Send + Sync {
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
    async fn do_it(&self, sgctx: SagaContext) -> SagaActionResult;

    /**
     * Executes the compensation action for this saga node, whatever that is.
     */
    async fn undo_it(&self, sgctx: SagaContext) -> SagaUndoResult;
}

/*
 * SagaAction implementations
 */

/** Represents the start node in a graph */
#[derive(Debug)]
pub struct SagaActionStartNode {}

#[async_trait]
impl SagaAction for SagaActionStartNode {
    async fn do_it(&self, _: SagaContext) -> SagaActionResult {
        eprintln!("<action for \"start\" node>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: SagaContext) -> SagaUndoResult {
        eprintln!("<undo for \"start\" node (saga is nearly done unwinding)>");
        Ok(())
    }
}

/** Represents the end node in a graph */
#[derive(Debug)]
pub struct SagaActionEndNode {}

#[async_trait]
impl SagaAction for SagaActionEndNode {
    async fn do_it(&self, _: SagaContext) -> SagaActionResult {
        eprintln!("<action for \"end\" node: saga is nearly done>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: SagaContext) -> SagaUndoResult {
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
impl SagaAction for SagaActionInjectError {
    async fn do_it(&self, sgctx: SagaContext) -> SagaActionResult {
        let message = format!(
            "<boom! error injected instead of action for \
            node \"{}\">",
            sgctx.node_label()
        );
        eprintln!("{}", message);
        Err(anyhow!("{}", message))
    }

    async fn undo_it(&self, _: SagaContext) -> SagaUndoResult {
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
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
    UndoFutType,
    UndoFuncType,
> where
    ActionFuncType: Fn(SagaContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput>>
        + Send
        + Sync
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType: Fn(SagaContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + Sync + 'static,
{
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
    phantom: PhantomData<(ActionFutType, UndoFutType)>,
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
    SagaActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(SagaContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput>>
        + Send
        + Sync
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType: Fn(SagaContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + Sync + 'static,
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
    ) -> Arc<dyn SagaAction> {
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

async fn undo_noop(sgctx: SagaContext) -> SagaUndoResult {
    eprintln!("<noop undo for node: \"{}\">", sgctx.node_label());
    Ok(())
}

/**
 * Given a function `f`, return a `SagaActionFunc` that uses `f` as the action
 * and provides a no-op undo function (which does nothing and always succeeds).
 */
pub fn new_action_noop_undo<ActionFutType, ActionFuncType, ActionFuncOutput>(
    f: ActionFuncType,
) -> Arc<dyn SagaAction>
where
    ActionFuncType: Fn(SagaContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput>>
        + Send
        + Sync
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
{
    SagaActionFunc::new_action(f, undo_noop)
}

#[async_trait]
impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > SagaAction
    for SagaActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(SagaContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput>>
        + Send
        + Sync
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType: Fn(SagaContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + Sync + 'static,
{
    async fn do_it(&self, sgctx: SagaContext) -> SagaActionResult {
        let label = sgctx.node_label().to_owned();
        let fut = { (self.action_func)(sgctx) };
        /*
         * Execute the caller's function and translate its type into the generic
         * JsonValue that the framework uses to store action outputs.
         */
        fut.await
            .with_context(|| format!("executing node \"{}\"", label))
            .and_then(|func_output| {
                serde_json::to_value(func_output).with_context(|| {
                    format!("serializing output from node \"{}\"", label)
                })
            })
            .map(Arc::new)
    }

    async fn undo_it(&self, sgctx: SagaContext) -> SagaUndoResult {
        let fut = { (self.undo_func)(sgctx) };
        fut.await
    }
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Debug
    for SagaActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(SagaContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = SagaFuncResult<ActionFuncOutput>>
        + Send
        + Sync
        + 'static,
    ActionFuncOutput: SagaActionOutput + 'static,
    UndoFuncType: Fn(SagaContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = SagaUndoResult> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
         * The type name for a function includes its name, so it's a handy
         * summary for debugging.
         */
        f.write_str(&type_name::<ActionFuncType>())
    }
}
