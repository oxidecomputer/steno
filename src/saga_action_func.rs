//! Saga actions implemented using a pair of functions
//!
//! This is the primary interface for actions that we expose to users.  It's a
//! little fancier than the generic [`Action`] trait.

use crate::saga_action_error::ActionError;
use crate::saga_action_generic::Action;
use crate::saga_action_generic::ActionData;
use crate::saga_action_generic::ActionResult;
use crate::saga_action_generic::SagaType;
use crate::saga_action_generic::UndoResult;
use crate::saga_exec::ActionContext;
use async_trait::async_trait;
use core::any::type_name;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use std::sync::Arc;

/**
 * Result of a function that implements a saga action
 */
/*
 * This differs from [`ActionResult`] because [`ActionResult`] returns a generic
 * type.  The function-oriented interface allows you to return more specific
 * types as long as they implement the [`ActionData`] trait.
 *
 * TODO-design There's no reason that the generic interface couldn't also look
 * like this.  We have this mechanism here to allow `ActionFunc` functions
 * to return specific types while storing the generic thing inside the
 * framework.  We do this translation in the impl of `ActionFunc`.  But we
 * could instead create another layer above `Action` that does this.  This gets
 * complicated and doesn't seem especially useful yet.
 */
pub type ActionFuncResult<T, E> = Result<T, E>;

/**
 * Implementation of [`Action`] that uses ordinary functions for the action and
 * undo action
 */
/*
 * The monstrous type parameters here are simpler than they look.  First,
 * `UserType` encapsulates a bunch of types provided by the consumer.  See
 * `SagaType` for more on that.
 *
 * Ultimately, `ActionFunc` just wraps two asynchronous functions.  Both
 * consume an `ActionContext`.  On success, the action function produces a type
 * that impls `ActionData` and the undo function produces nothing.  Because
 * they're asynchronous and because the first function can produce any type that
 * impls `ActionData`, we get this explosion of type parameters and trait
 * bounds.
 */
pub struct ActionFunc<
    UserType,
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
    UndoFutType,
    UndoFuncType,
> where
    UserType: SagaType,
    ActionFuncType:
        Fn(ActionContext<UserType>) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = ActionFuncResult<ActionFuncOutput, ActionError>>
        + Send
        + 'static,
    ActionFuncOutput: ActionData,
    UndoFuncType:
        Fn(ActionContext<UserType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = UndoResult> + Send + 'static,
{
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
    /*
     * The PhantomData type parameter below deserves some explanation.  First:
     * this struct needs to store the above fields of type ActionFuncType and
     * UndoFuncType.  These are async functions, so we need additional type
     * parameters and trait bounds to describe the futures that they produce.
     * But we don't actually use these futures in the struct.  Consumers
     * implicitly specify them when they specify the corresponding function type
     * parameters.  So far, this is a typical use case for PhantomData to
     * reference these type parameters without storing them.
     *
     * Like many future types, ActionFutType and UndoFutType will be Send, but
     * not necessarily Sync.  (We don't want to impose Sync on the caller
     * because many useful futures are not Sync -- like
     * hyper::client::ResponseFuture, for example.)  As a result, the obvious
     * choice of `PhantomData<(ActionFutType, UndoFutType)>` won't be Sync, and
     * then this struct (ActionFunc) won't be Sync -- and that's bad.  See the
     * comment on the Action trait for why this must be Sync.
     *
     * On the other hand, the type `PhantomData<fn() -> (ActionFutType,
     * UndoFutType)>` is Sync and also satisfies our need to reference these
     * type parameters in the struct's contents.
     */
    #[allow(clippy::type_complexity)]
    phantom: PhantomData<fn() -> (UserType, ActionFutType, UndoFutType)>,
}

impl<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
    ActionFunc<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    UserType: SagaType,
    ActionFuncType:
        Fn(ActionContext<UserType>) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = ActionFuncResult<ActionFuncOutput, ActionError>>
        + Send
        + 'static,
    ActionFuncOutput: ActionData,
    UndoFuncType:
        Fn(ActionContext<UserType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = UndoResult> + Send + 'static,
{
    /**
     * Construct an [`Action`] from a pair of functions, using `action_func` for
     * the action and `undo_func` for the undo action
     *
     * The result is returned as `Arc<dyn Action>` so that it can be used
     * directly where `Action`s are expected.  (The struct `ActionFunc` has no
     * interfaces of its own so there's generally no need to have the specific
     * type.)
     */
    pub fn new_action(
        action_func: ActionFuncType,
        undo_func: UndoFuncType,
    ) -> Arc<dyn Action<UserType>> {
        Arc::new(ActionFunc { action_func, undo_func, phantom: PhantomData })
    }
}

/*
 * TODO-cleanup why can't new_action_noop_undo live in the Action namespace?
 */

async fn undo_noop<UserType: SagaType>(
    _: ActionContext<UserType>,
) -> UndoResult {
    // TODO-log
    Ok(())
}

/**
 * Given a function `f`, return an `ActionFunc` that uses `f` as the action and
 * provides a no-op undo function (which does nothing and always succeeds).
 */
pub fn new_action_noop_undo<
    UserType,
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
>(
    f: ActionFuncType,
) -> Arc<dyn Action<UserType>>
where
    UserType: SagaType,
    ActionFuncType:
        Fn(ActionContext<UserType>) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = ActionFuncResult<ActionFuncOutput, ActionError>>
        + Send
        + 'static,
    ActionFuncOutput: ActionData,
{
    ActionFunc::new_action(f, undo_noop)
}

#[async_trait]
impl<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Action<UserType>
    for ActionFunc<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    UserType: SagaType,
    ActionFuncType:
        Fn(ActionContext<UserType>) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = ActionFuncResult<ActionFuncOutput, ActionError>>
        + Send
        + 'static,
    ActionFuncOutput: ActionData,
    UndoFuncType:
        Fn(ActionContext<UserType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = UndoResult> + Send + 'static,
{
    async fn do_it(&self, sgctx: ActionContext<UserType>) -> ActionResult {
        let fut = { (self.action_func)(sgctx) };
        /*
         * Execute the caller's function and translate its type into the generic
         * serde_json::Value that the framework uses to store action outputs.
         */
        fut.await
            .and_then(|func_output| {
                serde_json::to_value(func_output)
                    .map_err(ActionError::new_serialize)
            })
            .map(Arc::new)
    }

    async fn undo_it(&self, sgctx: ActionContext<UserType>) -> UndoResult {
        let fut = { (self.undo_func)(sgctx) };
        fut.await
    }
}

impl<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Debug
    for ActionFunc<
        UserType,
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    UserType: SagaType,
    ActionFuncType:
        Fn(ActionContext<UserType>) -> ActionFutType + Send + Sync + 'static,
    ActionFutType: Future<Output = ActionFuncResult<ActionFuncOutput, ActionError>>
        + Send
        + 'static,
    ActionFuncOutput: ActionData,
    UndoFuncType:
        Fn(ActionContext<UserType>) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = UndoResult> + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
         * The type name for a function includes its name, so it's a handy
         * summary for debugging.
         */
        f.write_str(&type_name::<ActionFuncType>())
    }
}
