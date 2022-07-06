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
use futures::future::BoxFuture;
use std::any::type_name;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
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
 * Trait that expresses the requirements for async functions to be used with
 * `ActionFunc`. This exists just to express the relationships between the types
 * involved in the function, so that they don't have to be repeated everywhere.
 * You don't need to implement it yourself -- a blanket implementation is
 * provided.
 */
pub trait ActionFn<'c, S: SagaType>: Send + Sync + 'static {
    /** Type returned when the future finally resolves. */
    type Output;
    /** Type of the future returned when the function is called. */
    type Future: Future<Output = Self::Output> + Send + 'c;
    /** Call the function. */
    fn act(&'c self, instance_id: u16, ctx: ActionContext<S>) -> Self::Future;
}

/* Blanket impl for Fn types returning futures */
impl<'c, F, S, FF> ActionFn<'c, S> for F
where
    S: SagaType,
    F: Fn(u16, ActionContext<S>) -> FF + Send + Sync + 'static,
    FF: std::future::Future + Send + 'c,
{
    type Future = FF;
    type Output = FF::Output;
    fn act(&'c self, instance_id: u16, ctx: ActionContext<S>) -> Self::Future {
        self(instance_id, ctx)
    }
}

/**
 * Implementation of [`Action`] that uses ordinary functions for the action and
 * undo action
 */
pub struct ActionFunc<ActionFuncType, UndoFuncType> {
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
}

impl<ActionFuncType, UndoFuncType> ActionFunc<ActionFuncType, UndoFuncType> {
    /**
     * Construct an [`Action`] from a pair of functions, using `action_func` for
     * the action and `undo_func` for the undo action
     *
     * The result is returned as `Arc<dyn Action>` so that it can be used
     * directly where `Action`s are expected.  (The struct `ActionFunc` has no
     * interfaces of its own so there's generally no need to have the specific
     * type.)
     */
    pub fn new_action<UserType, ActionFuncOutput>(
        action_func: ActionFuncType,
        undo_func: UndoFuncType,
    ) -> Arc<dyn Action<UserType>>
    where
        UserType: SagaType,
        for<'c> ActionFuncType: ActionFn<
            'c,
            UserType,
            Output = ActionFuncResult<ActionFuncOutput, ActionError>,
        >,
        ActionFuncOutput: ActionData,
        for<'c> UndoFuncType: ActionFn<'c, UserType, Output = UndoResult>,
    {
        Arc::new(ActionFunc { action_func, undo_func })
    }
}

/*
 * TODO-cleanup why can't new_action_noop_undo live in the Action namespace?
 */

/**
 * Given a function `f`, return an `ActionFunc` that uses `f` as the action and
 * provides a no-op undo function (which does nothing and always succeeds).
 */
pub fn new_action_noop_undo<UserType, ActionFuncType, ActionFuncOutput>(
    f: ActionFuncType,
) -> Arc<dyn Action<UserType>>
where
    UserType: SagaType,
    for<'c> ActionFuncType: ActionFn<
        'c,
        UserType,
        Output = ActionFuncResult<ActionFuncOutput, ActionError>,
    >,
    ActionFuncOutput: ActionData,
{
    // TODO-log
    ActionFunc::new_action(f, |_, _| async { Ok(()) })
}

impl<UserType, ActionFuncType, ActionFuncOutput, UndoFuncType> Action<UserType>
    for ActionFunc<ActionFuncType, UndoFuncType>
where
    UserType: SagaType,
    for<'c> ActionFuncType: ActionFn<
        'c,
        UserType,
        Output = ActionFuncResult<ActionFuncOutput, ActionError>,
    >,
    ActionFuncOutput: ActionData,
    for<'c> UndoFuncType: ActionFn<'c, UserType, Output = UndoResult>,
{
    fn do_it(
        &self,
        instance_id: u16,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'_, ActionResult> {
        Box::pin(async move {
            let fut = self.action_func.act(instance_id, sgctx);
            /*
             * Execute the caller's function and translate its type into the
             * generic JsonValue that the framework uses to store action
             * outputs.
             */
            fut.await
                .and_then(|func_output| {
                    serde_json::to_value(func_output)
                        .map_err(ActionError::new_serialize)
                })
                .map(Arc::new)
        })
    }

    fn undo_it(
        &self,
        instance_id: u16,
        sgctx: ActionContext<UserType>,
    ) -> BoxFuture<'_, UndoResult> {
        Box::pin(self.undo_func.act(instance_id, sgctx))
    }
}

impl<ActionFuncType, UndoFuncType> Debug
    for ActionFunc<ActionFuncType, UndoFuncType>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
         * The type name for a function includes its name, so it's a handy
         * summary for debugging.
         */
        f.write_str(&type_name::<ActionFuncType>())
    }
}
