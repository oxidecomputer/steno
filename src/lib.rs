//! Steno is an in-progress prototype based on distributed sagas.
//!
//! Sagas help organize execution of a set of asynchronous tasks that can fail,
//! providing useful semantics for unwinding the whole operation when that
//! happens.
//!
//! ## Getting started
//!
//! * Write some functions that will be used as _actions_ and _undo actions_ for
//!   your saga.  Package these into a [`SagaActionFunc`].
//! * Use [`SagaTemplateBuilder`] to build a graph of these actions.
//! * Use [`SagaExecutor`] to execute the saga.

#![deny(elided_lifetimes_in_paths)]
/*
 * We disable the warning for unstable name collisions because we deliberately
 * have some conflicts in rust_features.rs (corresponding to backports of
 * unstable features).  If and when these features are stabilized, we should see
 * warnings that our backported versions are unused and we can remove them.
 */
#![allow(unstable_name_collisions)]

mod example_provision;
mod rust_features;
mod saga_action;
mod saga_exec;
mod saga_log;
mod saga_template;

/*
 * TODO-design TODO-correctness TODO-robustness In a lot of places, we've
 * assumed that the caller should always know what type a node output or error
 * is, and that's fine -- except for the fact that this might be coming in off
 * the wire and something else may have corrupted it.  Maybe we should treat
 * these as explicit operational errors that always bubble up and cause
 * execution of the saga to fail with a generic-type error.
 */

/*
 * TODO-cleanup The example_provision stuff should probably be in a separate
 * crate that depends on "steno".  That would ensure it only uses public
 * interfaces.  However, the "steno" crate wants to have an example that uses
 * this crate, hence our problem.
 */
pub use example_provision::make_provision_saga;
pub use example_provision::ExampleContext;

pub use saga_action::new_action_noop_undo;
pub use saga_action::SagaActionError;
pub use saga_action::SagaActionFunc;
pub use saga_action::SagaActionUserError;
pub use saga_action::SagaFuncResult;
pub use saga_action::SagaUndoResult;
pub use saga_exec::SagaContext;
pub use saga_exec::SagaExecResult;
pub use saga_exec::SagaExecResultErr;
pub use saga_exec::SagaExecResultOk;
pub use saga_exec::SagaExecStatus;
pub use saga_exec::SagaExecutor;
pub use saga_log::SagaLog;
pub use saga_log::SagaLogSerialized;
pub use saga_template::SagaId;
pub use saga_template::SagaTemplate;
pub use saga_template::SagaTemplateBuilder;
pub use saga_template::SagaTemplateDot;
