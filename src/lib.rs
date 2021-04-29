//! Steno is an in-progress prototype implementation of distributed sagas.
//! Sagas orchestrate the execution of a set of asynchronous tasks that can
//! fail.  The saga pattern provides useful semantics for unwinding the whole
//! operation when any task fails.  For more on distributed sagas, see [this
//! 2017 JOTB talk by Caitie McCaffrey][1].
//!
//! [1]: https://www.youtube.com/watch?v=0UTOLRTwOX0
//!
//! ## Overview
//!
//! * Write some functions that will be used as _actions_ and _undo actions_ for
//!   your saga.  Package these up with [`ActionFunc::new_action()`].
//! * Use [`SagaTemplateBuilder`] to construct a graph of these actions.
//! * Use [`SagaExecutor`] to execute the saga.
//!
//! This crate is necessarily somewhat complex to use.  **For a detailed,
//! documented example, see examples/trip.rs.**
//!

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
mod saga_action_error;
mod saga_action_func;
mod saga_action_generic;
mod saga_exec;
mod saga_log;
mod saga_template;
mod store;

/*
 * TODO-cleanup The example_provision stuff should probably be in a separate
 * crate that depends on "steno".  That would ensure it only uses public
 * interfaces.  However, the "steno" crate wants to have an example that uses
 * this crate, hence our problem.
 */
pub use example_provision::make_example_provision_saga;
pub use example_provision::ExampleContext;
pub use example_provision::ExampleParams;

pub use saga_action_error::ActionError;
pub use saga_action_func::new_action_noop_undo;
pub use saga_action_func::ActionFunc;
pub use saga_action_func::ActionFuncResult;
pub use saga_action_generic::Action;
pub use saga_action_generic::ActionData;
pub use saga_action_generic::ActionResult;
pub use saga_action_generic::SagaType;
pub use saga_action_generic::UndoResult;
pub use saga_exec::ActionContext;
pub use saga_exec::SagaExecManager;
pub use saga_exec::SagaExecStatus;
pub use saga_exec::SagaResult;
pub use saga_exec::SagaResultErr;
pub use saga_exec::SagaResultOk;
pub use saga_log::SagaLog;
pub use saga_log::SagaNodeEvent;
pub use saga_log::SagaNodeEventType;
pub use saga_template::SagaId;
pub use saga_template::SagaTemplate;
pub use saga_template::SagaTemplateBuilder;
pub use saga_template::SagaTemplateDot;
pub use saga_template::SagaTemplateGeneric;
pub use saga_template::SagaTemplateMetadata;
pub use store::InMemoryStoreBackend;
pub use store::SagaStateView;
pub use store::Store;
pub use store::StoreBackend;
pub use store::read_log;

/*
 * XXX TODO-cleanup This ought not to be exposed.  It's here because we expose
 * SagaTemplateGeneric, which is important, and it has a function that uses this
 * type.  This ought to be a sealed trait where this function is private or
 * something.
 */
pub use store::StoreInternal;

#[macro_use]
extern crate slog;
#[macro_use]
extern crate newtype_derive;
