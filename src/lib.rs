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
#![feature(option_expect_none)]
#![feature(or_patterns)]

mod example_provision;
mod saga_action;
mod saga_exec;
mod saga_log;
mod saga_template;

pub use example_provision::make_provision_saga;
pub use saga_action::new_action_noop_undo;
pub use saga_action::SagaActionFunc;
pub use saga_action::SagaError;
pub use saga_action::SagaFuncResult;
pub use saga_action::SagaUndoResult;
pub use saga_exec::SagaContext;
pub use saga_exec::SagaExecResult;
pub use saga_exec::SagaExecutor;
pub use saga_log::SagaLog;
pub use saga_template::SagaTemplate;
pub use saga_template::SagaTemplateBuilder;
