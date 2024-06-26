//! Error types produced by saga actions

use crate::saga_action_generic::ActionData;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use thiserror::Error;

/// An error produced by a saga action
///
/// On failure, actions always return an `ActionError`.  This type can represent
/// a failure from Steno itself or a failure produced by the consumer (e.g., an
/// action whose body fails for some reason).  The various specific errors are
/// documented below.
///
/// You can use your own error type with [`ActionError`].  As long as it meets
/// the requirements of [`ActionData`], you can wrap your error in an
/// [`ActionError::ActionFailed`] variant using
/// [`ActionError::action_failed()`]. Given an [`ActionError::ActionFailed`]
/// variant, you can get your specific type back out again using
/// [`ActionError::convert()`].
///
/// Note that the conversion back to your specific error type can fail!  This
/// looks like a downcast, but it's not.  `ActionError`s are typically recorded
/// in the saga log and interpreted later, possibly after a crash and recovery.
/// Whether there was an intervening crash or not, the conversion here
/// deserializes the error from the log into your custom error type.  This won't
/// work if your error type is incompatible with the one that was used to
/// serialize the error in the first place.
///
/// # Example
///
/// ```rust
/// use serde::Deserialize;
/// use serde::Serialize;
/// use steno::ActionError;
///
/// #[derive(Debug, Deserialize, Serialize)]
/// struct MyError { message: String }
///
/// fn my_func_that_fails() -> Result<(), ActionError> {
///     Err(ActionError::action_failed(MyError { message: "boom!".to_owned() }))
/// }
///
/// fn handle_error(error: ActionError) {
///      match error.convert::<MyError>() {
///          Ok(my_error) => {
///              eprintln!("my action failed because: {}", my_error.message);
///          }
///          Err(other_error) => {
///              eprintln!(
///                  "my action failed because the framework had a problem: {}",
///                  other_error.to_string()
///              );
///          }
///      }
/// }
/// ```
#[derive(Clone, Debug, Deserialize, Error, JsonSchema, Serialize)]
pub enum ActionError {
    /// Action failed due to a consumer-specific error
    #[error("action failed")]
    ActionFailed { source_error: serde_json::Value },

    /// The framework failed to deserialize the saga parameters, an action's
    /// successful result, or an action's error.
    #[error("deserialize failed: {message}")]
    DeserializeFailed { message: String },

    /// The consumer requested that an error be injected instead of running a
    /// particular action's node.
    #[error("error injected")]
    InjectedError,

    /// The framework failed to serialize the saga parameters, an action's
    /// successful result, or an action's error.
    #[error("serialize failed: {message}")]
    SerializeFailed { message: String },

    /// The framework failed to create the requested subsaga
    #[error("failed to create subsaga")]
    SubsagaCreateFailed { message: String },
}

impl ActionError {
    /// Wrap a consumer-provided error in an [`ActionError`]
    // TODO-design Is there a way for us to provide this implementation
    // automatically?  It would be nice if a consumer could use their own error
    // type, use `?` in the body of their function, and then have that get
    // wrapped in an ActionError.  We'd like to provide a blanket impl for any
    // supported error type to convert it to ActionError.  But ActionError is
    // itself a supported error type (not necessarily by design), so this
    // doesn't work.
    pub fn action_failed<E: ActionData>(user_error: E) -> ActionError {
        match serde_json::to_value(user_error) {
            Ok(source_error) => ActionError::ActionFailed { source_error },
            Err(serialize_error) => ActionError::new_serialize(serialize_error),
        }
    }

    /// Try to convert the error to a specific consumer error
    ///
    /// This function streamlines the most common use case by decomposing the
    /// error into one of three cases:
    ///
    /// 1. If the error can be converted to the specific error type `E` (which
    ///    means that this is the `ActionError::ActionFailed` variant and the
    ///    wrapped error could be deserialized to `E`), this function returns
    ///    `Ok(E)`.
    ///
    /// 2. If the error is the `ActionError::ActionFailed` variant but could not
    ///    be converted to type `E`, this function returns `Err(ActionError)`
    ///    where the error is the `ActionError::DeserializeFailed`.  This is
    ///    either a bug in the current program or an unexpected operational
    ///    error, as might happen if incompatible versions of the saga executor
    ///    are deployed.  Most consumers will propagate this error up and
    ///    eventually abandon the saga.
    ///
    /// 3. If the error is any other variant, the error itself is returned as
    ///    `Err(ActionError)`.  Most consumers will propagate this error up.
    pub fn convert<E: ActionData>(self) -> Result<E, ActionError> {
        match self {
            ActionError::ActionFailed { source_error } => {
                serde_json::from_value(source_error)
                    .map_err(ActionError::new_deserialize)
            }
            _ => Err(self),
        }
    }

    pub fn new_serialize(source: serde_json::Error) -> ActionError {
        ActionError::SerializeFailed { message: source.to_string() }
    }

    pub fn new_deserialize<E: Display>(source: E) -> ActionError {
        ActionError::DeserializeFailed { message: format!("{:#}", source) }
    }

    pub fn new_subsaga(source: anyhow::Error) -> ActionError {
        let message = format!("{:#}", source);
        ActionError::SubsagaCreateFailed { message }
    }
}

/// An error produced by a failed undo action
///
/// **Returning an error from an undo action should be avoided if at all
/// possible.**  If undo actions experience transient issues, they should
/// generally retry until the undo action completes successfully.  That's
/// because by definition, failure of an undo action means that the saga's
/// actions cannot be unwound.  The system cannot move forward to the desired
/// saga end state nor backward to the initial state.  It's left forever in some
/// partially-updated state.  This should really only happen because of a bug.
/// It should be expected that human intervention will be required to repair the
/// result of an undo action that has failed.
#[derive(Clone, Debug, Deserialize, Error, JsonSchema, Serialize)]
#[error("undo action failed permanently: {message}")]
pub struct UndoActionPermanentError {
    message: String,
}

impl From<anyhow::Error> for UndoActionPermanentError {
    fn from(value: anyhow::Error) -> Self {
        UndoActionPermanentError { message: format!("{:#}", value) }
    }
}

impl From<ActionError> for UndoActionPermanentError {
    fn from(value: ActionError) -> Self {
        UndoActionPermanentError::from(anyhow::Error::from(value))
    }
}
