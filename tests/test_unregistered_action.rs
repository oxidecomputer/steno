//! Tests what happens when running a saga with an unregistered action

use slog::Drain;
use std::sync::Arc;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionRegistry;
use steno::DagBuilder;
use steno::Node;
use steno::SagaDag;
use steno::SagaId;
use steno::SagaName;
use steno::SagaType;
use uuid::Uuid;

fn new_log() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog::o!())
}

#[tokio::test]
async fn unregistered_action() {
    #[derive(Debug)]
    struct TestSaga;
    impl SagaType for TestSaga {
        type ExecContextType = ();
    }
    async fn my_action_func(
        _: ActionContext<TestSaga>,
    ) -> Result<(), ActionError> {
        Ok(())
    }
    let action = steno::new_action_noop_undo("my_action", my_action_func);
    let registry: ActionRegistry<TestSaga> = ActionRegistry::new();
    let mut builder = DagBuilder::new(SagaName::new("my-saga"));
    builder.append(Node::action("my_node", "my_node", &*action));
    let saga = SagaDag::new(
        builder.build().expect("failed to build saga"),
        serde_json::Value::Null,
    );

    let log = new_log();
    let sec = steno::sec(log.clone(), Arc::new(steno::InMemorySecStore::new()));
    let saga_id = SagaId(Uuid::new_v4());
    let context = Arc::new(());
    let saga_future = sec
        .saga_create(
            saga_id,
            Arc::clone(&context),
            Arc::new(saga),
            Arc::new(registry),
        )
        .await
        .expect("failed to create saga");

    sec.saga_start(saga_id).await.expect("failed to start saga running");
    let result = saga_future.await;
    let _ = result.kind.unwrap();
}
