//! Test utilities and fixtures for use across the crate.

use crate::Action;
use crate::ActionContext;
use crate::ActionError;
use crate::ActionFunc;
use crate::ActionRegistry;
use crate::DagBuilder;
use crate::Node;
use crate::SagaDag;
use crate::SagaName;
use crate::SagaType;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, Serialize, Deserialize)]
struct TestParams;

// This context object is a dynamically typed bucket of information for use by
// tests in this crate.
//
// It can be used by tests to monitor the frequency of saga node execution.
#[derive(Debug)]
pub(crate) struct TestContext {
    counters: Mutex<BTreeMap<String, u32>>,
}

impl TestContext {
    pub(crate) fn new() -> Self {
        TestContext { counters: Mutex::new(BTreeMap::new()) }
    }

    // Identifies that a function `name` has been called.
    pub(crate) fn call(&self, name: &str) {
        let mut map = self.counters.lock().unwrap();
        if let Some(count) = map.get_mut(name) {
            *count += 1;
        } else {
            map.insert(name.to_string(), 1);
        }
    }

    // Returns the number of times `name` has been called.
    pub(crate) fn get_count(&self, name: &str) -> u32 {
        let map = self.counters.lock().unwrap();
        if let Some(count) = map.get(name) {
            *count
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub(crate) struct TestSaga;
impl SagaType for TestSaga {
    type ExecContextType = TestContext;
}

pub(crate) fn make_test_saga() -> (Arc<ActionRegistry<TestSaga>>, Arc<SagaDag>)
{
    async fn do_n1(ctx: ActionContext<TestSaga>) -> Result<i32, ActionError> {
        ctx.user_data().call("do_n1");
        Ok(1)
    }
    async fn undo_n1(
        ctx: ActionContext<TestSaga>,
    ) -> Result<(), anyhow::Error> {
        ctx.user_data().call("undo_n1");
        Ok(())
    }

    async fn do_n2(ctx: ActionContext<TestSaga>) -> Result<i32, ActionError> {
        ctx.user_data().call("do_n2");
        Ok(2)
    }
    async fn undo_n2(
        ctx: ActionContext<TestSaga>,
    ) -> Result<(), anyhow::Error> {
        ctx.user_data().call("undo_n2");
        Ok(())
    }

    let mut registry = ActionRegistry::new();
    let action_n1 = ActionFunc::new_action("n1_out", do_n1, undo_n1);
    registry.register(Arc::clone(&action_n1));
    let action_n2 = ActionFunc::new_action("n2_out", do_n2, undo_n2);
    registry.register(Arc::clone(&action_n2));

    let mut builder = DagBuilder::new(SagaName::new("test-saga"));
    builder.append(Node::action("n1_out", "n1", &*action_n1));
    builder.append(Node::action("n2_out", "n2", &*action_n2));
    (
        Arc::new(registry),
        Arc::new(SagaDag::new(
            builder.build().unwrap(),
            serde_json::to_value(TestParams {}).unwrap(),
        )),
    )
}

fn single_action_registry(
) -> (Arc<ActionRegistry<TestSaga>>, Arc<dyn Action<TestSaga>>) {
    async fn do_node(ctx: ActionContext<TestSaga>) -> Result<i32, ActionError> {
        ctx.user_data().call("do_node");
        Ok(1)
    }
    async fn undo_node(
        ctx: ActionContext<TestSaga>,
    ) -> Result<(), anyhow::Error> {
        ctx.user_data().call("undo_node");
        Ok(())
    }

    let mut registry = ActionRegistry::new();
    let action = ActionFunc::new_action("node_action", do_node, undo_node);
    registry.register(Arc::clone(&action));
    (Arc::new(registry), action)
}

// Builds a diamond saga: start -> {a, p} -> c -> end
pub(crate) fn make_diamond_saga(
) -> (Arc<ActionRegistry<TestSaga>>, Arc<SagaDag>) {
    let (registry, action) = single_action_registry();
    let mut builder = DagBuilder::new(SagaName::new("diamond-saga"));
    builder.append_parallel(vec![
        Node::action("a_out", "a", &*action),
        Node::action("p_out", "p", &*action),
    ]);
    builder.append(Node::action("c_out", "c", &*action));
    (
        registry,
        Arc::new(SagaDag::new(
            builder.build().unwrap(),
            serde_json::to_value(TestParams {}).unwrap(),
        )),
    )
}
