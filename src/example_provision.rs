/*!
 * Common code shared by examples
 */

use crate::new_action_noop_undo;
use crate::ActionContext;
use crate::ActionError;
use crate::ActionFuncResult;
use crate::SagaId;
use crate::SagaTemplate;
use crate::SagaTemplateBuilder;
use crate::SagaType;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/*
 * Demo provision saga:
 *
 *          create instance (database)
 *              |  |  |
 *       +------+  +  +-------------+
 *       |         |                |
 *       v         v                v
 *    alloc IP   create volume    pick server
 *       |         |                |
 *       +------+--+                v
 *              |             allocate server resources
 *              |                   |
 *              +-------------------+
 *              |
 *              v
 *          configure instance (server)
 *              |
 *              v
 *          attach volume
 *              |
 *              v
 *          boot instance
 */

#[derive(Debug)]
pub struct ExampleSagaType {}
impl SagaType for ExampleSagaType {
    type ExecContextType = ExampleContext;
    type SagaParamsType = ExampleParams;
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExampleParams {
    pub instance_name: String,
}

#[derive(Debug, Default)]
pub struct ExampleContext;

type SagaExampleContext = ActionContext<ExampleSagaType>;

#[derive(Debug, Deserialize, Error, Serialize)]
enum ExampleError {
    #[error("example error")]
    AnError,
}

type ExFuncResult<T> = ActionFuncResult<T, ActionError>;

/* TODO-cleanup can we implement this generically? */
impl From<ExampleError> for ActionError {
    fn from(t: ExampleError) -> ActionError {
        ActionError::action_failed(t)
    }
}

/**
 * Returns a demo "VM provision" saga
 *
 * The actions in this saga do essentially nothing.  They print out what node is
 * running, they produce some data, and they consume some data from previous
 * nodes.  The intent is just to exercise the API.  You can interact with this
 * using the `demo-provision` example.
 */
pub fn make_example_provision_saga() -> Arc<SagaTemplate<ExampleSagaType>> {
    let mut w = SagaTemplateBuilder::new();

    w.append(
        "instance_id",
        "InstanceCreate",
        new_action_noop_undo(demo_prov_instance_create),
    );
    w.append_parallel(vec![
        (
            "instance_ip",
            "VpcAllocIp",
            new_action_noop_undo(demo_prov_vpc_alloc_ip),
        ),
        (
            "volume_id",
            "VolumeCreate",
            new_action_noop_undo(demo_prov_volume_create),
        ),
        (
            "server_id",
            "ServerAlloc (subsaga)",
            new_action_noop_undo(demo_prov_server_alloc),
        ),
    ]);
    w.append(
        "instance_configure",
        "InstanceConfigure",
        new_action_noop_undo(demo_prov_instance_configure),
    );
    w.append(
        "volume_attach",
        "VolumeAttach",
        new_action_noop_undo(demo_prov_volume_attach),
    );
    w.append(
        "instance_boot",
        "InstanceBoot",
        new_action_noop_undo(demo_prov_instance_boot),
    );
    w.append("print", "Print", new_action_noop_undo(demo_prov_print));
    Arc::new(w.build())
}

async fn demo_prov_instance_create(
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!(
        "running action: {} (instance name: {})",
        sgctx.node_label(),
        sgctx.saga_params().instance_name
    );
    /* exercise saga parameters */
    /* make up an instance ID */
    let instance_id = 1211u64;
    Ok(instance_id)
}

async fn demo_prov_vpc_alloc_ip(
    sgctx: SagaExampleContext,
) -> ExFuncResult<String> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using some data from a previous node */
    let instance_id = sgctx.lookup::<u64>("instance_id")?;
    assert_eq!(instance_id, 1211);
    /* make up an IP (simulate allocation) */
    let ip = String::from("10.120.121.122");
    Ok(ip)
}

/*
 * The next two steps are in a subsaga!
 */
#[derive(Debug)]
struct ExampleSubsagaType {}
impl SagaType for ExampleSubsagaType {
    type ExecContextType = ExampleContext;
    type SagaParamsType = ExampleSubsagaParams;
}

#[derive(Debug, Deserialize, Serialize)]
struct ExampleSubsagaParams {
    number_of_things: usize,
}

type SubsagaExampleContext = ActionContext<ExampleSubsagaType>;

async fn demo_prov_server_alloc(
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());

    let mut w = SagaTemplateBuilder::new();
    w.append(
        "server_id",
        "ServerPick",
        new_action_noop_undo(demo_prov_server_pick),
    );
    w.append(
        "server_reserve",
        "ServerReserve",
        new_action_noop_undo(demo_prov_server_reserve),
    );
    let sg = Arc::new(w.build());

    /*
     * The uuid here is deterministic solely for the smoke tests.  It would
     * probably be better to have a way to get uuids from the ActionContext, and
     * have a mode where those come from a seeded random number generator (or
     * some other controlled source for testing).
     */
    let saga_id = SagaId(
        Uuid::parse_str("bcf32552-2b54-485b-bf13-b316daa7d1d4").unwrap(),
    );
    let fut = sgctx
        .child_saga::<ExampleSubsagaType>(
            saga_id,
            sg,
            "server_alloc".to_string(),
            ExampleSubsagaParams { number_of_things: 1 },
        )
        .await?;
    let result = fut.await;
    match result.kind {
        Ok(success) => {
            let server_allocated: Arc<ServerAllocResult> =
                success.lookup_output("server_reserve")?;
            Ok(server_allocated.server_id)
        }
        Err(failure) => Err(failure.error_source),
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ServerAllocResult {
    server_id: u64,
}

async fn demo_prov_server_pick(
    sgctx: SubsagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise subsaga parameters */
    assert_eq!(sgctx.saga_params().number_of_things, 1);
    /* make up ("allocate") a new server id */
    let server_id = 1212u64;
    Ok(server_id)
}

async fn demo_prov_server_reserve(
    sgctx: SubsagaExampleContext,
) -> ExFuncResult<ServerAllocResult> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise subsaga parameters */
    assert_eq!(sgctx.saga_params().number_of_things, 1);
    /* exercise using data from previous nodes */
    let server_id = sgctx.lookup::<u64>("server_id")?;
    assert_eq!(server_id, 1212);
    /* package this up for downstream consumers */
    Ok(ServerAllocResult { server_id })
}

async fn demo_prov_volume_create(
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    /* make up ("allocate") a volume id */
    let volume_id = 1213u64;
    Ok(volume_id)
}
async fn demo_prov_instance_configure(
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);
    Ok(())
}
async fn demo_prov_volume_attach(
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);
    Ok(())
}
async fn demo_prov_instance_boot(
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);
    Ok(())
}

async fn demo_prov_print(sgctx: SagaExampleContext) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    eprintln!("printing final state:");
    let instance_id = sgctx.lookup::<u64>("instance_id")?;
    eprintln!("  instance id: {}", instance_id);
    let ip = sgctx.lookup::<String>("instance_ip")?;
    eprintln!("  IP address: {}", ip);
    let volume_id = sgctx.lookup::<u64>("volume_id")?;
    eprintln!("  volume id: {}", volume_id);
    let server_id = sgctx.lookup::<u64>("server_id")?;
    eprintln!("  server id: {}", server_id);
    Ok(())
}
