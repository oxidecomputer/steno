/*!
 * Common code shared by examples
 */

use crate::new_action_noop_undo;
use crate::ActionContext;
use crate::ActionError;
use crate::ActionFuncResult;
use crate::ActionName;
use crate::ActionRegistry;
use crate::Dag;
use crate::DagBuilder;
use crate::Node;
use crate::NodeConcurrency;
use crate::NodeSpec;
use crate::SagaName;
use crate::SagaType;
use crate::SubsagaSpec;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use thiserror::Error;

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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExampleParams {
    pub instance_name: String,
    pub number_of_instances: u16,
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

#[derive(Debug)]
struct ExampleSubsagaType {}
impl SagaType for ExampleSubsagaType {
    type ExecContextType = ExampleContext;
}

#[derive(Debug, Deserialize, Serialize)]
struct ExampleSubsagaParams {
    number_of_things: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct ServerAllocResult {
    server_id: u64,
}

/* TODO-cleanup can we implement this generically? */
impl From<ExampleError> for ActionError {
    fn from(t: ExampleError) -> ActionError {
        ActionError::action_failed(t)
    }
}

/// Create an ActionRegistry for use with the Saga DAG
pub fn make_example_action_registry() -> Arc<ActionRegistry<ExampleSagaType>> {
    let mut registry = ActionRegistry::new();

    // Create a start action that is capable of returning the saga parameters
    // for an instance creation saga. Each saga or subsaga needs one of these.
    registry.register(
        ActionName::new("instance_create_params"),
        new_action_noop_undo(demo_prov_instance_create_params),
    );

    registry.register(
        ActionName::new("instance_create"),
        new_action_noop_undo(demo_prov_instance_create),
    );
    registry.register(
        ActionName::new("vpc_alloc_ip"),
        new_action_noop_undo(demo_prov_vpc_alloc_ip),
    );
    registry.register(
        ActionName::new("volume_create"),
        new_action_noop_undo(demo_prov_volume_create),
    );
    registry.register(
        ActionName::new("instance_configure"),
        new_action_noop_undo(demo_prov_instance_configure),
    );
    registry.register(
        ActionName::new("volume_attach"),
        new_action_noop_undo(demo_prov_volume_attach),
    );
    registry.register(
        ActionName::new("instance_boot"),
        new_action_noop_undo(demo_prov_instance_boot),
    );
    registry.register(
        ActionName::new("print"),
        new_action_noop_undo(demo_prov_print),
    );

    // Subsaga actions are registered just like any other action
    // The order of registration doesn't matter
    registry.register(
        ActionName::new("server_alloc_params"),
        new_action_noop_undo(demo_prov_server_alloc_params),
    );
    registry.register(
        ActionName::new("server_pick"),
        new_action_noop_undo(demo_prov_server_pick),
    );
    registry.register(
        ActionName::new("server_reserve"),
        new_action_noop_undo(demo_prov_server_reserve),
    );

    Arc::new(registry)
}

// Create a subsaga for server allocation
pub fn server_alloc_subsaga<'a>() -> SubsagaSpec<'a> {
    SubsagaSpec(
        NodeSpec {
            name: "server_alloc_params",
            label: "ReturnServerAllocParams",
            action: "server_alloc_params",
        },
        vec![NodeConcurrency::Linear(vec![
            NodeSpec {
                name: "server_id",
                label: "ServerPick",
                action: "server_pick",
            },
            NodeSpec {
                name: "server_reserve",
                label: "ServerReserve",
                action: "server_reserve",
            },
        ])],
    )
}

/// Create a dag that describes a "VM Provision" Saga
///
/// The actions in this saga do essentially nothing. They print out what
/// node is running, they produce some data, and they consume some data
/// from previous nodes. The intent is just to exercise the API. You can
/// interact with this  using the `demo-provision` example.
pub fn make_example_provision_dag(params: &ExampleParams) -> Arc<Dag> {
    let name = SagaName::new("DemoVmProvision");
    let mut d = DagBuilder::new(name);

    // The saga instance Id (not related to VM instance)
    // TODO(AJS): Name this something else?
    let saga_instance_id = 0;
    d.append(Node::new_root(
        "instance_create_params",
        saga_instance_id,
        "InstanceCreateStart",
        ActionName::new("instance_create_params"),
        params,
    ));

    d.append(Node::new_child(
        "instance_id",
        saga_instance_id,
        "InstanceCreate",
        ActionName::new("instance_create"),
    ));
    d.append_parallel(vec![
        Node::new_child(
            "instance_ip",
            saga_instance_id,
            "VpcAllocIp",
            ActionName::new("vpc_alloc_ip"),
        ),
        Node::new_child(
            "volume_id",
            saga_instance_id,
            "VolumeCreate",
            ActionName::new("volume_create"),
        ),
    ]);

    // Take a subsaga spec and add its nodes to the DAG with the given
    // instance id and parameters.
    //
    // TODO: The original code allowed running saga nodes in parallel with a subsaga.
    // Do we need/want to enable that?
    let subsaga_instance_id = 1;
    let subsaga_params = ExampleSubsagaParams { number_of_things: 1 };
    d.append_subsaga(
        &server_alloc_subsaga(),
        subsaga_instance_id,
        &subsaga_params,
    );

    // Append nodes that will run after the subsaga completes
    d.append(Node::new_child(
        "instance_configure",
        saga_instance_id,
        "InstanceConfigure",
        ActionName::new("instance_configure"),
    ));
    d.append(Node::new_child(
        "volume_attach",
        saga_instance_id,
        "VolumeAttach",
        ActionName::new("volume_attach"),
    ));
    d.append(Node::new_child(
        "instance_boot",
        saga_instance_id,
        "InstanceBoot",
        ActionName::new("instance_boot"),
    ));
    d.append(Node::new_child(
        "print",
        saga_instance_id,
        "Print",
        ActionName::new("print"),
    ));

    Arc::new(d.build())
}

// This action takes the parameters from the node and outputs them so they
// can be looked up by subsequent nodes.
async fn demo_prov_instance_create_params(
    _instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<ExampleParams> {
    let params = sgctx.create_params::<ExampleParams>()?;

    eprintln!(
        "running action: {} (instance name: {})",
        sgctx.node_label(),
        params.instance_name
    );

    Ok(params)
}

async fn demo_prov_instance_create(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    let params =
        sgctx.lookup::<ExampleParams>("instance_create_params", instance_id)?;
    eprintln!(
        "running action: {} (instance name: {})",
        sgctx.node_label(),
        params.instance_name
    );
    /* exercise saga parameters */
    /* make up an instance ID */
    let instance_id = 1211u64;
    Ok(instance_id)
}

async fn demo_prov_vpc_alloc_ip(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<String> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using some data from a previous node */
    let instance_id = sgctx.lookup::<u64>("instance_id", instance_id)?;
    assert_eq!(instance_id, 1211);
    /* make up an IP (simulate allocation) */
    let ip = String::from("10.120.121.122");
    Ok(ip)
}

// The root subsaga action. It takes the parameters from the Dag node and
// outputs them.
async fn demo_prov_server_alloc_params(
    _instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<ExampleSubsagaParams> {
    eprintln!("running action: {}", sgctx.node_label());
    let params = sgctx.create_params::<ExampleSubsagaParams>()?;
    Ok(params)
}

// Another subsaga action
async fn demo_prov_server_pick(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    let params = sgctx
        .lookup::<ExampleSubsagaParams>("server_alloc_params", instance_id)?;
    /* exercise subsaga parameters */
    assert_eq!(params.number_of_things, 1);
    /* make up ("allocate") a new server id */
    let server_id = 1212u64;
    Ok(server_id)
}

// The last subsaga action
async fn demo_prov_server_reserve(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<ServerAllocResult> {
    eprintln!("running action: {}", sgctx.node_label());
    let params = sgctx
        .lookup::<ExampleSubsagaParams>("server_alloc_params", instance_id)?;

    /* exercise subsaga parameters */
    assert_eq!(params.number_of_things, 1);
    /* exercise using data from previous nodes */
    let server_id = sgctx.lookup::<u64>("server_id", instance_id)?;
    assert_eq!(server_id, 1212);
    /* package this up for downstream consumers */
    Ok(ServerAllocResult { server_id })
}

async fn demo_prov_volume_create(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id", instance_id)?, 1211);
    /* make up ("allocate") a volume id */
    let volume_id = 1213u64;
    Ok(volume_id)
}

async fn demo_prov_instance_configure(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id", instance_id)?, 1211);

    let params =
        sgctx.lookup::<ExampleParams>("instance_create_params", instance_id)?;
    assert_eq!(params.number_of_instances, 1);
    // Allocated by the subsaga. We know there is only one instance by looking
    // at the params. In cases with more subsagas we could loop over them. For
    // this example we decided to start the counting at 1 just to differentiate
    // the instance ids of the parent and child sagas.
    assert_eq!(sgctx.lookup::<u64>("server_id", 1)?, 1212);

    assert_eq!(sgctx.lookup::<u64>("volume_id", instance_id)?, 1213);
    Ok(())
}
async fn demo_prov_volume_attach(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id", instance_id)?, 1211);
    assert_eq!(sgctx.lookup::<u64>("volume_id", instance_id)?, 1213);

    // We know there is only one instance of the subsaga that created a server id
    assert_eq!(sgctx.lookup::<u64>("server_id", 1)?, 1212);
    Ok(())
}
async fn demo_prov_instance_boot(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id", instance_id)?, 1211);
    assert_eq!(sgctx.lookup::<u64>("volume_id", instance_id)?, 1213);

    // We know there is only one instance of the subsaga that created a server id
    assert_eq!(sgctx.lookup::<u64>("server_id", 1)?, 1212);
    Ok(())
}

async fn demo_prov_print(
    instance_id: u16,
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    eprintln!("printing final state:");
    let vm_instance_id = sgctx.lookup::<u64>("instance_id", instance_id)?;
    eprintln!("  instance id: {}", vm_instance_id);
    let ip = sgctx.lookup::<String>("instance_ip", instance_id)?;
    eprintln!("  IP address: {}", ip);
    let volume_id = sgctx.lookup::<u64>("volume_id", instance_id)?;
    eprintln!("  volume id: {}", volume_id);
    let server_id = sgctx.lookup::<u64>("server_id", 1)?;
    eprintln!("  server id: {}", server_id);
    Ok(())
}
