/*!
 * Common code shared by examples
 */

use crate::ActionContext;
use crate::ActionError;
use crate::ActionFuncResult;
use crate::ActionRegistry;
use crate::Dag;
use crate::DagBuilder;
use crate::SagaName;
use crate::SagaType;
use crate::UserNode;
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

#[doc(hidden)]
#[derive(Debug)]
pub struct ExampleSagaType {}
impl SagaType for ExampleSagaType {
    type ExecContextType = ExampleContext;
}

#[doc(hidden)]
#[derive(Debug, Deserialize, Serialize)]
pub struct ExampleParams {
    pub instance_name: String,
    pub number_of_instances: u16,
}

#[doc(hidden)]
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

mod actions {
    use super::ExampleSagaType;
    use crate::new_action_noop_undo;
    use crate::Action;
    use lazy_static::lazy_static;
    use std::sync::Arc;

    lazy_static! {
        pub static ref INSTANCE_CREATE: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "instance_create",
                super::demo_prov_instance_create,
            );
        pub static ref VPC_ALLOC_IP: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo("vpc_alloc_ip", super::demo_prov_vpc_alloc_ip);
        pub static ref VOLUME_CREATE: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "volume_create",
                super::demo_prov_volume_create,
            );
        pub static ref INSTANCE_CONFIGURE: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "instance_configure",
                super::demo_prov_instance_configure,
            );
        pub static ref VOLUME_ATTACH: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "volume_attach",
                super::demo_prov_volume_attach,
            );
        pub static ref INSTANCE_BOOT: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "instance_boot",
                super::demo_prov_instance_boot,
            );
        pub static ref PRINT: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo("print", super::demo_prov_print);
        pub static ref SERVER_PICK: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo("server_pick", super::demo_prov_server_pick);
        pub static ref SERVER_RESERVE: Arc<dyn Action<ExampleSagaType>> =
            new_action_noop_undo(
                "server_reserve",
                super::demo_prov_server_reserve,
            );
    }
}

/// Load our actions into an ActionRegistry
#[doc(hidden)]
pub fn load_example_actions(registry: &mut ActionRegistry<ExampleSagaType>) {
    registry.register(actions::INSTANCE_CREATE.clone());
    registry.register(actions::VPC_ALLOC_IP.clone());
    registry.register(actions::VOLUME_CREATE.clone());
    registry.register(actions::INSTANCE_CONFIGURE.clone());
    registry.register(actions::VOLUME_ATTACH.clone());
    registry.register(actions::INSTANCE_BOOT.clone());
    registry.register(actions::PRINT.clone());
    registry.register(actions::SERVER_PICK.clone());
    registry.register(actions::SERVER_RESERVE.clone());
}

/// Create a subsaga for server allocation
fn server_alloc_subsaga() -> Dag {
    // XXX-dap the "params" here is unused because this is a subsaga
    let params = ();
    let name = SagaName::new("server-alloc");
    let mut d = DagBuilder::new(name, params);
    d.append(UserNode::action(
        "server_id",
        "ServerPick",
        actions::SERVER_PICK.as_ref(),
    ));
    d.append(UserNode::action(
        "server_reserve",
        "ServerReserve",
        actions::SERVER_RESERVE.as_ref(),
    ));

    d.build()
}

/// Create a dag that describes a demo "VM Provision" Saga
///
/// The actions in this saga do essentially nothing. They print out what
/// node is running, they produce some data, and they consume some data
/// from previous nodes. The intent is just to exercise the API. You can
/// interact with this  using the `demo-provision` example.
#[doc(hidden)]
pub fn make_example_provision_dag(params: ExampleParams) -> Arc<Dag> {
    let name = SagaName::new("DemoVmProvision");
    let mut d = DagBuilder::new(name, params);

    d.append(UserNode::action(
        "instance_id",
        "InstanceCreate",
        actions::INSTANCE_CREATE.as_ref(),
    ));
    d.append_parallel(vec![
        UserNode::action(
            "instance_ip",
            "VpcAllocIp",
            actions::VPC_ALLOC_IP.as_ref(),
        ),
        UserNode::action(
            "volume_id",
            "VolumeCreate",
            actions::VOLUME_CREATE.as_ref(),
        ),
    ]);

    // Take a subsaga spec and add its nodes to the DAG with the given
    // instance id and parameters.
    //
    // TODO: The original code allowed running saga nodes in parallel with a
    // subsaga.  Do we need/want to enable that?
    // XXX-dap yes I think so
    let subsaga_params = ExampleSubsagaParams { number_of_things: 1 };
    d.append(UserNode::constant("server_alloc_params", subsaga_params));
    d.append(UserNode::subsaga(
        "server_alloc",
        server_alloc_subsaga(),
        "server_alloc_params",
    ));

    // Append nodes that will run after the subsaga completes
    d.append(UserNode::action(
        "instance_configure",
        "InstanceConfigure",
        actions::INSTANCE_CONFIGURE.as_ref(),
    ));
    d.append(UserNode::action(
        "volume_attach",
        "VolumeAttach",
        actions::VOLUME_ATTACH.as_ref(),
    ));
    d.append(UserNode::action(
        "instance_boot",
        "InstanceBoot",
        actions::INSTANCE_BOOT.as_ref(),
    ));
    d.append(UserNode::action("print", "Print", actions::PRINT.as_ref()));

    Arc::new(d.build())
}

async fn demo_prov_instance_create(
    sgctx: SagaExampleContext,
) -> ExFuncResult<u64> {
    let params = sgctx.saga_params::<ExampleParams>()?;
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

// Another subsaga action
async fn demo_prov_server_pick(sgctx: SagaExampleContext) -> ExFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    let params = sgctx.saga_params::<ExampleSubsagaParams>()?;
    /* exercise subsaga parameters */
    assert_eq!(params.number_of_things, 1);
    /* make up ("allocate") a new server id */
    let server_id = 1212u64;
    Ok(server_id)
}

// The last subsaga action
async fn demo_prov_server_reserve(
    sgctx: SagaExampleContext,
) -> ExFuncResult<ServerAllocResult> {
    eprintln!("running action: {}", sgctx.node_label());
    let params = sgctx.saga_params::<ExampleSubsagaParams>()?;

    /* exercise subsaga parameters */
    assert_eq!(params.number_of_things, 1);
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

    let params = sgctx.saga_params::<ExampleParams>()?;
    assert_eq!(params.number_of_instances, 1);
    assert_eq!(
        sgctx.lookup::<ServerAllocResult>("server_alloc")?.server_id,
        1212
    );

    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);
    Ok(())
}
async fn demo_prov_volume_attach(
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);

    assert_eq!(
        sgctx.lookup::<ServerAllocResult>("server_alloc")?.server_id,
        1212
    );
    Ok(())
}
async fn demo_prov_instance_boot(
    sgctx: SagaExampleContext,
) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(sgctx.lookup::<u64>("volume_id")?, 1213);

    // We know there is only one instance of the subsaga that created a server id
    assert_eq!(
        sgctx.lookup::<ServerAllocResult>("server_alloc")?.server_id,
        1212
    );
    Ok(())
}

async fn demo_prov_print(sgctx: SagaExampleContext) -> ExFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    eprintln!("printing final state:");
    let vm_instance_id = sgctx.lookup::<u64>("instance_id")?;
    eprintln!("  instance id: {}", vm_instance_id);
    let ip = sgctx.lookup::<String>("instance_ip")?;
    eprintln!("  IP address: {}", ip);
    let volume_id = sgctx.lookup::<u64>("volume_id")?;
    eprintln!("  volume id: {}", volume_id);
    let server_id =
        sgctx.lookup::<ServerAllocResult>("server_alloc")?.server_id;
    eprintln!("  server id: {}", server_id);
    Ok(())
}
