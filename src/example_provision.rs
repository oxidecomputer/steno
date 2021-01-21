/*!
 * Common code shared by examples
 */

use crate::new_action_noop_undo;
use crate::SagaContext;
use crate::SagaFuncResult;
use crate::SagaTemplate;
use crate::SagaTemplateBuilder;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

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

/**
 * Returns a demo "VM provision" saga
 *
 * The actions in this saga do essentially nothing.  They print out what node is
 * running, they produce some data, and they consume some data from previous
 * nodes.  The intent is just to exercise the API.  You can interact with this
 * using the `demo-provision` example.
 */
pub fn make_provision_saga() -> Arc<SagaTemplate> {
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

async fn demo_prov_instance_create(sgctx: SagaContext) -> SagaFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* make up an instance ID */
    let instance_id = 1211u64;
    Ok(instance_id)
}

async fn demo_prov_vpc_alloc_ip(sgctx: SagaContext) -> SagaFuncResult<String> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using some data from a previous node */
    let instance_id = sgctx.lookup::<u64>("instance_id");
    assert_eq!(instance_id, 1211);
    /* make up an IP (simulate allocation) */
    let ip = String::from("10.120.121.122");
    Ok(ip)
}

/*
 * The next two steps are in a subsaga!
 */
async fn demo_prov_server_alloc(sgctx: SagaContext) -> SagaFuncResult<u64> {
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

    let e = sgctx.child_saga(sg).await;
    e.run().await;
    let result = e.result();
    let server_allocated: Arc<ServerAllocResult> =
        result.lookup_output("server_reserve")?;
    Ok(server_allocated.server_id)
}

#[derive(Debug, Deserialize, Serialize)]
struct ServerAllocResult {
    server_id: u64,
}

async fn demo_prov_server_pick(sgctx: SagaContext) -> SagaFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* make up ("allocate") a new server id */
    let server_id = 1212u64;
    Ok(server_id)
}

async fn demo_prov_server_reserve(
    sgctx: SagaContext,
) -> SagaFuncResult<ServerAllocResult> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    let server_id = sgctx.lookup::<u64>("server_id");
    assert_eq!(server_id, 1212);
    /* package this up for downstream consumers */
    Ok(ServerAllocResult { server_id })
}

async fn demo_prov_volume_create(sgctx: SagaContext) -> SagaFuncResult<u64> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id"), 1211);
    /* make up ("allocate") a volume id */
    let volume_id = 1213u64;
    Ok(volume_id)
}
async fn demo_prov_instance_configure(
    sgctx: SagaContext,
) -> SagaFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}
async fn demo_prov_volume_attach(sgctx: SagaContext) -> SagaFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}
async fn demo_prov_instance_boot(sgctx: SagaContext) -> SagaFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(sgctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(sgctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(sgctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}

async fn demo_prov_print(sgctx: SagaContext) -> SagaFuncResult<()> {
    eprintln!("running action: {}", sgctx.node_label());
    eprintln!("printing final state:");
    let instance_id = sgctx.lookup::<u64>("instance_id");
    eprintln!("  instance id: {}", instance_id);
    let ip = sgctx.lookup::<String>("instance_ip");
    eprintln!("  IP address: {}", ip);
    let volume_id = sgctx.lookup::<u64>("volume_id");
    eprintln!("  volume id: {}", volume_id);
    let server_id = sgctx.lookup::<u64>("server_id");
    eprintln!("  server id: {}", server_id);
    Ok(())
}
