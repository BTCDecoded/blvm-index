//! blvm-index. The node binds Electrum and Esplora. This process does not.

use anyhow::Result;
use blvm_index::{
    electrum_reply, esplora_response, legacy_bind_requested, FEE_ESTIMATE_TARGETS,
    LEGACY_BIND_MESSAGE,
};
use blvm_node::module::ipc::protocol::{
    CliSpec, InvocationResultMessage, InvocationResultPayload, InvocationType,
};
use blvm_node::module::traits::{EventType, ModuleError, NodeAPI};
use blvm_sdk::module::runner::run_module_with_setup;
use blvm_sdk::module::{ModuleBootstrap, ModuleDb};
use blvm_wallet_index::{HeaderHub, StatusHub, WalletIndexConfig, WalletIndexModule, TREE_NAME};
use std::sync::{Arc, RwLock};
use tracing::warn;

const MODULE_NAME: &str = "blvm-index";

async fn refresh(node: &dyn NodeAPI, fees: &RwLock<Vec<(u32, u64)>>) {
    let Ok(table) = node
        .get_fee_table("combined", "conservative", FEE_ESTIMATE_TARGETS)
        .await
    else {
        return;
    };
    let quotes = table
        .quotes
        .into_iter()
        .filter_map(|(target, quote)| quote.map(|quote| (target, quote.sat_per_kvb)))
        .collect();
    *fees.write().unwrap_or_else(|err| err.into_inner()) = quotes;
}

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap = ModuleBootstrap::init_module(MODULE_NAME);
    let db = ModuleDb::open_or_temp(&bootstrap.data_dir, MODULE_NAME)?;
    let outputs = db.tree(TREE_NAME)?;
    let fees = Arc::new(RwLock::new(Vec::new()));
    let tree = Arc::clone(&outputs);
    let fees_dispatch = Arc::clone(&fees);
    let data_dir = bootstrap.data_dir.clone();
    let setup = {
        let outputs = Arc::clone(&outputs);
        let fees = Arc::clone(&fees);
        move |node_api: Arc<dyn NodeAPI>,
              _db: Arc<dyn blvm_node::storage::database::Database>,
              dir: &std::path::Path| {
            let outputs = Arc::clone(&outputs);
            let fees = Arc::clone(&fees);
            let dir = dir.to_path_buf();
            async move {
                let config = WalletIndexConfig::load(dir.join("config.toml")).unwrap_or_default();
                if legacy_bind_requested(&config.listen, &config.esplora) {
                    warn!("{LEGACY_BIND_MESSAGE}");
                }
                let node = Arc::clone(&node_api);
                let cached = Arc::clone(&fees);
                tokio::spawn(async move {
                    refresh(node.as_ref(), &cached).await;
                });
                let module = WalletIndexModule::with_hubs(
                    outputs,
                    StatusHub::default(),
                    HeaderHub::default(),
                );
                Ok((module.clone(), module))
            }
        }
    };
    let mut events = WalletIndexModule::event_types();
    events.push(EventType::FeeTableReady);
    run_module_with_setup(
        &bootstrap.socket_path,
        &bootstrap.module_id,
        MODULE_NAME,
        env!("CARGO_PKG_VERSION"),
        CliSpec {
            version: 1,
            name: MODULE_NAME.to_string(),
            about: Some("Script history, Electrum, and Esplora".to_string()),
            subcommands: Vec::new(),
        },
        &[],
        events,
        move |invocation, ctx, module, cli| {
            let correlation_id = invocation.correlation_id;
            let (success, payload, error) = match invocation.invocation_type {
                InvocationType::ServiceHttp { path_and_query, .. } => {
                    let quotes = fees_dispatch
                        .read()
                        .unwrap_or_else(|err| err.into_inner())
                        .clone();
                    let (status, content_type, body) =
                        esplora_response(tree.as_ref(), &path_and_query, &quotes);
                    (
                        true,
                        Some(InvocationResultPayload::ServiceHttp {
                            status,
                            headers: vec![("content-type".to_string(), content_type.to_string())],
                            body,
                        }),
                        None,
                    )
                }
                InvocationType::StreamData { bytes, .. } => (
                    true,
                    Some(InvocationResultPayload::Stream(electrum_reply(
                        tree.as_ref(),
                        &bytes,
                    ))),
                    None,
                ),
                InvocationType::StreamOpen { .. } | InvocationType::StreamClose { .. } => {
                    (true, None, None)
                }
                InvocationType::Cli { subcommand, args } => {
                    match cli.dispatch_cli(&ctx, &subcommand, &args) {
                        Ok(stdout) => (
                            true,
                            Some(InvocationResultPayload::Cli {
                                stdout,
                                stderr: String::new(),
                                exit_code: 0,
                            }),
                            None,
                        ),
                        Err(err) => (false, None, Some(err.to_string())),
                    }
                }
                InvocationType::Rpc { method, params } => {
                    match module.dispatch_rpc(&method, &params, ctx.db()) {
                        Ok(value) => (true, Some(InvocationResultPayload::Rpc(value)), None),
                        Err(err) => (false, None, Some(err.to_string())),
                    }
                }
                _ => (false, None, Some("unsupported invocation".to_string())),
            };
            InvocationResultMessage {
                correlation_id,
                success,
                payload,
                error,
            }
        },
        move |event, module, ctx| {
            let fees = Arc::clone(&fees);
            let module = module.clone();
            let ctx = ctx.clone();
            async move {
                if event.event_type == EventType::FeeTableReady {
                    if let Some(node) = ctx.node_api() {
                        refresh(node.as_ref(), &fees).await;
                    }
                }
                module.dispatch_event(event, &ctx).await
            }
        },
        setup,
        db.as_db(),
        &data_dir,
    )
    .await
    .map_err(|err: ModuleError| anyhow::anyhow!("{err}"))?;
    Ok(())
}
