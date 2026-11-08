//! Script history from `blvm-wallet-index`, served through node-hosted listeners.
//!
//! The database tree and rollback stay in `blvm-wallet-index`. This crate adds the
//! Esplora paths that crate does not serve, and the `/api` prefix.

use blvm_node::storage::database::Tree;
use blvm_node::storage::hashing::{double_sha256, hash_to_rpc_hex};
use blvm_wallet_index::{header_tip, reply_line};
use serde_json::{Map, Value};

/// `FeeEstimatorConfig::default` `lnd_targets` in `blvm-node/src/config/fees.rs`.
/// `GetFeeTable` is asked for these targets. A missing quote is omitted.
pub const FEE_ESTIMATE_TARGETS: &[u32] = &[1, 2, 3, 6, 12, 24, 48, 144, 504, 1008];

pub const LEGACY_BIND_MESSAGE: &str = "blvm-wallet-index listen and esplora moved to \
[services.electrum] and [services.esplora]; blvm-index does not bind those sockets";

/// The old module keys are set. Log [`LEGACY_BIND_MESSAGE`] and do not bind.
pub fn legacy_bind_requested(listen: &Option<String>, esplora: &Option<String>) -> bool {
    listen.as_ref().is_some_and(|value| !value.trim().is_empty())
        || esplora.as_ref().is_some_and(|value| !value.trim().is_empty())
}

/// Esplora `GET /fee-estimates`: target keys to sat/vB. `sat/kvB / 1000`.
pub fn fee_estimates_body(quotes: &[(u32, u64)]) -> String {
    let mut map = Map::new();
    for (target, sat_per_kvb) in quotes {
        if *target == 0 || *sat_per_kvb == 0 {
            continue;
        }
        let sat_per_vb = *sat_per_kvb as f64 / 1000.0;
        map.insert(target.to_string(), Value::from(sat_per_vb));
    }
    Value::Object(map).to_string()
}

/// Status, content type, and body. `quotes` are conservative combined sat/kvB.
pub fn esplora_response(tree: &dyn Tree, path_and_query: &str, quotes: &[(u32, u64)]) -> (u16, &'static str, Vec<u8>) {
    let path = path_and_query.split('?').next().unwrap_or(path_and_query);
    let path = path
        .strip_prefix("/api")
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .unwrap_or(path);
    let path = if path.is_empty() { "/" } else { path };
    if path == "/fee-estimates" {
        let body = fee_estimates_body(quotes);
        if body == "{}" {
            return (503, "text/plain", b"fee estimates unavailable".to_vec());
        }
        return (200, "application/json", body.into_bytes());
    }
    if path == "/blocks/tip/hash" {
        return match header_tip(tree) {
            Ok(Some((_, header))) => {
                let hash = double_sha256(&header);
                (200, "text/plain", hash_to_rpc_hex(&hash).into_bytes())
            }
            Ok(None) => (404, "text/plain", b"no header".to_vec()),
            Err(error) => (500, "text/plain", error.to_string().into_bytes()),
        };
    }
    let request = format!("GET {path} HTTP/1.1");
    let (status, content_type, body) = blvm_wallet_index::esplora_response(tree, &request);
    (status, content_type, body.into_bytes())
}

/// One Electrum request buffer in, the reply lines out.
pub fn electrum_reply(tree: &dyn Tree, bytes: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(bytes);
    let mut out = String::new();
    let mut saw_line = false;
    for line in text.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        saw_line = true;
        out.push_str(&reply_line(tree, line));
    }
    if !saw_line {
        return Vec::new();
    }
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use blvm_node::module::services::{
        BindRequest, HostedModule, ServiceHost, ServiceHttpRequest, ServiceHttpResponse,
        ServiceKind, StreamEvent, DEFAULT_MAX_CONNECTIONS,
    };
    use blvm_protocol::block::calculate_tx_id;
    use blvm_protocol::{
        Block, BlockHeader, OutPoint, Transaction, TransactionInput, TransactionOutput,
    };
    use blvm_sdk::module::ModuleDb;
    use blvm_wallet_index::{TREE_NAME, history, index_block};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    const ADDRESS: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";

    fn coinbase(script: &[u8]) -> Transaction {
        Transaction {
            version: 2,
            inputs: vec![TransactionInput {
                prevout: OutPoint {
                    hash: [0u8; 32],
                    index: 0xffff_ffff,
                },
                script_sig: Vec::new(),
                sequence: 0xffff_ffff,
            }]
            .into(),
            outputs: vec![TransactionOutput {
                value: 50_000,
                script_pubkey: script.to_vec(),
            }]
            .into(),
            lock_time: 0,
        }
    }

    fn block_at(tx: Transaction, height: u64) -> Block {
        Block::from_parts(
            BlockHeader {
                nonce: height,
                ..BlockHeader::default()
            },
            Box::from(vec![tx]),
        )
    }

    struct Front {
        tree: Arc<dyn Tree>,
        quotes: Vec<(u32, u64)>,
    }

    #[async_trait]
    impl HostedModule for Front {
        async fn http(&self, req: ServiceHttpRequest) -> Result<ServiceHttpResponse, String> {
            let (status, content_type, body) =
                esplora_response(self.tree.as_ref(), &req.path_and_query, &self.quotes);
            Ok(ServiceHttpResponse {
                status,
                headers: vec![("content-type".to_string(), content_type.to_string())],
                body,
            })
        }

        async fn stream(&self, event: StreamEvent) -> Result<Vec<u8>, String> {
            match event {
                StreamEvent::Data { bytes, .. } => Ok(electrum_reply(self.tree.as_ref(), &bytes)),
                StreamEvent::Open { .. } | StreamEvent::Close { .. } => Ok(Vec::new()),
            }
        }
    }

    #[test]
    fn a_renamed_wallet_index_directory_still_has_its_rows() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("blvm-wallet-index");
        std::fs::create_dir_all(&old).unwrap();
        let script = blvm_node::rpc::rawtx::address_string_to_script_pubkey(ADDRESS).unwrap();
        let tx = coinbase(&script);
        let txid = calculate_tx_id(&tx);
        {
            let db = ModuleDb::open(&old).unwrap();
            let tree = db.tree(TREE_NAME).unwrap();
            index_block(tree.as_ref(), &block_at(tx, 4), 4).unwrap();
            assert_eq!(history(tree.as_ref(), &script).unwrap().len(), 1);
        }
        let renamed = root.path().join("blvm-index");
        std::fs::rename(&old, &renamed).unwrap();
        let db = ModuleDb::open(&renamed).unwrap();
        let tree = db.tree(TREE_NAME).unwrap();
        let rows = history(tree.as_ref(), &script).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, txid);
        assert!(legacy_bind_requested(&Some("127.0.0.1:50001".into()), &None));
        assert!(!legacy_bind_requested(&None, &Some("  ".into())));
    }

    #[test]
    fn fee_estimates_are_sat_per_vb_under_either_prefix() {
        let body = fee_estimates_body(&[(1, 1_500), (6, 1_000)]);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["1"], 1.5);
        assert_eq!(parsed["6"], 1.0);
        assert!(parsed.get("2").is_none());
    }

    #[tokio::test]
    async fn electrum_and_esplora_answer_on_node_listeners() {
        let dir = tempfile::tempdir().unwrap();
        let db = ModuleDb::open(dir.path()).unwrap();
        let tree = db.tree(TREE_NAME).unwrap();
        let script = blvm_node::rpc::rawtx::address_string_to_script_pubkey(ADDRESS).unwrap();
        index_block(tree.as_ref(), &block_at(coinbase(&script), 7), 7).unwrap();
        let host = ServiceHost::new();
        let front = Arc::new(Front {
            tree: Arc::clone(&tree),
            quotes: vec![(1, 2_500), (144, 1_000)],
        });
        let addrs = host
            .bind(
                "blvm-index",
                vec![
                    BindRequest {
                        name: "esplora".to_string(),
                        kind: ServiceKind::Http,
                        listen: "127.0.0.1:0".parse().unwrap(),
                        max_connections: DEFAULT_MAX_CONNECTIONS,
                        tls_cert: None,
                    },
                    BindRequest {
                        name: "electrum".to_string(),
                        kind: ServiceKind::Stream,
                        listen: "127.0.0.1:0".parse().unwrap(),
                        max_connections: DEFAULT_MAX_CONNECTIONS,
                        tls_cert: None,
                    },
                ],
                front,
            )
            .await
            .unwrap();
        let http_addr = addrs[0];
        let stream_addr = addrs[1];

        let fees = http_get(http_addr, "/fee-estimates").await;
        assert!(fees.starts_with("HTTP/1.1 200"));
        let json = fees.split("\r\n\r\n").nth(1).unwrap();
        let parsed: Value = serde_json::from_str(json).unwrap();
        assert_eq!(parsed["1"], 2.5);
        assert_eq!(parsed["144"], 1.0);
        let prefixed_fees = http_get(http_addr, "/api/fee-estimates").await;
        assert!(prefixed_fees.starts_with("HTTP/1.1 200"));
        let prefixed_json = prefixed_fees.split("\r\n\r\n").nth(1).unwrap();
        let prefixed_parsed: Value = serde_json::from_str(prefixed_json).unwrap();
        assert_eq!(prefixed_parsed, parsed);

        let prefixed = http_get(http_addr, "/api/blocks/tip/height").await;
        assert!(prefixed.contains("\r\n\r\n7"));
        let hash = http_get(http_addr, "/api/blocks/tip/hash").await;
        let hash_body = hash.split("\r\n\r\n").nth(1).unwrap().trim();
        let header = header_tip(tree.as_ref()).unwrap().unwrap().1;
        let expect = hash_to_rpc_hex(&double_sha256(&header));
        assert_eq!(hash_body, expect);

        let mut electrum = TcpStream::connect(stream_addr).await.unwrap();
        electrum
            .write_all(b"{\"id\":1,\"method\":\"server.ping\",\"params\":[]}\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 512];
        let n = electrum.read(&mut buf).await.unwrap();
        let line = String::from_utf8_lossy(&buf[..n]);
        assert!(line.contains("\"result\":null"), "{line}");
        host.unbind("blvm-index").await;
    }

    #[tokio::test]
    async fn an_empty_fee_table_is_unavailable_on_either_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let db = ModuleDb::open(dir.path()).unwrap();
        let tree = db.tree(TREE_NAME).unwrap();
        let host = ServiceHost::new();
        let front = Arc::new(Front {
            tree,
            quotes: Vec::new(),
        });
        let addrs = host
            .bind(
                "blvm-index",
                vec![BindRequest {
                    name: "esplora".to_string(),
                    kind: ServiceKind::Http,
                    listen: "127.0.0.1:0".parse().unwrap(),
                    max_connections: DEFAULT_MAX_CONNECTIONS,
                    tls_cert: None,
                }],
                front,
            )
            .await
            .unwrap();
        for path in ["/fee-estimates", "/api/fee-estimates"] {
            let response = http_get(addrs[0], path).await;
            assert!(response.starts_with("HTTP/1.1 503"), "{path}: {response}");
            let body = response.split("\r\n\r\n").nth(1).unwrap();
            assert_eq!(body, "fee estimates unavailable");
        }
        host.unbind("blvm-index").await;
    }

    async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut body = Vec::new();
        stream.read_to_end(&mut body).await.unwrap();
        String::from_utf8(body).unwrap()
    }
}
