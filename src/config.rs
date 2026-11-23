//! Module configuration. Electrum and Esplora bind on the node, not in this process.

use std::path::Path;

use blvm_sdk_macros::config;
use serde::{Deserialize, Serialize};

const RETIRED_BINDS: &[&str] = &["listen", "esplora"];
const RETIRED_ENVS: &[&str] = &["MODULE_CONFIG_LISTEN", "MODULE_CONFIG_ESPLORA"];

/// Index has no module bind keys. Listeners are `[services.electrum]` and `[services.esplora]`.
#[config(name = "blvm-index")]
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct WalletIndexConfig;

blvm_sdk::impl_module_config!(WalletIndexConfig);

impl WalletIndexConfig {
    pub fn to_context_map(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }
}

/// Fail when a config file or node override still names a retired bind key.
pub fn reject_retired_binds(path: &Path) -> Result<(), String> {
    for key in RETIRED_ENVS {
        if std::env::var(key).is_ok() {
            return Err(retired_bind_message());
        }
    }
    if !path.exists() {
        return Ok(());
    }
    let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    let value: toml::Value = toml::from_str(&text).map_err(|error| error.to_string())?;
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    if RETIRED_BINDS.iter().any(|key| table.contains_key(*key)) {
        return Err(retired_bind_message());
    }
    Ok(())
}

fn retired_bind_message() -> String {
    "listen and esplora are not blvm-index settings. Bind with [services.electrum] and [services.esplora]".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retired_bind_key_fails_and_an_empty_file_does_not() {
        let dir = std::env::temp_dir().join(format!("index-binds-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "listen = \"127.0.0.1:50001\"\n").unwrap();
        let error = reject_retired_binds(&path).unwrap_err();
        assert!(error.contains("[services.electrum]"), "{error}");
        std::fs::write(&path, "\n").unwrap();
        assert!(reject_retired_binds(&path).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
