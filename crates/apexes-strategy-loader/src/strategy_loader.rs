//! Loads a `Box<dyn Strategy>` from a [`crate::StrategyRef`].
//!
//! **Adding a new Rust-native strategy:** Rust has no runtime
//! reflection, so every selectable Rust-native strategy struct must be
//! manually registered in [`RUST_NATIVE_STRATEGIES`] below -- add a
//! `("your-strategy-name", || Box::new(YourStrategy::new()) as Box<dyn
//! Strategy>)` entry to that table. As of this writing, no
//! production-usable (non-test) Rust-native `Strategy` implementation
//! exists anywhere in the workspace -- every `impl Strategy for ..`
//! outside `DriftScriptStrategy` lives in `#[cfg(test)]` modules or
//! `tests/` files. `RUST_NATIVE_STRATEGIES` therefore legitimately
//! starts EMPTY; this is the correct, documented starting state, not a
//! gap to fill with a placeholder.
//!
//! This module's `list_available_strategies` is also used directly by
//! `apexes-ui` to populate its launcher dropdown WITHOUT going through
//! any running `apexes-engine` process -- see design spec §6.3's note
//! on why `GET /session/strategies` ended up not needing a server at
//! all: `apexes-engine` is "one process per session", so before a
//! session's process exists there is nothing to query over the
//! network; `apexes-ui` answers this purely locally instead.

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use apexes_strategy::{Strategy, StrategyError, drift_dsl::DriftScriptStrategy};

use crate::StrategyRef;

/// Errors that can occur loading a strategy via [`load_strategy`].
#[derive(Debug, thiserror::Error)]
pub enum StrategyLoadError {
    #[error("failed to load Drift Script strategy: {0}")]
    DriftScript(#[from] StrategyError),

    #[error(
        "unknown Rust-native strategy name: {name} (not present in RUST_NATIVE_STRATEGIES -- \
         see strategy_loader.rs's module doc comment for how to register one)"
    )]
    UnknownRustNative { name: String },
}

/// Constructs one Rust-native `Strategy` implementation, for
/// [`RUST_NATIVE_STRATEGIES`]'s table.
type StrategyConstructor = fn() -> Box<dyn Strategy>;

/// The compile-time-fixed table of Rust-native strategies selectable by
/// name. See this module's doc comment for how to add an entry. Starts
/// empty (see doc comment) -- this is the correct, documented starting
/// state.
pub static RUST_NATIVE_STRATEGIES: LazyLock<HashMap<&'static str, StrategyConstructor>> =
    LazyLock::new(HashMap::new);

/// Scans `drift_script_dir` for `*.drift` files and combines them with
/// [`RUST_NATIVE_STRATEGIES`]'s registered names into a single list of
/// selectable [`StrategyRef`]s, for a UI dropdown.
///
/// If `drift_script_dir` does not exist or cannot be read, the
/// Drift-Script half of the list is simply empty (logged at
/// `tracing::warn` level) -- this is not treated as a fatal error,
/// since `RUST_NATIVE_STRATEGIES`'s names are still validly listable
/// even with no script directory configured.
pub fn list_available_strategies(drift_script_dir: &Path) -> Vec<StrategyRef> {
    let mut refs: Vec<StrategyRef> = match std::fs::read_dir(drift_script_dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("drift"))
            .map(|path| StrategyRef::DriftScript { path })
            .collect(),
        Err(err) => {
            tracing::warn!(
                dir = %drift_script_dir.display(),
                error = %err,
                "failed to scan drift_script_dir for *.drift files; listing Rust-native strategies only"
            );
            Vec::new()
        }
    };

    refs.extend(
        RUST_NATIVE_STRATEGIES
            .keys()
            .map(|name| StrategyRef::RustNative {
                name: name.to_string(),
            }),
    );

    refs
}

/// Loads a `Box<dyn Strategy>` from `strategy_ref`:
/// - `StrategyRef::DriftScript { path }`: delegates to
///   `DriftScriptStrategy::from_file(path)`, which fails closed (clear
///   `StrategyError`) if `path` does not exist, fails to parse, or has
///   no top-level `on_market_event` function.
/// - `StrategyRef::RustNative { name }`: looks `name` up in
///   [`RUST_NATIVE_STRATEGIES`], failing with a clear
///   `StrategyLoadError::UnknownRustNative` if `name` is not
///   registered.
pub fn load_strategy(strategy_ref: &StrategyRef) -> Result<Box<dyn Strategy>, StrategyLoadError> {
    match strategy_ref {
        StrategyRef::DriftScript { path } => {
            let strategy = DriftScriptStrategy::from_file(path)?;
            Ok(Box::new(strategy))
        }
        StrategyRef::RustNative { name } => RUST_NATIVE_STRATEGIES
            .get(name.as_str())
            .map(|constructor| constructor())
            .ok_or_else(|| StrategyLoadError::UnknownRustNative { name: name.clone() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "apexes-strategy-loader-test-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&path).expect("failed to create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn list_available_strategies_finds_drift_files_and_rust_native_names() {
        let dir = TempDir::new();
        std::fs::write(dir.path.join("strategy_a.drift"), "// fixture a").unwrap();
        std::fs::write(dir.path.join("strategy_b.drift"), "// fixture b").unwrap();
        std::fs::write(dir.path.join("not_a_strategy.txt"), "ignored").unwrap();

        let refs = list_available_strategies(&dir.path);

        let drift_paths: Vec<PathBuf> = refs
            .iter()
            .filter_map(|r| match r {
                StrategyRef::DriftScript { path } => Some(path.clone()),
                StrategyRef::RustNative { .. } => None,
            })
            .collect();
        assert_eq!(drift_paths.len(), 2);
        assert!(drift_paths.contains(&dir.path.join("strategy_a.drift")));
        assert!(drift_paths.contains(&dir.path.join("strategy_b.drift")));

        let rust_native_names: Vec<String> = refs
            .iter()
            .filter_map(|r| match r {
                StrategyRef::RustNative { name } => Some(name.clone()),
                StrategyRef::DriftScript { .. } => None,
            })
            .collect();
        // RUST_NATIVE_STRATEGIES legitimately starts empty (see module
        // doc comment) -- assert this explicitly so a future addition
        // to the table is a deliberate, visible change to this test.
        assert_eq!(rust_native_names, Vec::<String>::new());
    }

    #[test]
    fn list_available_strategies_tolerates_missing_directory() {
        let missing = std::env::temp_dir().join(format!(
            "apexes-strategy-loader-nonexistent-{}",
            uuid::Uuid::new_v4()
        ));
        let refs = list_available_strategies(&missing);
        assert!(
            refs.iter()
                .all(|r| matches!(r, StrategyRef::RustNative { .. }))
        );
    }

    #[test]
    fn load_strategy_drift_script_succeeds_for_real_fixture() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let strategy_ref = StrategyRef::DriftScript {
            path: fixture_path("always_buy.drift"),
        };
        let result = load_strategy(&strategy_ref);
        assert!(result.is_ok(), "expected Ok, got an Err");
    }

    #[test]
    fn load_strategy_drift_script_fails_clearly_for_nonexistent_path() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _guard = rt.enter();

        let strategy_ref = StrategyRef::DriftScript {
            path: fixture_path("does_not_exist.drift"),
        };
        let err = match load_strategy(&strategy_ref) {
            Ok(_) => panic!("expected Err for a nonexistent Drift Script path"),
            Err(err) => err,
        };
        assert!(matches!(err, StrategyLoadError::DriftScript(_)));
        assert!(err.to_string().contains("failed to load Drift Script"));
    }

    #[test]
    fn load_strategy_rust_native_fails_clearly_for_unregistered_name() {
        let strategy_ref = StrategyRef::RustNative {
            name: "no-such-strategy".to_string(),
        };
        let err = match load_strategy(&strategy_ref) {
            Ok(_) => panic!("expected Err for an unregistered Rust-native name"),
            Err(err) => err,
        };
        assert!(matches!(err, StrategyLoadError::UnknownRustNative { .. }));
        assert!(err.to_string().contains("no-such-strategy"));
    }
}
