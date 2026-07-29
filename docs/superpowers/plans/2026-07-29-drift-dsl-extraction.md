# Drift-DSL Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract `crates/drift-script` and `crates/drift-script-stdlib` out of the `drift`
monorepo into a new standalone repository, `github.com/LeafAI/drift-dsl`, then refactor
`drift`'s consumer crates to depend on it externally via git, so both `drift` and the future
`apexes-strategy` sub-project can share the same embedded scripting language.

**Architecture:** A small two-member Cargo workspace in `drift-dsl` (`drift-script` +
`drift-script-stdlib`, unchanged package names, unchanged source, unchanged tests) replaces
the same two directories inside `drift`'s `crates/*` workspace. `drift`'s 4 consumer crates
switch their dependency source from `path = "../..."` to `git = "...", rev = "..."`. No
language/API changes — this is a pure repository/dependency-graph relocation.

**Tech Stack:** Rust edition 2024, Cargo workspaces, git (GitHub, `gh` CLI available and
authenticated with `repo`+`workflow` scopes), GitHub Actions for CI.

## Global Constraints

- Rust edition `2024` in both `drift-dsl` and (unchanged) `drift`.
- MIT license, copied verbatim from `drift`'s existing `LICENSE` file (Copyright (c) 2026
  Drift Contributors) into `drift-dsl`'s `LICENSE`.
- Every commit in EVERY repo touched (`drift-dsl`, `drift`) uses `git commit -S -s` (signed +
  signed-off) — confirmed this is the user's global git config (SSH-based signing, already
  used for all of `drift`'s existing commits, not an apexes-specific convention), so it
  applies automatically to any repo, including the brand-new `drift-dsl`.
- Do NOT bump any dependency version during this extraction. Use the EXACT version numbers
  already pinned in `drift`'s root `Cargo.toml` `[workspace.dependencies]`: `oxc_parser =
  "0.26"`, `oxc_ast = "0.26"`, `oxc_span = "0.26"`, `serde = "1"` (with `features =
  ["derive"]` where used), `thiserror = "1"`, `tracing = "0.1"`, `tokio = "1"` (with
  `features = ["full"]` where used), `grep-regex = "0.1"`, `grep-searcher = "0.1"`.
  `oxc_allocator` is not a workspace dependency in `drift` (each crate pins it directly) —
  use `oxc_allocator = "0.26"` to match the version resolved in `drift`'s own `Cargo.lock`
  (confirmed: `oxc_allocator 0.26.0`). Non-workspace, crate-pinned dependencies (`futures =
  "0.3"` in `drift-script`; `reqwest = "0.12"`, `serde_json = "1"`, `glob = "0.3"`, `ignore =
  "0.4"`, `regex = "1"`, `tempfile = "3"`, `wiremock = "0.6"` in `drift-script-stdlib`) keep
  their exact current version strings unchanged too.
- Do NOT touch `drift`'s pre-existing unrelated uncommitted local changes: modified
  `Cargo.toml`, `Cargo.lock`, `USAGE.md`, `crates/drift-tools/Cargo.toml`,
  `crates/drift-tools/src/lib.rs`, `crates/drift-tools/src/tools/web.rs`,
  `crates/drift/src/main.rs`, and the new untracked file
  `docs/superpowers/specs/2026-07-29-web-crawl-tool-design.md`. If any task's required edit
  would touch one of these already-modified files (e.g. `drift`'s root `Cargo.toml` is in
  this list AND needs editing for this extraction), apply ONLY the minimal lines needed for
  the extraction and leave every other existing uncommitted change in that file exactly as
  found — do not revert, stash, or discard any of it.
- `drift`'s workspace `members` is the glob `"crates/*"` — deleting a crate's directory
  automatically removes it from the workspace with no `Cargo.toml` members-list edit needed.
- Behavioral-identity requirement: every existing test in both crates must pass, unchanged,
  after the move. This is a relocation, not a rewrite. Do not "improve" anything in the copied
  source during this extraction (no refactoring, no clippy-driven rewrites beyond what's
  needed to pass `-D warnings` in the new standalone context if a lint only fires due to the
  loss of workspace-level lint config — investigate any such case before changing code).
- Design spec of record:
  `docs/superpowers/specs/2026-07-29-drift-dsl-extraction-design.md`. If you find a mismatch
  between this plan and that spec while implementing, the spec wins — stop and flag it rather
  than improvising.

---

### Task 1: Create the `drift-dsl` repository skeleton and copy `drift-script`

**Files (in `~/src/oss/drift-dsl`, a NEW local clone of the empty `LeafAI/drift-dsl` repo):**
- Create: `Cargo.toml` (workspace root)
- Create: `LICENSE`
- Create: `README.md`
- Create: `.gitignore`
- Create: `crates/drift-script/` (copied from `~/src/oss/drift/crates/drift-script/`, all 12
  source files + adjusted `Cargo.toml`)

**Interfaces:**
- Consumes: `~/src/oss/drift/crates/drift-script/*` (read-only source, the copy source).
- Produces: a working, standalone `drift-script` crate inside the new `drift-dsl` workspace
  that `cargo build`/`cargo test`/`cargo clippy -- -D warnings`/`cargo fmt --check` all pass
  on, with zero behavioral change from the original.

- [ ] **Step 1: Clone the empty `drift-dsl` repo locally**

Run:
```bash
cd ~/src/oss
git clone https://github.com/LeafAI/drift-dsl.git
cd drift-dsl
git status
```
Expected: an empty repository (no commits yet, or possibly an auto-generated default branch
with no files — check `git log` and `ls -la` to confirm the actual empty state before
proceeding).

- [ ] **Step 2: Rename the default branch to `master`**

Run:
```bash
cd ~/src/oss/drift-dsl
git checkout -b master 2>/dev/null || git branch -m master
gh repo edit LeafAI/drift-dsl --default-branch master
```
If the repo has no commits yet, you may need to make an initial commit first (e.g. a bare
`README.md` placeholder) before `gh repo edit --default-branch` can target a real branch --
if so, create a minimal placeholder commit first, push it, run the rename, then continue with
the real content in Step 3 onward (this placeholder commit is fine to have in history, it's
not something to avoid).

- [ ] **Step 3: Create the workspace root `Cargo.toml`**

Create `~/src/oss/drift-dsl/Cargo.toml`:

```toml
[workspace]
members = ["crates/*"]
resolver = "3"

[workspace.package]
edition = "2024"
license = "MIT"

[workspace.dependencies]
oxc_allocator = "0.26"
oxc_parser = "0.26"
oxc_ast = "0.26"
oxc_span = "0.26"
serde = { version = "1", features = ["derive"] }
thiserror = "1"
tracing = "0.1"
tokio = { version = "1", features = ["full"] }
grep-regex = "0.1"
grep-searcher = "0.1"
```

- [ ] **Step 4: Create `LICENSE`**

Create `~/src/oss/drift-dsl/LICENSE` with the exact content of `~/src/oss/drift/LICENSE`
(copy it verbatim -- read the source file first, then write the identical content; do not
alter the copyright line).

- [ ] **Step 5: Create `README.md`**

Create `~/src/oss/drift-dsl/README.md`:

```markdown
# drift-dsl

An embedded, JavaScript-like scripting language for Rust host applications, with a clean
`BindingProvider` trait for registering native Rust functions into the language's global
scope.

Extracted from the [Drift](https://github.com/LeafAI/drift) AI agent framework, where it
still powers workflow scripting, tool-call automation, and MCP/ACP integrations via
`drift-script-ai` (kept in the `drift` monorepo). This repository hosts the reusable core:

- **`drift-script`** — parser, HIR, bytecode compiler, VM, tree-walking interpreter,
  `Environment`/`BindingProvider`/`BindingRegistry`, `DriftValue`/`NativeFn`,
  `Module`/`ModuleLoader`, `Promise`.
- **`drift-script-stdlib`** — general-purpose language bindings: filesystem, shell, HTTP,
  JSON, logging, console, OS, process, string, object utilities.

## License

MIT
```

- [ ] **Step 6: Create `.gitignore`**

`drift`'s own `.gitignore` (already checked during planning) does NOT ignore `Cargo.lock` --
it's committed at `drift`'s workspace root. Follow the same convention here. Create
`~/src/oss/drift-dsl/.gitignore`:

```
/target/
.DS_Store
```

Do NOT add `Cargo.lock` to this file -- it will be committed (see Step 9, which explicitly
includes it via `git add .`).

- [ ] **Step 7: Copy `drift-script`'s source files verbatim**

Copy every file from `~/src/oss/drift/crates/drift-script/src/` (12 files: `chunk.rs`,
`compiler.rs`, `environment.rs`, `error.rs`, `hir.rs`, `interpreter.rs`, `lib.rs`, `module.rs`,
`parser.rs`, `promise.rs`, `value.rs`, `vm.rs`) into
`~/src/oss/drift-dsl/crates/drift-script/src/`, byte-for-byte identical (use a file copy, not
a manual retype, to guarantee this). Also copy any `tests/` directory if
`~/src/oss/drift/crates/drift-script/` has one at the crate root (check first).

Create `~/src/oss/drift-dsl/crates/drift-script/Cargo.toml`:

```toml
[package]
name = "drift-script"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
oxc_allocator = { workspace = true }
oxc_parser = { workspace = true }
oxc_ast = { workspace = true }
oxc_span = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
futures = "0.3"
tracing = { workspace = true }

[dev-dependencies]
serde_json = "1"
tempfile = "3"
```

(This mirrors `~/src/oss/drift/crates/drift-script/Cargo.toml`'s dependency list exactly --
re-read that file directly before finalizing this one, to make sure no dependency was missed
or a version mismatched; the block above is expected to match but treat the real source file
as authoritative if there's any discrepancy.)

- [ ] **Step 8: Verify `drift-script` builds and tests standalone**

Run:
```bash
cd ~/src/oss/drift-dsl
cargo build -p drift-script
cargo test -p drift-script
cargo clippy -p drift-script -- -D warnings
cargo fmt -p drift-script -- --check
```
Expected: all succeed. Report the exact test count you observe (compare mentally against
what `cargo test -p drift-script` reports when run inside the ORIGINAL `~/src/oss/drift`
repo, to confirm identical test count -- run that comparison command too and report both
numbers).

- [ ] **Step 9: Commit and push**

```bash
cd ~/src/oss/drift-dsl
git add .
git commit -S -s -m "feat: extract drift-script from the drift monorepo"
git push -u origin master
```

---

### Task 2: Copy `drift-script-stdlib` into the new workspace

**Files (in `~/src/oss/drift-dsl`):**
- Create: `crates/drift-script-stdlib/` (copied from
  `~/src/oss/drift/crates/drift-script-stdlib/`, all 11 source files + adjusted `Cargo.toml`)

**Interfaces:**
- Consumes: `~/src/oss/drift/crates/drift-script-stdlib/*` (read-only source), `drift-script`
  (Task 1, now available as a workspace-sibling path dependency within `drift-dsl`).
- Produces: a working, standalone `drift-script-stdlib` crate that `cargo build`/`cargo
  test`/`cargo clippy -- -D warnings`/`cargo fmt --check` all pass on.

- [ ] **Step 1: Copy `drift-script-stdlib`'s source files verbatim**

Copy every file from `~/src/oss/drift/crates/drift-script-stdlib/src/` (11 files:
`console.rs`, `fs.rs`, `http.rs`, `json.rs`, `lib.rs`, `log.rs`, `object.rs`, `os.rs`,
`process.rs`, `shell.rs`, `string.rs`) into
`~/src/oss/drift-dsl/crates/drift-script-stdlib/src/`, byte-for-byte identical.

Create `~/src/oss/drift-dsl/crates/drift-script-stdlib/Cargo.toml`:

```toml
[package]
name = "drift-script-stdlib"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
drift-script = { path = "../drift-script" }
tokio = { workspace = true }
reqwest = { version = "0.12", features = ["json"] }
serde_json = "1"
glob = "0.3"
ignore = "0.4"
grep-regex = { workspace = true }
grep-searcher = { workspace = true }
regex = "1"
tracing = { workspace = true }

[dev-dependencies]
tempfile = "3"
wiremock = "0.6"
```

(Re-read `~/src/oss/drift/crates/drift-script-stdlib/Cargo.toml` directly before finalizing
this one to confirm every dependency/version matches exactly; the block above is expected to
match but treat the real source file as authoritative.)

- [ ] **Step 2: Verify `drift-script-stdlib` builds and tests standalone**

Run:
```bash
cd ~/src/oss/drift-dsl
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all succeed. Report the exact total test count across BOTH crates in the new
workspace, and separately run `cargo test -p drift-script-stdlib` inside the ORIGINAL
`~/src/oss/drift` repo to confirm the test count matches (report both numbers).

- [ ] **Step 3: Commit and push**

```bash
cd ~/src/oss/drift-dsl
git add .
git commit -S -s -m "feat: extract drift-script-stdlib from the drift monorepo"
git push origin master
```

- [ ] **Step 4: Add a GitHub Actions CI workflow**

Create `~/src/oss/drift-dsl/.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo build --workspace --verbose
      - run: cargo test --workspace --verbose
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo fmt --check
```

Commit and push:
```bash
cd ~/src/oss/drift-dsl
git add .github/workflows/ci.yml
git commit -S -s -m "ci: add GitHub Actions workflow for build/test/clippy/fmt"
git push origin master
```

Do NOT record a commit SHA yet for consumer pinning -- Task 3 adds one more module before
this repo's first "stable" commit that `drift`'s consumers should pin to.

---

### Task 3: Add a `math` binding module to `drift-script-stdlib`

**Files (in `~/src/oss/drift-dsl`):**
- Create: `crates/drift-script-stdlib/src/math.rs`
- Modify: `crates/drift-script-stdlib/src/lib.rs`

**Interfaces:**
- Consumes: `drift_script::{DriftValue, NativeFn}` (from Task 1), the crate-private
  `error_object`/`require_number` helper pattern already used by sibling modules (e.g.
  `string.rs`'s `require_usize`).
- Produces: a new `math` module registered into `BindingRegistry`, exposing `math.abs`,
  `math.min`, `math.max`, `math.round`, `math.floor`, `math.ceil`, `math.trunc`, `math.pow`,
  `math.sqrt`, `math.log`, `math.exp`, `math.clamp` (functions) and `math.PI`, `math.E`
  (constants) to any Drift Script using this stdlib.

- [ ] **Step 1: Write `math.rs`**

Create `~/src/oss/drift-dsl/crates/drift-script-stdlib/src/math.rs`:

```rust
//! Math module bindings for Drift Script.
//!
//! Provides general-purpose numeric helpers not already covered by the
//! language's own arithmetic operators: `abs`, `min`, `max`, `round`,
//! `floor`, `ceil`, `trunc`, `pow`, `sqrt`, `log` (natural log), `exp`,
//! `clamp`. Constants `PI`/`E` are exposed separately (see
//! `math_constants()`), not as zero-argument functions, since Drift
//! Script has no distinct "constant" binding kind beyond a plain value.
//!
//! Deliberately minimal: no statistics, no domain-specific (e.g.
//! trading/financial) functions -- those belong in a downstream binding
//! built on top of this crate.

use std::sync::Arc;

use drift_script::{DriftValue, NativeFn};

use crate::error_object;

pub fn all_bindings() -> Vec<(&'static str, NativeFn)> {
    vec![
        ("abs", native_abs()),
        ("min", native_min()),
        ("max", native_max()),
        ("round", native_round()),
        ("floor", native_floor()),
        ("ceil", native_ceil()),
        ("trunc", native_trunc()),
        ("pow", native_pow()),
        ("sqrt", native_sqrt()),
        ("log", native_log()),
        ("exp", native_exp()),
        ("clamp", native_clamp()),
    ]
}

/// `math.PI` / `math.E` constants, registered separately from the function
/// bindings above (see `lib.rs`'s `register_math`) since they are plain
/// `DriftValue::Number` values, not `NativeFunction`s.
pub fn constants() -> Vec<(&'static str, f64)> {
    vec![("PI", std::f64::consts::PI), ("E", std::f64::consts::E)]
}

fn require_number(args: &[DriftValue], index: usize, name: &str) -> Result<f64, DriftValue> {
    match args.get(index) {
        Some(DriftValue::Number(n)) => Ok(*n),
        Some(other) => Err(error_object(&format!(
            "{}: expected number argument at position {}, got {}",
            name,
            index,
            other.type_name()
        ))),
        None => Err(error_object(&format!("{}: missing argument at position {}", name, index))),
    }
}

fn native_abs() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.abs") {
                Ok(n) => DriftValue::Number(n.abs()),
                Err(e) => e,
            }
        })
    })
}

fn native_min() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let a = match require_number(&args, 0, "math.min") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let b = match require_number(&args, 1, "math.min") {
                Ok(n) => n,
                Err(e) => return e,
            };
            DriftValue::Number(a.min(b))
        })
    })
}

fn native_max() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let a = match require_number(&args, 0, "math.max") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let b = match require_number(&args, 1, "math.max") {
                Ok(n) => n,
                Err(e) => return e,
            };
            DriftValue::Number(a.max(b))
        })
    })
}

fn native_round() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.round") {
                Ok(n) => DriftValue::Number(n.round()),
                Err(e) => e,
            }
        })
    })
}

fn native_floor() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.floor") {
                Ok(n) => DriftValue::Number(n.floor()),
                Err(e) => e,
            }
        })
    })
}

fn native_ceil() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.ceil") {
                Ok(n) => DriftValue::Number(n.ceil()),
                Err(e) => e,
            }
        })
    })
}

fn native_trunc() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.trunc") {
                Ok(n) => DriftValue::Number(n.trunc()),
                Err(e) => e,
            }
        })
    })
}

fn native_pow() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let base = match require_number(&args, 0, "math.pow") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let exponent = match require_number(&args, 1, "math.pow") {
                Ok(n) => n,
                Err(e) => return e,
            };
            DriftValue::Number(base.powf(exponent))
        })
    })
}

fn native_sqrt() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.sqrt") {
                Ok(n) if n < 0.0 => error_object("math.sqrt: argument must be non-negative"),
                Ok(n) => DriftValue::Number(n.sqrt()),
                Err(e) => e,
            }
        })
    })
}

fn native_log() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.log") {
                Ok(n) if n <= 0.0 => error_object("math.log: argument must be positive"),
                Ok(n) => DriftValue::Number(n.ln()),
                Err(e) => e,
            }
        })
    })
}

fn native_exp() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            match require_number(&args, 0, "math.exp") {
                Ok(n) => DriftValue::Number(n.exp()),
                Err(e) => e,
            }
        })
    })
}

fn native_clamp() -> NativeFn {
    Arc::new(|args| {
        Box::pin(async move {
            let value = match require_number(&args, 0, "math.clamp") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let min = match require_number(&args, 1, "math.clamp") {
                Ok(n) => n,
                Err(e) => return e,
            };
            let max = match require_number(&args, 2, "math.clamp") {
                Ok(n) => n,
                Err(e) => return e,
            };
            if min > max {
                return error_object("math.clamp: min must be <= max");
            }
            DriftValue::Number(value.clamp(min, max))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn call(f: &NativeFn, args: Vec<DriftValue>) -> DriftValue {
        f(args).await
    }

    #[tokio::test]
    async fn abs_returns_absolute_value() {
        let result = call(&native_abs(), vec![DriftValue::Number(-5.0)]).await;
        assert_eq!(result, DriftValue::Number(5.0));
    }

    #[tokio::test]
    async fn min_returns_smaller_value() {
        let result =
            call(&native_min(), vec![DriftValue::Number(3.0), DriftValue::Number(7.0)]).await;
        assert_eq!(result, DriftValue::Number(3.0));
    }

    #[tokio::test]
    async fn max_returns_larger_value() {
        let result =
            call(&native_max(), vec![DriftValue::Number(3.0), DriftValue::Number(7.0)]).await;
        assert_eq!(result, DriftValue::Number(7.0));
    }

    #[tokio::test]
    async fn round_rounds_to_nearest_integer() {
        let result = call(&native_round(), vec![DriftValue::Number(2.6)]).await;
        assert_eq!(result, DriftValue::Number(3.0));
    }

    #[tokio::test]
    async fn floor_rounds_down() {
        let result = call(&native_floor(), vec![DriftValue::Number(2.9)]).await;
        assert_eq!(result, DriftValue::Number(2.0));
    }

    #[tokio::test]
    async fn ceil_rounds_up() {
        let result = call(&native_ceil(), vec![DriftValue::Number(2.1)]).await;
        assert_eq!(result, DriftValue::Number(3.0));
    }

    #[tokio::test]
    async fn trunc_drops_fractional_part() {
        let result = call(&native_trunc(), vec![DriftValue::Number(-2.9)]).await;
        assert_eq!(result, DriftValue::Number(-2.0));
    }

    #[tokio::test]
    async fn pow_raises_to_exponent() {
        let result =
            call(&native_pow(), vec![DriftValue::Number(2.0), DriftValue::Number(10.0)]).await;
        assert_eq!(result, DriftValue::Number(1024.0));
    }

    #[tokio::test]
    async fn sqrt_of_negative_number_returns_error_object() {
        let result = call(&native_sqrt(), vec![DriftValue::Number(-1.0)]).await;
        match result {
            DriftValue::Object(map) => assert!(map.contains_key("error")),
            other => panic!("expected an error object, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sqrt_of_positive_number_returns_root() {
        let result = call(&native_sqrt(), vec![DriftValue::Number(16.0)]).await;
        assert_eq!(result, DriftValue::Number(4.0));
    }

    #[tokio::test]
    async fn log_of_non_positive_number_returns_error_object() {
        let result = call(&native_log(), vec![DriftValue::Number(0.0)]).await;
        match result {
            DriftValue::Object(map) => assert!(map.contains_key("error")),
            other => panic!("expected an error object, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn exp_of_zero_is_one() {
        let result = call(&native_exp(), vec![DriftValue::Number(0.0)]).await;
        assert_eq!(result, DriftValue::Number(1.0));
    }

    #[tokio::test]
    async fn clamp_restricts_value_to_range() {
        let below =
            call(&native_clamp(), vec![DriftValue::Number(-5.0), DriftValue::Number(0.0), DriftValue::Number(10.0)])
                .await;
        assert_eq!(below, DriftValue::Number(0.0));

        let above =
            call(&native_clamp(), vec![DriftValue::Number(15.0), DriftValue::Number(0.0), DriftValue::Number(10.0)])
                .await;
        assert_eq!(above, DriftValue::Number(10.0));

        let within =
            call(&native_clamp(), vec![DriftValue::Number(5.0), DriftValue::Number(0.0), DriftValue::Number(10.0)])
                .await;
        assert_eq!(within, DriftValue::Number(5.0));
    }

    #[tokio::test]
    async fn clamp_rejects_inverted_range() {
        let result =
            call(&native_clamp(), vec![DriftValue::Number(5.0), DriftValue::Number(10.0), DriftValue::Number(0.0)])
                .await;
        match result {
            DriftValue::Object(map) => assert!(map.contains_key("error")),
            other => panic!("expected an error object, got {other:?}"),
        }
    }
}
```

Before finalizing, check `drift_script::DriftValue`'s actual `Debug`/`PartialEq` derives
(read `~/src/oss/drift-dsl/crates/drift-script/src/value.rs`, copied in Task 1) to confirm
`assert_eq!(result, DriftValue::Number(...))` and `{other:?}` formatting both compile as
written above -- `value.rs`'s `DriftValue` enum derives (or manually implements) `Debug` and
has a custom `PartialEq` impl per the file's own `impl PartialEq for DriftValue` block (seen
during planning); if the real derives differ from this assumption, adjust the test assertions
to use whatever comparison mechanism the type actually supports (e.g. pattern-matching
`DriftValue::Number(n)` and asserting on `n` directly if `PartialEq`/`Debug` aren't derived
the way assumed here).

- [ ] **Step 2: Register the `math` module in `lib.rs`**

Modify `~/src/oss/drift-dsl/crates/drift-script-stdlib/src/lib.rs`. Add `pub mod math;` to the
module declarations at the top (alongside the existing `pub mod console;`, `pub mod fs;`,
etc.).

Find:
```rust
        registry.register_fs();
        registry.register_shell();
        registry.register_http();
        registry.register_json();
        registry.register_log();
        registry.register_module("os", os_bindings);
        registry.register_process();
        registry.register_object();
        registry.register_string();
        registry.register_console();
        registry
    }
```
Replace with (adds `register_math()` to the chain):
```rust
        registry.register_fs();
        registry.register_shell();
        registry.register_http();
        registry.register_json();
        registry.register_log();
        registry.register_module("os", os_bindings);
        registry.register_process();
        registry.register_object();
        registry.register_string();
        registry.register_console();
        registry.register_math();
        registry
    }
```

Add a new `register_math` method to the `impl BindingRegistry` block (alongside
`register_string`, `register_process`, etc.):
```rust
    fn register_math(&mut self) {
        self.register_module("math", math::all_bindings());
        for (name, value) in math::constants() {
            self.globals.insert(format!("math.{name}"), DriftValue::Number(value));
        }
    }
```

Wait -- inserting flat `"math.PI"`/`"math.E"` keys into `self.globals` would make them
top-level globals named literally `math.PI` (an identifier Drift Script's parser cannot
reference as `math.PI`, a property access, since `self.globals` populates top-level bindings,
not nested object properties). Instead, the constants must be merged INTO the same `math`
object that `register_module("math", ...)` creates, so `math.PI` works as a real property
access on the `math` object at the script level. Fix `register_math` to build the module map
manually instead of relying on `register_module`'s function-only signature:

```rust
    fn register_math(&mut self) {
        let mut math_map: HashMap<String, NativeFn> =
            math::all_bindings().into_iter().map(|(name, f)| (name.to_string(), f)).collect();
        // Constants (PI, E) are plain numbers, not NativeFunctions, so they
        // can't go through `math_map` (which is NativeFn-only) or through
        // `register_module` (function-only) -- register the whole `math`
        // object as a single combined DriftValue::Object global instead,
        // mixing NativeFunction and Number entries directly.
        let mut combined = HashMap::new();
        for (name, f) in math_map.drain() {
            combined.insert(name, DriftValue::NativeFunction(f));
        }
        for (name, value) in math::constants() {
            combined.insert(name.to_string(), DriftValue::Number(value));
        }
        self.register_global("math", DriftValue::Object(combined));
    }
```

This mirrors `register_console`'s existing pattern (which also builds a combined object map
manually rather than using `register_module`, for the same reason: `register_module` only
handles `NativeFn` entries, and `console`/`math` both need a plain `DriftValue::Object` with
mixed content). Re-read `register_console`'s exact existing code in `lib.rs` (already shown
above during planning) to confirm this new `register_math` follows the identical structural
pattern before finalizing.

- [ ] **Step 3: Write a script-level integration test**

Add to `~/src/oss/drift-dsl/crates/drift-script-stdlib/src/lib.rs`'s existing test module (if
one exists at the crate root -- check first; if not, create `#[cfg(test)] mod tests` at the
bottom of `lib.rs`) a test that exercises `math` end-to-end through a real `Interpreter`,
mirroring however the existing crate-level tests (if any) verify other modules end-to-end. If
no such crate-level integration test pattern already exists in `drift-script-stdlib` (i.e.
all existing tests are per-module unit tests calling `NativeFn`s directly, like the ones
written in Step 1), it's acceptable to skip a separate script-level integration test for
`math` and rely on `math.rs`'s own unit tests (Step 1) as sufficient coverage -- match
whatever the codebase's existing convention actually is, don't invent a new testing pattern
for just this one module.

- [ ] **Step 4: Run tests**

Run:
```bash
cd ~/src/oss/drift-dsl
cargo test -p drift-script-stdlib math
```
Expected: all `math`-module tests pass (count depends on exactly how many you wrote in Step
1 -- report the real number).

- [ ] **Step 5: Run clippy and fmt**

```bash
cd ~/src/oss/drift-dsl
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: no warnings, no formatting diffs.

- [ ] **Step 6: Commit and push**

```bash
cd ~/src/oss/drift-dsl
git add crates/drift-script-stdlib/src/math.rs crates/drift-script-stdlib/src/lib.rs
git commit -S -s -m "feat: add math binding module to drift-script-stdlib"
git push origin master
```

Record the commit SHA of this push (`git rev-parse HEAD`) -- this is the `rev` value Task 4
will pin `drift`'s consumer crates to.

---

### Task 4: Refactor `drift-lsp` and `drift-script-ai` to depend on `drift-dsl` externally

**Files (in `~/src/oss/drift`):**
- Modify: `crates/drift-lsp/Cargo.toml`
- Modify: `crates/drift-script-ai/Cargo.toml`

**Interfaces:**
- Consumes: the `drift-dsl` repo pushed in Task 2 (specifically the commit SHA recorded at
  the end of Task 2's Step 4).
- Produces: `drift-lsp` and `drift-script-ai` building successfully against the external
  `drift-script` crate, with zero source-code changes (only the dependency source line
  changes).

- [ ] **Step 1: Update `crates/drift-lsp/Cargo.toml`**

Find the line:
```toml
drift-script = { path = "../drift-script" }
```
Replace with (using the ACTUAL commit SHA recorded at the end of Task 2's Step 4, not a
placeholder):
```toml
drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha>" }
```

- [ ] **Step 2: Update `crates/drift-script-ai/Cargo.toml`**

Apply the identical change: find `drift-script = { path = "../drift-script" }`, replace with
the same `git = "...", rev = "<commit-sha>"` line (same SHA as Step 1 -- both crates must
pin the exact same `drift-dsl` commit).

- [ ] **Step 3: Verify these two crates still build**

Run (from `~/src/oss/drift`):
```bash
cargo build -p drift-lsp -p drift-script-ai
```
Expected: succeeds. This will NOT yet build the whole workspace cleanly, since
`crates/drift-script` and `crates/drift-script-stdlib` still exist locally and other crates
(`drift-tools`, `drift`) still reference them by local path -- that's expected at this point
in the plan; do not attempt to fix that here, it's Task 5's job. If `cargo build -p drift-lsp
-p drift-script-ai` itself fails for a reason unrelated to the other still-local-path crates,
investigate and report the exact error.

- [ ] **Step 4: Run each crate's own tests**

Run:
```bash
cargo test -p drift-lsp
cargo test -p drift-script-ai
```
Expected: both pass, with identical results to before this change (this is a pure
dependency-source change, not a logic change -- any test failure here must be investigated,
not worked around).

- [ ] **Step 5: Commit (do NOT push yet -- wait until Task 5 completes so `drift`'s workspace is in a consistently buildable state for each pushed commit)**

```bash
cd ~/src/oss/drift
git add crates/drift-lsp/Cargo.toml crates/drift-script-ai/Cargo.toml
git commit -S -s -m "chore: point drift-lsp and drift-script-ai at the external drift-dsl repo"
```

Do NOT run `git push` yet. Do NOT touch `Cargo.lock` in this commit if it's part of the
pre-existing uncommitted changes list (per Global Constraints) -- if `cargo build` modifies
`Cargo.lock` as a side effect of this dependency change, that IS an expected, necessary part
of THIS commit (the lockfile must reflect the new git dependency), so include it in `git add`
for just this commit's own changes -- but do not otherwise touch any of the other
pre-existing uncommitted modifications to `Cargo.lock` beyond what's mechanically required by
this dependency-source change. If you're unsure whether a `Cargo.lock` diff is "required by
this change" vs. "part of the pre-existing unrelated modification", stop and report the exact
diff you're seeing rather than guessing.

---

### Task 5: Refactor `drift-tools` and `drift` (CLI) to depend on `drift-dsl` externally, delete the old local crates

**Files (in `~/src/oss/drift`):**
- Modify: `crates/drift-tools/Cargo.toml`
- Modify: `crates/drift/Cargo.toml`
- Delete: `crates/drift-script/` (entire directory)
- Delete: `crates/drift-script-stdlib/` (entire directory)

**Interfaces:**
- Consumes: the same `drift-dsl` commit SHA from Task 2/3.
- Produces: `drift`'s entire workspace building and testing cleanly with `drift-script`/
  `drift-script-stdlib` no longer present locally at all.

- [ ] **Step 1: Update `crates/drift-tools/Cargo.toml`**

Find both lines:
```toml
drift-script = { path = "../drift-script" }
drift-script-stdlib = { path = "../drift-script-stdlib" }
```
Replace with (same commit SHA as Task 4):
```toml
drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha>" }
drift-script-stdlib = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha>" }
```

- [ ] **Step 2: Update `crates/drift/Cargo.toml`**

Apply the identical change to its `drift-script`/`drift-script-stdlib` lines (same SHA).

- [ ] **Step 3: Delete the old local crate directories**

```bash
cd ~/src/oss/drift
rm -rf crates/drift-script crates/drift-script-stdlib
```

- [ ] **Step 4: Verify the full workspace builds and tests clean**

Run:
```bash
cd ~/src/oss/drift
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all succeed, with results identical to the pre-extraction baseline (report the
exact test counts you observe; if you have a way to compare against the test counts from
BEFORE this whole extraction started, e.g. from git history or your own memory of running
`cargo test --workspace` at the very start of this task, note that comparison -- otherwise
just report the current totals clearly broken down by crate).

- [ ] **Step 5: Commit and push**

```bash
cd ~/src/oss/drift
git add crates/drift-tools/Cargo.toml crates/drift/Cargo.toml
git add -u crates/drift-script crates/drift-script-stdlib
git commit -S -s -m "chore: point drift-tools and drift CLI at the external drift-dsl repo; remove local drift-script(-stdlib)"
git push origin master
```

Note: `git add -u` on the now-deleted directories stages their removal. Double-check with
`git status` before committing that ONLY the expected files are staged (the 2 Cargo.toml
edits + the deletions) and NONE of the pre-existing unrelated uncommitted changes (modified
`USAGE.md`, `drift-tools/src/lib.rs`, `drift-tools/src/tools/web.rs`, `drift/src/main.rs`,
the untracked web-crawl-tool spec doc) are accidentally included in this commit. If `git
status` shows any of those files as staged, unstage them (`git restore --staged <file>`)
before committing -- this commit must contain ONLY the drift-dsl extraction's own changes.

---

### Task 6: Final whole-effort verification

**Files:** None created/modified -- this task only runs verification commands across both
repos and confirms the extraction's end state.

**Interfaces:**
- Consumes: the final state of both `drift-dsl` (Tasks 1-3) and `drift` (Task 5).
- Produces: confirmation that both repos are independently healthy and the extraction is
  complete.

- [ ] **Step 1: Re-verify `drift-dsl` standalone (fresh clone, to rule out any local-state contamination)**

```bash
cd /tmp
rm -rf drift-dsl-verify
git clone https://github.com/LeafAI/drift-dsl.git drift-dsl-verify
cd drift-dsl-verify
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all succeed on a completely fresh clone (proves the repo is self-contained and has
no hidden dependency on the original `drift` monorepo's workspace settings).

- [ ] **Step 2: Re-verify `drift`'s full workspace one more time**

```bash
cd ~/src/oss/drift
git status
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```
Expected: all succeed. `git status` should show ONLY the pre-existing unrelated uncommitted
changes (never touched by this plan) -- confirm this explicitly by listing what `git status`
shows and comparing against the Global Constraints' list of pre-existing files.

- [ ] **Step 3: Confirm no stray local crate references remain**

```bash
cd ~/src/oss/drift
grep -rn "path = \"\.\./drift-script" crates/*/Cargo.toml
```
Expected: no output (zero matches) -- confirms every consumer now uses the external git
dependency, not a stale local path.

- [ ] **Step 4: Report final summary**

Summarize: `drift-dsl` repo URL, default branch, final commit SHA, test counts;
`drift` repo's final commit SHA for this extraction, test counts, confirmation that
pre-existing unrelated uncommitted changes are untouched. No commit needed for this
verification-only task.

---

## Plan Self-Review Notes

**Spec coverage:** §2 (scope: extract `drift-script` + `drift-script-stdlib` plus the new
`math` module, not `drift-script-ai`/`drift-script-signing`) → Tasks 1-2 (the copy), Task 3
(the new `math` module), and Task 5 (deletion from `drift`, confirming nothing else was
extracted); §3 (target repo, branch rename, CI) → Task 1 Steps 1-2, Task 2 Step 4; §4
(migration mechanics: version pinning, consumer table, workspace glob behavior) → Task 1 Step
3/7 (versions), Tasks 4-5 (the 4 consumers exactly per the design spec's table:
`drift-lsp`+`drift-script-ai` in Task 4, `drift-tools`+`drift` in Task 5); §5 (testing
strategy: behavioral identity, fresh-clone verification) → Task 1 Step 8, Task 2 Step 2, Task
6 Step 1; §6 (error handling: fix forward, don't revert to path deps) → called out explicitly
in Global Constraints and Task 4 Step 3's guidance; §7 (non-goals: no apexes work, no
language changes, no touching pre-existing unrelated `drift` changes) → enforced throughout
via Global Constraints and Task 5 Step 5's explicit `git status` check.

**Placeholder scan:** No TBD/TODO/"implement later" strings. The one genuine
runtime-dependent value (`<commit-sha>`) is explicitly flagged at every occurrence as "the
ACTUAL commit SHA recorded at the end of Task 2's Step 4" -- not a placeholder to leave
unfilled, but a real value each task must look up from the prior task's actual output.

**Type consistency:** Crate names (`drift-script`, `drift-script-stdlib`), the git URL
(`https://github.com/LeafAI/drift-dsl.git`), and the consumer-crate list (`drift-lsp`,
`drift-script-ai`, `drift-tools`, `drift`) are used identically across every task that
references them, matching the design spec's dependency table exactly.

**Known risk flagged for implementers:** Task 1 Step 2 (branch rename before any commit
exists) may need adjustment on the fly since GitHub's exact behavior for renaming the default
branch of a truly-empty repo can vary -- the step includes a fallback (push a placeholder
commit first) rather than assuming one exact sequence will work, since this wasn't (and
couldn't be) verified against the live empty repo during planning without actually starting
the work.
