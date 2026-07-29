# Extract `drift-script` into `LeafAI/drift-dsl` — Design Spec

Status: Approved (pending final spec review)
Repos affected: `LeafAI/drift-dsl` (new), `LeafAI/drift` (refactored consumer)
Not part of the `apexes` workspace — this is a prerequisite effort for sub-project #4
(`apexes-strategy`), tracked separately from the Apexes roadmap.

## 1. Motivation

`apexes-strategy` (Apexes sub-project #4) needs a scripting/DSL layer so researchers can author
trading strategies without a full Rust compile cycle, while keeping strategy execution fast
(no Python GIL/interop overhead). The user has already built exactly this kind of engine —
`drift-script`, a JavaScript-like embedded language with a parser, HIR, bytecode compiler, VM,
tree-walking interpreter, and a clean `BindingProvider` trait for registering native Rust
functions into the language's global scope — as part of the `drift` AI-agent-framework
monorepo (`~/src/oss/drift`, `crates/drift-script`).

`drift-script` has **zero dependency on any other `drift` crate** (`drift-core`,
`drift-provider`, `drift-tools` are all consumed only by the separate `drift-script-ai`
binding crate, not by `drift-script` itself) — confirmed by inspecting
`crates/drift-script/Cargo.toml` and its 12 source files directly. This makes it cleanly
extractable as a standalone crate that both `drift` and `apexes` can depend on externally,
rather than either vendoring a copy or `apexes` reaching into an unrelated monorepo's internal
crate path.

## 2. Scope

**Extract exactly one crate:** `crates/drift-script` (parser, HIR, compiler, VM, interpreter,
`Environment`/`BindingProvider`/`BindingRegistry`, `DriftValue`/`DriftFunction`/`NativeFn`,
`Module`/`ModuleLoader`, `Promise`/`promise_all`/`promise_race`) — 12 source files, no
external `drift-*` dependencies.

**Explicitly NOT extracted** (stay in the `drift` monorepo, unchanged in scope):
- `drift-script-stdlib` (fs/http/regex bindings) — `drift`-specific utility bindings; Apexes
  will write its own trading-specific `BindingProvider` in `apexes-strategy` instead.
- `drift-script-ai` (LLM/MCP/ACP bindings) — has real dependencies on `drift-core`,
  `drift-provider`, `drift-tools`; not relevant to Apexes at all.
- `drift-script-signing` — an unrelated Ed25519 signing utility (name coincidentally similar,
  no actual dependency on `drift-script`).
- `drift-lsp`, `drift-tools`, `drift` (CLI) — these become **consumers** of the extracted
  crate via an external git dependency, not part of the extraction itself.

**Crate/package naming:** the extracted crate keeps its Rust package name `drift-script`
(not renamed to `drift-dsl`) — only the *repository* is named `drift-dsl`. This means none of
`drift`'s 5 consumer crates (`drift-lsp`, `drift-script-ai`, `drift-script-stdlib`,
`drift-tools`, `drift`) need any `use drift_script::...` import changes — only their
`Cargo.toml`'s dependency *source* changes from a local `path` to a git URL.

## 3. Target repository

`github.com/LeafAI/drift-dsl` (already created, currently empty, default branch to be renamed
from `main` to `master` to match the project's established branch-naming convention). Its own
`Cargo.toml` workspace (or a single-crate `Cargo.toml`, since there's only one crate), README,
LICENSE (match `drift-script`'s current license — check `drift`'s workspace/crate-level
license metadata before publishing), and a CI setup (GitHub Actions: `cargo test`, `cargo
clippy -- -D warnings`, `cargo fmt --check` on push, mirroring the conventions already used in
this session for Apexes) — this is a new, independent, publicly-consumable open-source crate,
so it should have baseline CI from day one, not rely on manual verification alone going
forward.

## 4. Migration mechanics

1. Copy `crates/drift-script`'s full source tree (all 12 files + its `Cargo.toml`) into the
   new `drift-dsl` repo, adjusted to be a standalone crate. This requires resolving
   `drift-script`'s current `Cargo.toml` dependencies that use `{ workspace = true }` (pointing
   at version pins declared in `drift`'s root `Cargo.toml`: `oxc_parser`/`oxc_ast`/`oxc_span`
   version `"0.26"`, `serde`/`thiserror`/`tracing` at whatever versions `drift`'s workspace
   pins) into explicit version numbers in the new standalone `Cargo.toml`, since there is no
   parent workspace to inherit from. Use the exact same version numbers `drift`'s workspace
   currently pins (checked directly from `drift`'s root `Cargo.toml` before writing the new
   crate's `Cargo.toml`) to guarantee behavioral identity — do not bump any dependency version
   during this extraction.
2. Preserve `drift-script`'s existing test suite (`#[cfg(test)] mod tests` blocks, if any
   exist per-file, plus any `tests/` integration tests) verbatim as part of the copy — this is
   a move, not a rewrite; the design goal is behavioral identity, zero functional changes.
3. Push to `LeafAI/drift-dsl`, rename default branch to `master`, verify `cargo build`/`cargo
   test`/`cargo clippy -- -D warnings`/`cargo fmt --check` all pass in the new repo standalone
   (no dependency on the `drift` monorepo's workspace `Cargo.toml` settings).
4. In the `drift` monorepo, update each of the 5 consumer crates' `Cargo.toml`:
   ```toml
   # before:
   drift-script = { path = "../drift-script" }
   # after:
   drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha-from-step-3>" }
   ```
5. Delete `crates/drift-script` from the `drift` monorepo's `crates/` directory. Note:
   `drift`'s workspace `members` is the glob `"crates/*"` (confirmed by inspecting `drift`'s
   root `Cargo.toml`), so deleting the directory automatically removes it from the workspace
   with no `Cargo.toml` edit needed for this step.
6. Run `cargo build --workspace` and `cargo test --workspace` in `drift` to confirm nothing
   broke — this is the critical regression check, since `drift` is a live, actively-used
   project (uncommitted unrelated work already exists in the working tree, per the
   pre-flight check — those files must not be touched by this refactor).
7. Commit the `drift`-side changes (Cargo.toml updates for the 5 consumers, workspace member
   list update, `crates/drift-script` deletion) as its own logical commit set, separate from
   any of the pre-existing uncommitted unrelated work in that repo.

## 5. Testing strategy

- **`drift-dsl` repo**: all of `drift-script`'s existing tests must pass unchanged after the
  move (behavioral-identity requirement — this is a relocation, not a rewrite). Add a CI
  workflow (`cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`) so the new repo
  has automated verification from its first commit.
- **`drift` repo**: after switching the 5 consumers to the git dependency, `cargo build
  --workspace` and `cargo test --workspace` must pass with the exact same test results as
  before the refactor (this is a pure dependency-source change, not a logic change — any test
  failure here indicates the extraction broke something and must be investigated before
  proceeding, not silently worked around).

## 6. Error handling / rollback

If `drift`'s workspace fails to build/test after switching to the git dependency, the fix
must be to correct the git dependency reference (wrong rev, missing feature flag, etc.) or fix
a genuine behavioral difference introduced during the copy — not to revert to the local `path`
dependency, since the whole point of this effort is to make the git dependency the source of
truth going forward. If a truly blocking issue arises that can't be resolved in a reasonable
number of attempts, stop and report to the user rather than reverting silently.

## 7. Non-goals

- No `apexes`-side work happens in this effort — `apexes-strategy`'s own design (which
  `BindingProvider` to write for trading primitives, how `Strategy` wraps the interpreter) is
  a separate, subsequent brainstorming cycle once `drift-dsl` exists as a stable, pushed,
  externally-consumable crate.
- No changes to `drift-script`'s language semantics, grammar, or public API during this
  extraction — this is purely a repository/dependency-graph change.
- No changes to the unrelated in-progress uncommitted work already present in the `drift`
  working tree (`Cargo.toml`/`Cargo.lock`/`drift-tools` web.rs/`drift/main.rs`/the new
  web-crawl-tool spec doc) — these files must be left untouched unless the extraction
  mechanically requires touching one of them (e.g. the workspace `Cargo.toml`'s `members`
  list), in which case only the minimal necessary lines are touched and the rest of the
  existing diff in that file is preserved as-is.
