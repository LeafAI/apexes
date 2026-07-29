# Extract `drift-script`, `drift-script-stdlib` + new `drift-script-math` into `LeafAI/drift-dsl` — Design Spec

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

**Extract two crates:**
- `crates/drift-script` (parser, HIR, compiler, VM, interpreter,
  `Environment`/`BindingProvider`/`BindingRegistry`, `DriftValue`/`DriftFunction`/`NativeFn`,
  `Module`/`ModuleLoader`, `Promise`/`promise_all`/`promise_race`) — 12 source files, no
  external `drift-*` dependencies.
- `crates/drift-script-stdlib` (general-purpose language bindings: `fs`, `shell`, `http`,
  `json`, `log`, `console`, `os`, `process`, `string`, `object`) — 11 source files. Confirmed
  by inspecting its `Cargo.toml` and every source file: it depends only on `drift-script`
  (the sibling crate above) plus generic, general-purpose crates (`tokio`, `reqwest`,
  `serde_json`, `glob`, `ignore`, `grep-regex`, `grep-searcher`, `regex`, `tracing`) — nothing
  `drift`-monorepo-specific (no `drift-core`/`drift-provider`/`drift-tools`). Its bindings
  (filesystem, shell exec, HTTP, JSON, logging) are genuinely general-purpose language
  standard-library functionality, not agent-framework-specific, so it belongs alongside the
  language itself rather than staying `drift`-only.

**New scope added to this extraction: a new, independent `drift-script-math` crate.** Rather
than a module inside `drift-script-stdlib`, math/statistics functionality is its own crate in
the `drift-dsl` workspace (third workspace member, alongside `drift-script` and
`drift-script-stdlib`) — matching `drift-script-stdlib`'s own shape (depends only on
`drift-script` + generic crates, registers a `BindingRegistry`-compatible set of bindings) but
kept separate since numeric/statistical functionality is a distinct concern from
OS/IO/string/console bindings, and this repository is expected to grow further independent
binding crates over time (see "Future extensibility" below).

Scope: general-purpose arithmetic helpers not already covered by the language's own
operators (`abs`, `min`, `max`, `round`, `floor`, `ceil`, `trunc`, `pow`, `sqrt`, `log`
natural log, `exp`, `clamp`, plus the constants `PI`/`E`) PLUS general-purpose statistics
over an array of numbers (`mean`, `stddev`, `variance`, `sum`, `product`). Still no
trading-specific indicators (SMA/EMA/RSI/Bollinger Bands, etc.) — those belong to
`apexes-strategy`'s own binding crate, built as a downstream consumer of
`drift-script-math` (and `drift-script`/`drift-script-stdlib`) rather than living in
`drift-dsl` itself, since they're domain-specific to trading, not general-purpose language
functionality.

All numeric values follow `DriftValue::Number(f64)`, matching every other numeric value in
the language (`DriftValue` has no separate integer type, per `drift-script`'s `value.rs`).
Statistics functions accept a `DriftValue::Array(Vec<DriftValue>)` where every element must
itself be `DriftValue::Number` (return an error object, consistent with the rest of the
stdlib's error-handling convention, if any element is not a number).

**Future extensibility (not otherwise part of this extraction, noted for context):**
`drift-dsl` is expected to grow further independent binding crates over time beyond
`drift-script-stdlib` and `drift-script-math` (following the same pattern: depend only on
`drift-script` + generic crates, register bindings via the same `BindingRegistry`-compatible
shape). This extraction doesn't need to build anything beyond `drift-script-math` as
described above; the note exists only to confirm the repository's intended shape (one core
language crate + a growing set of independent, general-purpose
binding functionality) so this extraction's structure doesn't need revisiting later.

**Explicitly NOT extracted** (stay in the `drift` monorepo, unchanged in scope):
- `drift-script-ai` (LLM/MCP/ACP bindings) — has real dependencies on `drift-core`,
  `drift-provider`, `drift-tools`; not relevant to Apexes at all, and not general-purpose
  (agent-framework-specific).
- `drift-script-signing` — an unrelated Ed25519 signing utility (name coincidentally similar,
  no actual dependency on `drift-script`).
- `drift-lsp`, `drift-tools`, `drift` (CLI) — these become **consumers** of the extracted
  crates via an external git dependency, not part of the extraction itself.

**Crate/package naming:** the two extracted crates keep their Rust package names
`drift-script` and `drift-script-stdlib` (not renamed) — only the *repository* is named
`drift-dsl`. This means none of `drift`'s consumer crates need any `use
drift_script::...`/`use drift_script_stdlib::...` import changes — only their
`Cargo.toml`'s dependency *source* changes from a local `path` to a git URL. The new
`drift-script-math` crate is a brand-new crate (no prior existence in `drift` to preserve
import compatibility for), so it can be named however makes sense from the start — 
`drift-script-math` matches the sibling `drift-script-stdlib`'s naming convention.

## 3. Target repository

`github.com/LeafAI/drift-dsl` (already created, currently empty, default branch to be renamed
from `main` to `master` to match the project's established branch-naming convention). Its own
`Cargo.toml` workspace (three members: `drift-script`, `drift-script-stdlib`,
`drift-script-math`), README, LICENSE (match `drift-script`'s current license — check
`drift`'s workspace/crate-level license metadata before publishing), and a CI setup (GitHub
Actions: `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` on push, mirroring
the conventions already used in this session for Apexes) — this is a new, independent,
publicly-consumable open-source crate,
so it should have baseline CI from day one, not rely on manual verification alone going
forward.

## 4. Migration mechanics

Note: `drift-script-math` is a brand-new crate with no existing consumers in `drift` — none
of `drift`'s current code uses it, so it requires no `drift`-side Cargo.toml changes at all.
The consumer-migration steps below apply only to the two crates being *relocated* from
`drift` (`drift-script`, `drift-script-stdlib`); `drift-script-math` is simply added
alongside them as a new workspace member in `drift-dsl`, to be consumed later by `apexes`.

**Consumers of the two relocated crates within `drift`** (confirmed by grep across all
`crates/*/Cargo.toml`):

| Consumer crate | Depends on `drift-script` | Depends on `drift-script-stdlib` |
|---|---|---|
| `drift-lsp` | Yes | No |
| `drift-script-ai` | Yes | No |
| `drift-tools` | Yes | Yes |
| `drift` (CLI) | Yes | Yes |

(`drift-script-stdlib` itself also depends on `drift-script`, but that dependency moves
*with* it into `drift-dsl` — both crates live in the same new repo, so that dependency
becomes an intra-repo path dependency there, unchanged in spirit.)

1. Copy `crates/drift-script`'s full source tree (all 12 files + its `Cargo.toml`) AND
   `crates/drift-script-stdlib`'s full source tree (all 11 files + its `Cargo.toml`) into the
   new `drift-dsl` repo as a Cargo workspace, adjusted to be standalone. This requires
   resolving both crates' current `Cargo.toml` dependencies that use `{ workspace = true }`
   (pointing at version pins declared in `drift`'s root `Cargo.toml`:
   `oxc_parser`/`oxc_ast`/`oxc_span` version `"0.26"`,
   `serde`/`thiserror`/`tracing`/`tokio`/`grep-regex`/`grep-searcher` at whatever versions
   `drift`'s workspace currently pins) into a `[workspace.dependencies]` section in the new
   repo's root `Cargo.toml` mirroring the relevant subset of `drift`'s pins — use the EXACT
   SAME version numbers `drift`'s workspace currently pins (checked directly from `drift`'s
   root `Cargo.toml` before writing the new repo's `Cargo.toml`) to guarantee behavioral
   identity — do not bump any dependency version during this extraction.
   `drift-script-stdlib`'s dependency on `drift-script` becomes `drift-script = { path =
   "../drift-script" }` within the new repo.
2. Add the new `drift-script-math` crate as a third workspace member (see §2 above for its
   scope) — this one has no `drift`-side equivalent to copy from, it's written fresh as part
   of this extraction.
3. Preserve both relocated crates' existing test suites (`#[cfg(test)] mod tests` blocks,
   plus any `tests/` integration tests) verbatim as part of the copy — this is a move, not a
   rewrite for those two; the design goal is behavioral identity, zero functional changes for
   `drift-script`/`drift-script-stdlib`. `drift-script-math` is new code and needs its own
   real test suite written as part of this extraction (not a copy).
3. Push to `LeafAI/drift-dsl`, rename default branch to `master`, verify `cargo build`/`cargo
   test`/`cargo clippy -- -D warnings`/`cargo fmt --check` all pass in the new repo standalone
   (no dependency on the `drift` monorepo's workspace `Cargo.toml` settings).
4. In the `drift` monorepo, update each consumer crate's `Cargo.toml` per the table above:
   ```toml
   # before:
   drift-script = { path = "../drift-script" }
   drift-script-stdlib = { path = "../drift-script-stdlib" }  # only for drift-tools, drift
   # after:
   drift-script = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha-from-push-step>" }
   drift-script-stdlib = { git = "https://github.com/LeafAI/drift-dsl.git", rev = "<commit-sha-from-push-step>" }
   ```
5. Delete `crates/drift-script` AND `crates/drift-script-stdlib` from the `drift` monorepo's
   `crates/` directory. Note: `drift`'s workspace `members` is the glob `"crates/*"`
   (confirmed by inspecting `drift`'s root `Cargo.toml`), so deleting the directories
   automatically removes them from the workspace with no `Cargo.toml` edit needed for this
   step.
6. Run `cargo build --workspace` and `cargo test --workspace` in `drift` to confirm nothing
   broke — this is the critical regression check, since `drift` is a live, actively-used
   project (uncommitted unrelated work already exists in the working tree, per the
   pre-flight check — those files must not be touched by this refactor).
7. Commit the `drift`-side changes (Cargo.toml updates for the 4 consumer crates,
   `crates/drift-script` + `crates/drift-script-stdlib` deletion) as its own logical commit
   set, separate from any of the pre-existing uncommitted unrelated work in that repo.

## 5. Testing strategy

- **`drift-dsl` repo**: all of `drift-script`'s existing tests must pass unchanged after the
  move (behavioral-identity requirement — this is a relocation, not a rewrite). The new
  `drift-script-math` crate needs a real, from-scratch test suite covering every exposed
  function (arithmetic helpers and statistics), including edge cases (e.g. `sqrt` of a
  negative number, `mean`/`stddev` of an empty array, a statistics function given a
  non-numeric array element) — this is new code, not a relocation, so it doesn't inherit
  test coverage from anywhere. Add a CI workflow (`cargo test`, `cargo clippy -- -D
  warnings`, `cargo fmt --check`) so the new repo has automated verification from its first
  commit.
- **`drift` repo**: after switching the 4 consumer crates to the git dependency, `cargo build
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
  `BindingProvider` to write for trading primitives, how `Strategy` wraps the interpreter,
  how the technical-indicator library relates to `drift-script-math`) is a separate,
  subsequent brainstorming cycle once `drift-dsl` exists as a stable, pushed,
  externally-consumable set of crates. `drift-script-math` provides only general-purpose
  arithmetic/statistics — trading-specific indicators (SMA/EMA/RSI/Bollinger Bands) are
  entirely out of scope for this effort and will be designed as part of `apexes-strategy`.
- No changes to `drift-script`'s language semantics, grammar, or public API during this
  extraction — this is purely a repository/dependency-graph change.
- No changes to the unrelated in-progress uncommitted work already present in the `drift`
  working tree (`Cargo.toml`/`Cargo.lock`/`drift-tools` web.rs/`drift/main.rs`/the new
  web-crawl-tool spec doc) — these files must be left untouched unless the extraction
  mechanically requires touching one of them (e.g. the workspace `Cargo.toml`'s `members`
  list), in which case only the minimal necessary lines are touched and the rest of the
  existing diff in that file is preserved as-is.
