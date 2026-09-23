# webhooksmith — improvement to-do

Current downloads: 149 (webhooksmith), 78 (webhooksmith-axum), 48 (webhooksmith-actix)

## Quick wins ✅ DONE

- [x] Add `//!` crate-level doc comment to `webhooksmith/src/lib.rs`
      — zero doc warnings; docs.rs page now shows full API overview with examples
- [x] `webhooksmith/Cargo.toml`: keywords `"outbox"` → `"retry"`, `"postgres"` → `"queue"`
- [x] `webhooksmith-axum/Cargo.toml`: keyword `"verification"` → `"signing"`
- [x] `webhooksmith-actix/Cargo.toml`: keyword `"verification"` → `"signing"`

## Medium effort ✅ DONE

- [x] Built `webhooksmith-cli` — 7 commands, release binary tested against live Postgres:
      `stats`, `endpoints`, `events`, `log`, `retry`, `retry-all`, `cleanup`
      Fixed clap 4 `global = true` bug (was duplicating argument requirement for subcommands)
      Supports both `--db-url` flag and `WEBHOOKSMITH_DATABASE_URL` env var
- [x] `webhooksmith/README.md`: added Changelog section (0.1.0 → 0.1.10)

## Verified ✅

- All test suites pass: 0 failures across all test files
  (15 unit + 26 adversarial + 10 bombardment + 5 circuit_breaker + 15 crud + ... = 256+ total)
- All doctests pass (7 pass, 1 correctly ignored — sqlx::query! with user-defined table)
- All 3 library crates docs build with zero warnings
- CLI edge cases verified: bad status, bad UUID, missing auth, flag vs env var, retry on non-dead event

## Remaining (requires human action)

- [ ] Write one blog post: "Reliable webhook delivery in Rust with the transactional outbox pattern"
      — post on dev.to or lobste.rs, link back to crate; estimated 3-5x download impact
- [ ] Publish `webhooksmith-cli` to crates.io on next version bump
      — `cargo publish -p webhooksmith-cli`
