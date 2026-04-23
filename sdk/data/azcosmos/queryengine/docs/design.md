# Cosmos DB Cross-Partition Query Engine (Go) — Design

- **Linear issue:** BYT-9239
- **Date:** 2026-04-22
- **Status:** Proposed
- **Authoritative target query list:** [`./target-queries.md`](./target-queries.md) (40 queries across 13 feature areas, each annotated with `.NET SDK` pass/fail and failure reason)

## 1. Context and problem statement

Bytebase's Cosmos DB SQL viewer fails on every query that requires client-side cross-partition orchestration — `TOP`, single-column `ORDER BY`, scalar aggregates in both aliased (`AS x`) and `VALUE` forms, multi-aggregate in one SELECT, `GROUP BY`, `DISTINCT`, `DISTINCT VALUE`, `SELECT VALUE agg(...)`, and `OFFSET/LIMIT`. The gateway returns `BadRequest 400`: *"The provided cross partition query can not be directly served by the gateway. … This exception is traced, but unless you see it bubble up as an exception (which only happens on older SDK clients), you can safely ignore this message."*

### 1.1 Root cause

Bytebase's driver imports `github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos`, replaced in `go.mod` with the fork `github.com/bytebase/azure-sdk-for-go/sdk/data/azcosmos v0.0.0-20260414094732-5b89f0889452` (commit `5b89f0889452`, 2026-04-14, on the upstream `1.5.0-beta.6 (Unreleased)` track). The fork already exposes a `queryengine.QueryEngine` interface on `QueryOptions.QueryEngine`, but neither the SDK nor Bytebase ship an implementation — Microsoft's reference implementation lives in a separate `azcosmoscx` package (Rust-backed) and is not imported.

Queries that succeed in the .NET SDK (`Microsoft.Azure.Cosmos` 3.47.1, the reference implementation) fail in the Go fork for a simple reason: **the .NET SDK has a built-in distributed query engine; the Go SDK has only an interface hook.** A .NET reproduction harness (maintained in the Bytebase monorepo under `scripts/cosmos-repro-dotnet/`) confirms this — 39/40 queries PASS on .NET (Gateway and Direct modes), the one remaining FAIL is 4.3 which requires a server-side composite index and fails identically on every SDK.

> The reproduction harness and the Go twin harness referenced throughout this document (`scripts/cosmos-repro-dotnet/`, `scripts/cosmos-repro-go/`) live in the Bytebase monorepo, not this repository. They are test infrastructure, not part of the SDK itself. The SDK's own tests live in `sdk/data/azcosmos/**_test.go` as usual.

### 1.2 Why the obvious fixes don't work

- **Sync the fork with upstream.** Already done — problem persists because upstream itself only ships the interface, not an implementation.
- **Use `azcosmoscx`.** Rust / cgo / plugin dependency that Bytebase's codebase does not have today. Rejected.
- **Reject cross-partition queries with better errors.** Does not make any currently-failing query pass. Rejected as insufficient.
- **Run a .NET side-process.** Adds a runtime dependency and deployment footprint. Rejected as heavyweight.

## 2. Goals and non-goals

### 2.1 Goal
Ship a **pure-Go distributed query engine** inside the Bytebase fork of `azcosmos` so that every Cosmos SQL query that succeeds on `Microsoft.Azure.Cosmos` 3.47.1 also succeeds against the same account through Bytebase, with matching row output.

The authoritative target list is the 40-query inventory in the plan file referenced at the top of this document. For each query, the `.NET SDK` column defines the expected pass/fail outcome; the Go engine must match it. 39/40 must pass; query 4.3 remains FAIL because it requires a composite index — a server constraint, not an SDK gap.

### 2.2 Non-goals
1. Not implementing any feature the .NET SDK itself doesn't support against a serverless account.
2. Not vendoring, cgo-linking, or plugin-loading Microsoft's `azcosmoscx` (Rust) engine.
3. Not upstreaming the implementation to `Azure/azure-sdk-for-go` in the same delivery. (Follow-up.)
4. Not changing any Bytebase driver, parser, or UI surface for this fix. (A one-line `go.mod` bump is the only Bytebase-side change.)
5. Not optimizing for high-QPS analytical workloads — correctness first; perf work is a follow-up if latency/memory becomes a problem in practice.

### 2.3 Success criteria
- `scripts/cosmos-repro-dotnet/` has a Go twin that runs the same target query list. 39 of 40 pass; 4.3 continues to fail with the same server error as .NET.
- The new engine is auto-enabled in `container.NewCrossPartitionQueryItemsPager`; callers get correct results without code changes.
- CI unit tests pass in under 30 s, mocked end-to-end. Integration suite runs when `AZURE_COSMOS_KEY` is set; skipped otherwise.

## 3. SDK architecture

### 3.1 Package layout in the fork

```
sdk/data/azcosmos/
├── queryengine/                       # existing — interface & types
│   ├── cosmos_query_engine.go         # existing
│   └── internal/
│       └── gonative/                  # NEW — pure-Go default implementation
│           ├── engine.go              # implements queryengine.QueryEngine
│           ├── pipeline.go            # QueryPipeline fsm
│           ├── plan.go                # JSON model of the query plan
│           ├── merge.go               # k-way ORDER BY merge
│           ├── agg.go                 # COUNT/SUM/MIN/MAX/AVG finalizers
│           ├── distinct.go            # hash-set dedupe
│           ├── group.go               # GROUP BY hashmap
│           ├── topskiptake.go         # TOP / OFFSET / LIMIT
│           └── *_test.go              # unit tests (mocked plans + partition data)
├── cosmos_container_query_engine.go   # existing — modify to default-wire gonative
└── cosmos_container.go                # existing — NewCrossPartitionQueryItemsPager unchanged externally
```

`gonative` lives under `internal/` so its package surface is not part of the SDK's public API. Only the `queryengine.QueryEngine` interface implementation escapes — everything else in `gonative` can be refactored freely by later stages. No new public API on the `azcosmos` package itself.

### 3.2 Activation

Auto-enable with a safe fallback:

1. If `QueryOptions.QueryEngine == nil`, the SDK injects `gonative.Default()` at the top of `NewCrossPartitionQueryItemsPager`. Callers retain full control: passing a non-nil value overrides; passing a new sentinel `queryengine.Disabled` preserves today's raw-pass-through behavior as an escape hatch.
2. For every cross-partition query, the SDK fetches the plan. The plan response tells us whether the gateway can serve it directly. If yes, we skip the engine and run the existing fast path — **zero orchestration overhead for single-partition queries beyond the plan round-trip**.
3. An in-memory LRU plan cache keyed by `(databaseID, containerID, normalizedQuery, features)` on `ContainerClient` (default 256 entries, configurable via `CosmosClientOptions.QueryPlanCacheSize`) amortizes the plan-fetch cost across a SQL-viewer session.

### 3.3 Data flow

```
Bytebase.QueryConn
  └─ azcosmos.NewCrossPartitionQueryItemsPager
       ├─ (cache-miss) getQueryPlanFromGateway → plan JSON
       ├─ getPartitionKeyRangesRaw              → pkranges JSON
       ├─ engine.CreateQueryPipeline(query, plan, pkranges) → QueryPipeline
       └─ Pager loop (drives pipeline):
            for req in pipeline.NextRequests():
                resp = sendQuery(req.PKRangeID, req.Query, req.Continuation)
                pipeline.Feed(QueryResult{...})
            rows = pipeline.Drain()
       → returns a page of merged rows
```

For a gateway-servable query, the pipeline's `NextRequests()` returns a single pass-through request with no rewrite and `Drain()` is just a stream of raw rows — no added work.

### 3.4 Memory and streaming

| Operator | Intermediate state | Can emit incrementally? |
|---|---|---|
| Pass-through | O(page) | Yes |
| `ORDER BY` (single key) | O(partitions × 1 row) — k-way merge heap | Yes |
| `TOP N` / `OFFSET+LIMIT` | O(N) | Yes (emits as soon as window fills) |
| `DISTINCT` | O(distinct cardinality) — hash set of seen keys | Yes (emit first occurrence) |
| `GROUP BY` | O(group cardinality) — hashmap of state | No — must drain all partitions first |
| Scalar aggregates (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) | O(1) per aggregate | No — emits single final row |

`GROUP BY` and scalar aggregates require draining all partitions before any row is emitted. Everything else streams. The Bytebase driver's existing `queryContext.Limit` early-termination loop at `cosmosdb.go:172-190` continues to work for streaming operators; for non-streaming operators, `Limit` caps the emitted row count at the end (same semantics as any DB).

### 3.5 Error handling

The engine never invents error types. Gateway errors propagate through unchanged (so the UI keeps seeing the same `BadRequest` for query 4.3's composite-index case). A plan feature the engine hasn't implemented returns `queryengine.ErrUnsupportedPlanFeature`; the pager falls back to the raw pass-through — i.e. current behavior — so partially-landed stages never make things worse.

## 4. Rollout order and per-stage scope

Seven PRs in dependency order. Each is reviewable standalone, behind a feature announcement in `SupportedFeatures()`, and adds no regression to queries outside its scope.

### Stage 0 — Engine skeleton
`queryengine/gonative/` package skeleton; default-wiring in `NewCrossPartitionQueryItemsPager`; plan fetch + pk-ranges + LRU plan cache; a pass-through pipeline; `SupportedFeatures()` returns empty — gateway refuses any cross-partition orchestration, exactly like today. **Queries covered:** none (parity). **Tests:** unit (plan cache, pass-through, fallback); integration asserts no regression on the 20 queries currently passing in Go (1.1, 1.2, 2.1–2.9, 3.1–3.3, 7.1, 8.1, 9.1, 9.2, 11.1, 13.1) and the 20 still failing (1.3, 4.1–4.3, 5.1/5.1b/5.2/5.2b/5.3/5.3b/5.4/5.4b/5.5, 6.1, 6.2, 10.1, 10.2, 11.2, 12.1, 12.2).

### Stage 1 — Scalar aggregates
`agg.go` with finalizers for `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`. Both `SELECT VALUE agg(x) FROM c` and `SELECT agg(x) AS alias FROM c` (gateway rewrites aliased form to `VALUE` then the pipeline wraps scalar into `{alias: value}`). Multi-aggregate in one SELECT via a parallel state vector. **Queries covered:** 5.1, 5.1b, 5.2, 5.2b, 5.3, 5.3b, 5.4, 5.4b, 5.5, 11.2.

### Stage 2 — DISTINCT
`distinct.go`: streaming hash-set keyed on canonical JSON (sorted keys) of the distinct projection. Both object DISTINCT and `DISTINCT VALUE`. Ordered DISTINCT composes with Stage 3 automatically. **Queries covered:** 10.1, 10.2.

### Stage 3 — ORDER BY (single key)
`merge.go`: min-heap (or max-heap) k-way merge across partition streams, typed comparator using Cosmos item ordering (`undefined < null < bool < number < string`). ASC and DESC. Multi-key ORDER BY (4.3) is **not** added — server-side composite-index requirement, out of scope. **Queries covered:** 4.1, 4.2.

### Stage 4 — TOP
`topskiptake.go`: wraps upstream operator, emits first N, cancels upstream on satisfaction (streaming early-termination). Stacks on Stage 3's merger. **Queries covered:** 1.3.

### Stage 5 — OFFSET / LIMIT
Extends Stage 4 with a skip counter. Cosmos syntactically requires `ORDER BY` before `OFFSET/LIMIT`, so upstream is always the merger. **Queries covered:** 12.1, 12.2.

### Stage 6 — GROUP BY
`group.go`: `map[groupKey]*aggState` where `aggState` reuses Stage 1's finalizers. Drains all partitions, emits one row per group. **Queries covered:** 6.1, 6.2.

### Stage 7 — Repro-harness parity (Go twin)
`scripts/cosmos-repro-go/` mirroring `scripts/cosmos-repro-dotnet/`. Running both against the same target list should produce the same PASS/FAIL column for all 40 queries. Becomes the regression guard for BYT-9239.

### Cumulative pass count from the 40-query inventory

| Stage | `SupportedFeatures()` added | Queries unlocked | Cumulative PASS |
|---|---|---|---|
| 0 | *(none)* | 0 | 20 / 40 |
| 1 | `Aggregate` | 10 (5.1, 5.1b, 5.2, 5.2b, 5.3, 5.3b, 5.4, 5.4b, 5.5, 11.2) | 30 / 40 |
| 2 | `Distinct` | 2 (10.1, 10.2) | 32 / 40 |
| 3 | `OrderBy` | 2 (4.1, 4.2) | 34 / 40 |
| 4 | `Top` | 1 (1.3) | 35 / 40 |
| 5 | `OffsetAndLimit` | 2 (12.1, 12.2) | 37 / 40 |
| 6 | `GroupBy` | 2 (6.1, 6.2) | 39 / 40 |
| 7 | *(harness)* | 0 | 39 / 40 (confirmed end-to-end against Azure) |

The permanent FAIL is 4.3 (composite-index server constraint — out of scope for any SDK).

## 5. Per-stage engineering details

### 5.1 Wire shapes (Stage 0)

Query plan JSON (trimmed to relevant fields):

```json
{
  "partitionedQueryExecutionInfoVersion": 2,
  "queryInfo": {
    "rewrittenQuery": "SELECT ... FROM c WHERE ...",
    "hasSelectValue": true,
    "hasNonStreamingOrderBy": false,
    "distinctType": "None | Ordered | Unordered",
    "top": null,
    "offset": null,
    "limit": null,
    "orderBy": ["Ascending"],
    "orderByExpressions": ["c.population"],
    "groupByExpressions": [],
    "groupByAliases": [],
    "aggregates": ["Count"],
    "groupByAliasToAggregateType": {}
  },
  "queryRanges": [{"min": "...", "max": "...", "isMinInclusive": true, "isMaxInclusive": false}]
}
```

`rewrittenQuery` is the per-partition sub-query. Empty means "use the original query unchanged." The pk-ranges response is a list of `{id, minInclusive, maxExclusive}` entries; we fire one partition request per entry that overlaps the plan's `queryRanges`.

**Plan cache.** New private field on `ContainerClient`:

```go
planCache *lru.Cache[planCacheKey, []byte]   // default 256 entries
type planCacheKey struct{ dbID, containerID, normalizedQuery, features string }
```

Stored value is the raw plan bytes; re-parse on every use (parse cost is negligible next to the RTT).

### 5.2 Pipeline FSM (Stage 0, reused by every later stage)

```go
type pipeline struct {
    plan          *planDoc
    partitions    []partitionState      // one per overlapping pk-range
    operator      operator              // built from plan
    pendingReqs   []queryengine.QueryRequest
    doneOnce      sync.Once
    err           error
}

type operator interface {
    Next(ctx context.Context) (row json.RawMessage, ok bool, err error)
}
```

Operators compose top-down: passthrough → orderByMerge → topSkipTake → aggregate → groupBy → distinct. `NextRequests()` returns the initial batch (one request per partition) and later batches for partitions with continuation tokens still outstanding. `Feed()` parses a `QueryResult`'s `Data` into `[]json.RawMessage` and pushes to that partition's buffered channel. `NextRows()` pulls from `operator.Next()` and emits whatever's ready.

### 5.3 Concurrency

One goroutine per active partition, each pulling one page's continuation at a time. Bounded semaphore caps in-flight requests at `min(len(partitions), runtime.GOMAXPROCS(0))`. Context cancellation unblocks everything. `errgroup` coordinates failures.

### 5.4 Operator details

**Stage 1 — Aggregates.** Gateway emits partials in documented wire forms:

- `Count`: raw integer per partition.
- `Sum`: raw number per partition.
- `Min` / `Max`: `{"min": v, "count": n}` or `{"max": v, "count": n}`; n=0 means "this partition had no values" and is skipped.
- `Avg`: `{"sum": s, "count": n}`; finalize as `Σs / Σn`.

Finalizers combine partials linearly. Aliased projection (`SELECT agg(x) AS alias FROM c`): gateway's `rewrittenQuery` rewrites to `VALUE` form; after finalization, the pipeline wraps the scalar back into `{"alias": v}` (alias from `queryInfo.groupByAliasToAggregateType`, which — despite the name — also carries non-grouped aliased aggregates).

Multi-aggregate (`SELECT MIN(x) AS a, MAX(x) AS b`): gateway emits `aggregates: ["Min","Max"]` and a per-partition array of per-aggregate partials in position order. The pipeline carries a parallel state vector.

**Stage 2 — DISTINCT.** Hash key = SHA-256 of canonical JSON (sorted keys, no whitespace) of the projection row. 256-bit keys avoid collision handling and bound memory. Emit on first occurrence, skip on repeat. Same code path handles object DISTINCT and `DISTINCT VALUE` (both are "hash the whole row").

**Stage 3 — ORDER BY single-key.** Min/max-heap over `{partitionIdx, row, orderKey}`. Popping the top pulls the next row from that partition's channel; if the channel is empty *and* the partition has no continuation left, drop it; else block. Comparator uses Cosmos item ordering.

**Stage 4 — TOP.** Wrap upstream: count emitted rows; on reaching `N`, close upstream (cancels outstanding partition goroutines via context).

**Stage 5 — OFFSET / LIMIT.** Same operator as Stage 4 with a `skip` counter before emit. Upstream is always the Stage 3 merger.

**Stage 6 — GROUP BY.** `map[groupKey]*aggState`. `groupKey` = canonical JSON of grouping expressions from `queryInfo.groupByExpressions`. `aggState` = state vector from Stage 1. Drain → emit. Order unspecified (matches Cosmos semantics).

### 5.5 Private helpers the fork touches

Stage 0 is the only stage that reaches beyond `queryengine/gonative/`:

| File | Change |
|---|---|
| `cosmos_container_query_engine.go` | In `NewCrossPartitionQueryItemsPager`, default `queryOptions.QueryEngine` to `gonative.Default()` when nil (unless `queryengine.Disabled`). Existing private helpers `getQueryPlanFromGateway` and `getPartitionKeyRangesRaw` are already wired; no new public API. |
| `cosmos_container.go` | Add `planCache` field to `ContainerClient`; initialize in `NewContainer`. |
| `cosmos_client_options.go` | Add optional `QueryPlanCacheSize int` (default 256). |
| `queryengine/cosmos_query_engine.go` | Add `Disabled` sentinel, `ErrUnsupportedPlanFeature` sentinel, helper `JoinFeatures([]Feature) string`. |
| `CHANGELOG.md` | One entry per stage. |

### 5.6 Failure modes

| Scenario | Behavior |
|---|---|
| Plan fetch errors (network / auth / throttle) | Propagate to caller. No fallback — the plan fetch is the first real call. |
| Plan references a feature not yet implemented | `CreateQueryPipeline` returns `ErrUnsupportedPlanFeature`; pager falls back to raw single-query path (today's behavior; guaranteed no regression). |
| Partition query errors mid-stream | Cancel other partitions; return wrapped error. No partial results. |
| Context cancellation | All goroutines exit on next select; pager returns `context.Canceled`. |
| Gateway 429 on per-partition query | SDK's existing retry policy handles `x-ms-retry-after-ms` at the HTTP layer; partition goroutines retry transparently. |

### 5.7 Explicitly not touched

- `CreateReadManyPipeline` → `return nil, ErrUnsupportedPlanFeature`. Bytebase doesn't use ReadMany; parity goal doesn't cover it.
- `hasNonStreamingOrderBy` queries (vector/hybrid search scoring) → `ErrUnsupportedPlanFeature`. Not in the target inventory.
- Upstream contribution to `Azure/azure-sdk-for-go` → follow-up after Stage 6.

## 6. Rollout, operations, risks

### 6.1 Rollout mechanics

Each of the seven stages is a separate PR against the `bytebase/azure-sdk-for-go` fork on integration branch `bytebase/cosmos-query-engine`. Merge to fork `main` once all seven land and the Stage 7 Go-twin harness reports 39/40 against both Azure and the emulator. Bytebase's backend picks up the new SDK via a single `go.mod` bump — no per-stage bumps. That bump is the only Bytebase-side reviewable change; it looks like a routine dependency bump.

Each fork PR adds a CHANGELOG entry under `1.5.0-beta.6 (Unreleased)`, documenting the newly-announced `SupportedFeatures()` value and the operator it enables.

### 6.2 Observability

Reuse the fork's existing `EventQueryEngine` log channel. Three log points, all gated behind that event:

- **Plan fetch** — `{query_hash, cache_hit, features_requested, rtt_ms}`.
- **Pipeline start** — `{operator_chain, partition_count, query_hash}`.
- **Partition sub-query** (debug) — `{pk_range_id, continuation_present, rewritten_query}`.

No new metrics surface.

### 6.3 Backward compatibility

- `QueryOptions.QueryEngine == nil` → auto-enable `gonative.Default()`. Existing callers see new behavior on the SDK bump; the change is "queries that used to 400 now succeed."
- `QueryOptions.QueryEngine = queryengine.Disabled` → preserves today's raw-forward behavior exactly. Escape hatch.
- All existing public APIs keep their signatures.

### 6.4 Open risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Plan-JSON shape drift | Medium | Decode with unknown-field tolerance; only reject on known-flag unsupported values. |
| Item-ordering semantics mismatch | Medium | Golden tests: Go output compared row-by-row to .NET harness across every type mix. |
| Upstream fork divergence on next sync | Low | All engine code in new sub-package; ~20 LOC surface in existing fork files. |
| Unbounded memory on huge GROUP BY / DISTINCT cardinality | Low / catastrophic | Configurable caps (`MaxGroupCount`, `MaxDistinctCount`, default 1e6); return `ErrCardinalityExceeded`. |
| Concurrency bugs in partition fan-out | Medium | `errgroup` pattern; race detector in CI; deliberate-latency fake partitions in unit tests. |
| Plan-cache staleness vs partition splits | Low | Cache stores only `queryInfo`; pk-ranges always fetched fresh. |

### 6.5 Follow-ups (out of scope)

1. Upstream PR to `Azure/azure-sdk-for-go` with `queryengine/gonative/` — separate issue after Stage 6 soaks for a week on Bytebase's staging.
2. UI hint for composite-index errors (query 4.3 class) — frontend-only PR, separate from BYT-9239.
3. `ReadManyItems` parity — if Bytebase ever adopts ReadMany.
4. Remove `backend/plugin/db/cosmosdb/emulator.go` if the engine obsoletes the hand-rolled REST fallback — verify during Stage 7.

### 6.6 Exit criteria

Before the `go.mod` bump merges into Bytebase:

- `scripts/cosmos-repro-dotnet/` and `scripts/cosmos-repro-go/` both report 39/40 PASS on the same Azure container; the one FAIL is 4.3 on both.
- All fork unit tests pass under `-race` on Linux amd64, Linux arm64, macOS arm64.
- Integration suite passes in the fork CI against Azure (key from secrets); auto-skipped locally without `AZURE_COSMOS_KEY`.
- CHANGELOG entry in the fork for `1.5.0-beta.6` summarizes the feature and notes the default-activation behavior change.

## 7. Open concerns from adversarial review (to be resolved in the implementation plan)

An adversarial review (2026-04-22) raised three high-severity concerns against the spec as written. They are not blockers for starting the plan, but each implementation stage must explicitly decide how to address them.

1. **Activation regression risk.** Default-wiring `gonative.Default()` for nil callers turns today's single-call gateway path into a multi-call path (plan fetch + PK-range discovery) before the first row is returned. Transient auth/throttle/network failures on those new calls could break queries that succeed today. The implementation plan should either (a) only engage the engine after the gateway returns the specific unsupported-cross-partition error, or (b) keep the engine opt-in and have Bytebase's driver pass it explicitly. Section 3.2's current "auto-enable" wording is the working hypothesis — the plan must test this assumption before Stage 0 merges.

2. **No operator-level kill switch from Bytebase.** `queryengine.Disabled` is the SDK-side escape hatch, but Bytebase's driver never sets `QueryOptions.QueryEngine`. After the `go.mod` bump, operators can't disable the engine without another code-and-dependency rollout. The plan must add a Bytebase-side config or feature flag (e.g. `BB_COSMOS_DISABLE_QUERY_ENGINE`) that the driver reads and propagates to `QueryOptions.QueryEngine = queryengine.Disabled` when set.

3. **Parity harness is too shallow to catch silent wrong-result bugs.** Stage 7's harness compares PASS/FAIL and row counts only (see `scripts/cosmos-repro-dotnet/Program.cs` — it counts `page.Count`, never serializes rows). That won't catch bad ORDER BY merges, DISTINCT leaks, wrong GROUP BY buckets, or malformed VALUE/aliased aggregate shapes. Stage 7 must be upgraded to canonicalize result payloads (sorted JSON for ordered queries, sorted-by-canonical-key for unordered) and diff .NET vs Go row-by-row. The existing `.NET` harness also needs updating to emit the canonical payload.

## 8. Appendix — reproduction artifacts

- `.NET` harness: `scripts/cosmos-repro-dotnet/` (`Microsoft.Azure.Cosmos 3.47.1`, targets `net10.0`) — produces the `.NET SDK` column of the target inventory.
- Query inventory and PASS/FAIL matrix: `./target-queries.md`.
- Raw output logs: logs captured during the reproduction run.
