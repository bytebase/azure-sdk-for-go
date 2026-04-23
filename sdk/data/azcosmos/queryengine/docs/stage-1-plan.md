# Cosmos Query Engine — Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unlock the 10 scalar-aggregate queries from the target inventory (5.1, 5.1b, 5.2, 5.2b, 5.3, 5.3b, 5.4, 5.4b, 5.5, 11.2) end-to-end — as measured by the Bytebase SQL viewer against an Azure Cosmos account — by shipping the first working operator in the pure-Go query engine, plus the activation mechanism that engages it safely (engage-on-error) and a Bytebase-side kill switch.

**Architecture:** Three phases in order.

- **Phase A — wire formats.** Capture real query-plan JSON and per-partition aggregate payloads from the live Azure account for each of the 10 target queries. Encode the observed shapes as Go structs with fixture-driven tests. No production code yet.
- **Phase B — aggregate operator.** Implement the finalizer math (COUNT/SUM/MIN/MAX/AVG, including multi-aggregate in one SELECT) and the `QueryPipeline` that drives it. `gonative.Default()` starts returning a real pipeline when the plan has aggregates and only aggregates. `SupportedFeatures()` advertises `"Aggregate"`.
- **Phase C — activation + downstream.** Add engage-on-error wiring in `NewCrossPartitionQueryItemsPager` (gateway first; on the specific cross-partition BadRequest, switch to the engine path). Start using the plan cache. Bump Bytebase's `go.mod` to pick up the new fork and add a `BB_COSMOS_DISABLE_QUERY_ENGINE` env var that propagates to `QueryOptions.QueryEngine = queryengine.Disabled`.

Each phase ships as its own commit(s) on the fork branch; the whole stage lands as one fork PR plus one Bytebase PR (the Bytebase PR is the smallest possible: `go.mod` + one file in the driver + one test).

**Tech Stack:** Go 1.21+; fork already has `hashicorp/golang-lru/v2` from Stage 0. No new SDK deps. Bytebase-side: existing stdlib (`os.Getenv`).

**Reference spec:** [`docs/superpowers/specs/2026-04-22-cosmos-cross-partition-query-engine-design.md`](./design.md) (also landed in the fork PR #2 at `sdk/data/azcosmos/queryengine/docs/design.md`).

**Authoritative target query list:** the 10 queries in Section 5 + 11.2 of [`./target-queries.md`](./target-queries.md).

**Stage 0 anchor:** `f86978d44f4f` on `bytebase/cosmos-query-engine-stage-0`. When Stage 1 ships, the new fork pseudo-version becomes the target for Bytebase's `go.mod`.

---

## Out of scope for Stage 1 (deferred)

- `DISTINCT`, `ORDER BY`, `TOP`, `OFFSET/LIMIT`, `GROUP BY` operators (Stages 2–6).
- Multi-key `ORDER BY` with composite indexes (query 4.3 — permanent FAIL).
- `ReadMany` pipeline — `CreateReadManyPipeline` still returns `ErrUnsupportedPlanFeature`.
- Streaming emission of aggregate results. Aggregates are O(1) per state, emit one row at the end — no streaming needed.
- Cosmos emulator vnext compatibility testing (deferred to Stage 7 alongside the Go harness).

## File map

```
# Fork: github.com/bytebase/azure-sdk-for-go
sdk/data/azcosmos/
├── cosmos_container.go                               # MODIFY — engage-on-error wrapper in NewCrossPartitionQueryItemsPager
├── cosmos_container_query_engine.go                  # MODIFY — plan-cache lookup around getQueryPlanFromGateway; expose as engine-path entry
├── queryengine/
│   └── internal/gonative/
│       ├── engine.go                                 # MODIFY — CreateQueryPipeline parses plan and dispatches; SupportedFeatures returns "Aggregate"
│       ├── plan.go                                   # CREATE — plan JSON model
│       ├── plan_test.go                              # CREATE — fixture-driven plan parse tests
│       ├── agg.go                                    # CREATE — per-aggregate finalizers
│       ├── agg_test.go                               # CREATE — finalizer tests with captured partition payloads
│       ├── pipeline.go                               # CREATE — QueryPipeline (aggregate flavor for Stage 1)
│       ├── pipeline_test.go                          # CREATE — pipeline end-to-end tests against captured fixtures
│       └── testdata/                                 # CREATE — captured JSON from Azure + .NET SDK
│           ├── query_5_1_plan.json                   # CREATE — plan JSON for each of the 10 target queries
│           ├── query_5_1_partitions/                 # CREATE — per-partition raw response bytes
│           │   ├── range_0.json
│           │   └── range_N.json
│           └── ... (one dir per query)
├── CHANGELOG.md                                      # MODIFY — Stage 1 entry

# Bytebase: github.com/bytebase/bytebase
go.mod                                                # MODIFY — bump fork pseudo-version to Stage 1 head
go.sum                                                # MODIFY — via go mod tidy
backend/plugin/db/cosmosdb/cosmosdb.go                # MODIFY — read BB_COSMOS_DISABLE_QUERY_ENGINE; set QueryOptions.QueryEngine
backend/plugin/db/cosmosdb/cosmosdb_kill_switch_test.go # CREATE — kill-switch env-var test
```

## Conventions

- Commits are grouped by phase (A/B/C). Squash-or-not is reviewer's choice at merge; as authored they tell a linear story.
- Fork commits live on branch `bytebase/cosmos-query-engine-stage-1` (off Stage 0's merge commit, not off `main` directly, until Stage 0 lands — then rebase onto `main`).
- Bytebase commits live on a short-lived branch off `main` in the user's working tree — same process Bytebase uses for any driver change.
- Tests that need the live Azure account are gated by `AZURE_COSMOS_KEY` env var; they skip when absent (matches Stage 0 pattern).

---

# Phase A — wire-format capture and plan JSON model

## Task A1: Capture query plans and per-partition payloads for the 10 target queries

**Files:**
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/testdata/query_<id>_plan.json` (10 files)
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/testdata/query_<id>_partitions/range_<pkrid>.json` (≥1 file per query)

Purpose: we cannot write the parser until we know what the server actually returns. Capture the ground truth *once* and freeze it as test fixtures. Stage 1's correctness is verified against these fixtures; Stage 7 will add live re-verification.

This step is **discovery work against a live Azure account**. The deliverable is the set of fixture files; the exact mechanism to capture them is left to the engineer — three reasonable options:

**Option A (recommended): in-package test with a build tag.** Add a temporary file `sdk/data/azcosmos/capture_plan_fixtures_test.go` (note: in the `azcosmos` package itself, not `queryengine/internal/gonative`, so it can reach the package-private `getQueryPlanFromGateway` / `getPartitionKeyRangesRaw` / `sendCrossPartitionQueryRequest` helpers) with build tag `//go:build capture`. The test iterates the 10 target queries, calls the private helpers with the `x-ms-cosmos-is-query-plan-request=True` header for the plan and per-pk-range queries for the partition data, writes each response body verbatim to `queryengine/internal/gonative/testdata/`. Delete the file after capture — only the fixtures get committed.

**Option B: HTTP via `curl` with a Cosmos auth helper.** Implement the HMAC-SHA256 auth scheme documented at https://learn.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources in a small shell script, run each query twice (once with the plan-request header, once per pk-range). More portable but more auth-header code.

**Option C: capture from the .NET SDK.** The .NET SDK exposes `QueryRequestOptions.WithDiagnosticsHandler`. Modify `scripts/cosmos-repro-dotnet/Program.cs` to dump the raw HTTP request/response bodies for the 10 queries. Copy the captured JSONs into `testdata/`.

Regardless of option, the **output shape is fixed**:

```
sdk/data/azcosmos/queryengine/internal/gonative/testdata/
├── query_5_1_plan.json               # gateway response body when the plan-request header is set
├── query_5_1_pkranges.json           # GET /pkranges response body
├── query_5_1_partitions/
│   ├── range_<pkrid>.json            # full response body of the cross-partition query for that range
│   └── ...
├── query_5_1b_plan.json
├── query_5_1b_pkranges.json
├── query_5_1b_partitions/...
├── ... (8 more query ids: 5_2, 5_2b, 5_3, 5_3b, 5_4, 5_4b, 5_5, 11_2)
```

Requirements:

1. **No secrets in fixtures.** Before commit, run `grep -rE "bytebase-cosmostest\.documents\.azure\.com|AZURE_COSMOS_KEY" sdk/data/azcosmos/queryengine/internal/gonative/testdata/` and scrub anything that matches. Master keys never appear in response bodies, but endpoint hostnames can show up in `_self`/`_rid` links. Replace the hostname with `example.documents.azure.com` if present.
2. **Pretty-print for diff friendliness:** `find sdk/data/azcosmos/queryengine/internal/gonative/testdata -name '*.json' -exec sh -c 'jq . "$1" > "$1.pretty" && mv "$1.pretty" "$1"' _ {} \;`
3. **`11_2` fixture is allowed to be a symlink or duplicate of `5_1b`** (both are `SELECT VALUE COUNT(1) FROM c`). A copy is simpler — the plan is bit-identical.

- [ ] **Step 1: Capture the fixtures using your chosen option.** Verify each file is valid JSON with `jq . <file> > /dev/null`.

- [ ] **Step 2: Scrub any endpoint hostnames / RIDs as described above.**

- [ ] **Step 3: Commit fixtures only (not the capture harness).**

```bash
cd ~/OpenSource/azure-sdk-for-go
rm -f sdk/data/azcosmos/capture_plan_fixtures_test.go   # Option A only
git add sdk/data/azcosmos/queryengine/internal/gonative/testdata/
git commit -m "azcosmos/queryengine: capture Stage 1 aggregate query plans and partition payloads"
```

**Acceptance:** 10 `*_plan.json`, 10 `*_pkranges.json`, ≥10 `partitions/range_*.json` committed. `git grep` after commit finds no hostnames or keys.

## Task A2: Define the plan JSON struct (just what Stage 1 needs)

**Files:**
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/plan.go`
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/plan_test.go`

Purpose: a Go struct that deserializes the plan JSON from Task A1. Stage 1 only needs the aggregate-relevant fields; later stages add their own fields.

- [ ] **Step 1: Write the failing test**

Create `plan_test.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative

import (
    "os"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestParsePlan_5_1_AliasedIsGroupByForm(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_1_plan.json"))
    require.NoError(t, err)
    p, err := parsePlan(raw)
    require.NoError(t, err)
    // 5.1 is `SELECT COUNT(1) AS totalRecords FROM c`.
    // IMPORTANT: aliased aggregates use the groupBy-alias wire format (single-group),
    // NOT the aggregates array. See testdata/query_5_1_plan.json.
    assert.Empty(t, p.QueryInfo.Aggregates, "aliased form leaves aggregates[] empty")
    assert.Equal(t, []string{"totalRecords"}, p.QueryInfo.GroupByAliases)
    assert.Equal(t, map[string]string{"totalRecords": "Count"}, p.QueryInfo.GroupByAliasToAggregateType)
    assert.False(t, p.QueryInfo.HasSelectValue)
    assert.Contains(t, p.QueryInfo.RewrittenQuery, "AS payload",
        "aliased form rewrites to {...} AS payload — our partition parser matches on payload key")
}

func TestParsePlan_5_1b_ValueForm(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_1b_plan.json"))
    require.NoError(t, err)
    p, err := parsePlan(raw)
    require.NoError(t, err)
    // 5.1b is `SELECT VALUE COUNT(1) FROM c` — one aggregate, VALUE form.
    assert.Equal(t, []string{"Count"}, p.QueryInfo.Aggregates)
    assert.True(t, p.QueryInfo.HasSelectValue)
    assert.Empty(t, p.QueryInfo.GroupByAliasToAggregateType, "VALUE form has no aliases")
    assert.Contains(t, p.QueryInfo.RewrittenQuery, `SELECT VALUE [{"item":`,
        "VALUE form rewrites to an array of {item: ...} objects")
}

func TestParsePlan_5_5_MultiAgg_Aliased(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_5_plan.json"))
    require.NoError(t, err)
    p, err := parsePlan(raw)
    require.NoError(t, err)
    // 5.5 has two aliased aggregates — same groupBy form, just two aliases.
    assert.Empty(t, p.QueryInfo.Aggregates)
    assert.ElementsMatch(t, []string{"minPop", "maxPop"}, p.QueryInfo.GroupByAliases)
    assert.Equal(t, map[string]string{"minPop": "Min", "maxPop": "Max"}, p.QueryInfo.GroupByAliasToAggregateType)
    // MIN/MAX aliased use the reliable "item2" partial — see the rewritten query.
    assert.Contains(t, p.QueryInfo.RewrittenQuery, `"item2"`,
        "MIN/MAX aliased include item2: {min|max, count} for cross-partition correctness")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd sdk/data/azcosmos && go test ./queryengine/internal/gonative/... -run "^TestParsePlan" -count=1 -v`
Expected: compile error — `parsePlan`, `planDoc` (or similar) undefined.

- [ ] **Step 3: Implement the plan parser**

Create `plan.go`. Start with the minimal fields observed in the captured fixtures:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative

import (
    "encoding/json"
    "fmt"
)

// planDoc is the subset of the Cosmos query-plan document that Stage 1 uses.
// Fields will be added in later stages as operators come online.
type planDoc struct {
    PartitionedQueryExecutionInfoVersion int         `json:"partitionedQueryExecutionInfoVersion"`
    QueryInfo                            planQueryInfo `json:"queryInfo"`
    QueryRanges                          []queryRange  `json:"queryRanges"`
}

type planQueryInfo struct {
    RewrittenQuery              string            `json:"rewrittenQuery"`
    HasSelectValue              bool              `json:"hasSelectValue"`
    Aggregates                  []string          `json:"aggregates"`
    GroupByAliasToAggregateType map[string]string `json:"groupByAliasToAggregateType"`
    // Fields populated by later stages but decoded defensively so unknown entries don't break parsing.
    DistinctType          string   `json:"distinctType,omitempty"`
    Top                   *int     `json:"top,omitempty"`
    Offset                *int     `json:"offset,omitempty"`
    Limit                 *int     `json:"limit,omitempty"`
    OrderBy               []string `json:"orderBy,omitempty"`
    OrderByExpressions    []string `json:"orderByExpressions,omitempty"`
    GroupByExpressions    []string `json:"groupByExpressions,omitempty"`
    GroupByAliases        []string `json:"groupByAliases,omitempty"`
    HasNonStreamingOrderBy bool    `json:"hasNonStreamingOrderBy,omitempty"`
}

type queryRange struct {
    Min            string `json:"min"`
    Max            string `json:"max"`
    IsMinInclusive bool   `json:"isMinInclusive"`
    IsMaxInclusive bool   `json:"isMaxInclusive"`
}

func parsePlan(raw []byte) (*planDoc, error) {
    var p planDoc
    if err := json.Unmarshal(raw, &p); err != nil {
        return nil, fmt.Errorf("gonative: parse query plan: %w", err)
    }
    return &p, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./queryengine/internal/gonative/... -run "^TestParsePlan" -count=1 -v`
Expected: all three tests PASS. If they don't, the captured fixtures have a field you didn't include — add it.

- [ ] **Step 5: Commit**

```bash
git add sdk/data/azcosmos/queryengine/internal/gonative/plan.go sdk/data/azcosmos/queryengine/internal/gonative/plan_test.go
git commit -m "azcosmos/queryengine: add plan JSON struct and parser"
```

## Task A3: Define the per-partition aggregate wire formats (VALUE and aliased)

**Files:**
- Modify: `sdk/data/azcosmos/queryengine/internal/gonative/plan.go` (append types)
- Modify: `sdk/data/azcosmos/queryengine/internal/gonative/plan_test.go`

Purpose: the per-partition result for an aggregate query comes back as a JSON document array. There are **two distinct shapes** depending on whether the plan used the VALUE form or the aliased (single-group GROUP BY) form. Captured fixtures from Task A1 show both.

### Observed shapes (from testdata/)

**VALUE form** (e.g. `SELECT VALUE COUNT(1) FROM c` → 5.1b, 5.2b, 5.3b, 5.4b, 11.2):
```json
{ "Documents": [ [ {"item": 18} ] ] }
```
- Outer array wraps a single object per document.
- Each doc is a JSON array of 1 element.
- Element is `{"item": <partial>}`.

**Aliased form** (e.g. `SELECT COUNT(1) AS totalRecords FROM c` → 5.1, 5.2, 5.3, 5.4, 5.5):
```json
{ "Documents": [ { "payload": { "totalRecords": { "item": 18 } } } ] }
```
- Doc is an object with a single `payload` key.
- `payload` is an object keyed by alias name.
- Each alias value is `{"item": <partial>}` — and for MIN/MAX, additionally `{"item": <direct>, "item2": {"min"|"max": v, "count": n}}`.

**Partial shapes** (same across both forms):
- `Count`, `Sum`: raw number.
- `Avg`: `{"sum": <num>, "count": <int>}`.
- `Min`: scalar (single-partition short-circuit) or `{"min": <v>, "count": <int>}` (reliable cross-partition).
- `Max`: scalar or `{"max": <v>, "count": <int>}`.

For cross-partition correctness, the finalizer **must prefer `item2`** (reliable partial) if present; `item` alone is only valid when there's a single partition.

- [ ] **Step 1: Write tests that validate both shapes**

Append to `plan_test.go`:

```go
func TestParseValuePartition_CountIsNumeric(t *testing.T) {
    // 5.1b: SELECT VALUE COUNT(1) FROM c
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_1b_partitions", "range_0.json"))
    require.NoError(t, err)
    p, err := parseValuePartitionResponse(raw)
    require.NoError(t, err)
    require.NotEmpty(t, p.Documents, "at least one document expected")
    // Each document is [ {"item": <partial>} ].
    require.Len(t, p.Documents[0], 1)
    var n float64
    require.NoError(t, json.Unmarshal(p.Documents[0][0].Item, &n))
    assert.Positive(t, n)
}

func TestParseValuePartition_AvgHasSumAndCount(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_4b_partitions", "range_0.json"))
    require.NoError(t, err)
    p, err := parseValuePartitionResponse(raw)
    require.NoError(t, err)
    require.NotEmpty(t, p.Documents)
    require.Len(t, p.Documents[0], 1)
    var partial map[string]json.RawMessage
    require.NoError(t, json.Unmarshal(p.Documents[0][0].Item, &partial))
    assert.Contains(t, partial, "sum")
    assert.Contains(t, partial, "count")
}

func TestParseAliasedPartition_Count(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_1_partitions", "range_0.json"))
    require.NoError(t, err)
    p, err := parseAliasedPartitionResponse(raw)
    require.NoError(t, err)
    require.NotEmpty(t, p.Documents)
    // Each document is {"payload": {"<alias>": {"item": <partial>}}}
    alias, ok := p.Documents[0].Payload["totalRecords"]
    require.True(t, ok, "expected totalRecords alias in payload")
    var n float64
    require.NoError(t, json.Unmarshal(alias.Item, &n))
    assert.Positive(t, n)
    assert.Nil(t, alias.Item2, "Count aliased has no item2")
}

func TestParseAliasedPartition_MinMaxHasItem2(t *testing.T) {
    raw, err := os.ReadFile(filepath.Join("testdata", "query_5_5_partitions", "range_0.json"))
    require.NoError(t, err)
    p, err := parseAliasedPartitionResponse(raw)
    require.NoError(t, err)
    require.NotEmpty(t, p.Documents)
    minPop, ok := p.Documents[0].Payload["minPop"]
    require.True(t, ok)
    require.NotNil(t, minPop.Item2, "Min aliased MUST carry item2 for cross-partition correctness")
    var reliable struct {
        Min   json.RawMessage `json:"min"`
        Count int             `json:"count"`
    }
    require.NoError(t, json.Unmarshal(minPop.Item2, &reliable))
    assert.NotZero(t, reliable.Count)
}
```

- [ ] **Step 2: Implement the two parsers**

Append to `plan.go`:

```go
// aliasedItem is the shape of each alias's value in an aliased-form partition document.
// Item is always present; Item2 is populated for MIN/MAX as the reliable cross-partition partial.
type aliasedItem struct {
    Item  json.RawMessage `json:"item"`
    Item2 json.RawMessage `json:"item2,omitempty"`
}

// aliasedDoc is one entry of Documents for an aliased aggregate query.
type aliasedDoc struct {
    Payload map[string]aliasedItem `json:"payload"`
}

// aliasedPartitionResponse is the decoded body of an aliased-form per-partition response.
type aliasedPartitionResponse struct {
    Documents []aliasedDoc `json:"Documents"`
}

func parseAliasedPartitionResponse(raw []byte) (*aliasedPartitionResponse, error) {
    var r aliasedPartitionResponse
    if err := json.Unmarshal(raw, &r); err != nil {
        return nil, fmt.Errorf("gonative: parse aliased partition response: %w", err)
    }
    return &r, nil
}

// valueItem is the shape inside each Documents element for a VALUE-form aggregate query.
type valueItem struct {
    Item json.RawMessage `json:"item"`
}

// valuePartitionResponse decodes the Documents array where each entry is itself an array
// of valueItem (gateway wraps VALUE-form results this way).
type valuePartitionResponse struct {
    Documents [][]valueItem `json:"Documents"`
}

func parseValuePartitionResponse(raw []byte) (*valuePartitionResponse, error) {
    var r valuePartitionResponse
    if err := json.Unmarshal(raw, &r); err != nil {
        return nil, fmt.Errorf("gonative: parse value partition response: %w", err)
    }
    return &r, nil
}
```

- [ ] **Step 3: Run tests, commit**

```bash
go test ./queryengine/internal/gonative/... -run "^TestParse" -count=1 -v
# all PASS
git add sdk/data/azcosmos/queryengine/internal/gonative/plan.go sdk/data/azcosmos/queryengine/internal/gonative/plan_test.go
git commit -m "azcosmos/queryengine: model per-partition aggregate response shape"
```

---

# Phase B — aggregate operator

## Task B1: Implement per-aggregate finalizers

**Files:**
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/agg.go`
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/agg_test.go`

Purpose: the finalizer is the pure math — given a slice of per-partition partial values for a single aggregate, return the final scalar. One finalizer per aggregate type.

- [ ] **Step 1: Write failing tests per aggregate type**

Create `agg_test.go`. For each of COUNT, SUM, MIN, MAX, AVG, write a table-driven test that feeds in hand-crafted partials and asserts the finalized value. Include edge cases: zero partials, single partial, partials with `count=0` (empty partition), and mixed-type MIN/MAX. Use the captured fixtures' numbers as realism where possible.

Example for COUNT:

```go
func TestCountFinalizer(t *testing.T) {
    cases := []struct{
        name string
        partials []json.RawMessage
        want float64
    }{
        {"empty",          nil,                                        0},
        {"single",         []json.RawMessage{[]byte(`5`)},             5},
        {"three",          []json.RawMessage{[]byte(`5`), []byte(`0`), []byte(`13`)}, 18},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            a := newAggregator("Count")
            require.NotNil(t, a)
            for _, p := range tc.partials {
                require.NoError(t, a.feedPartial(p))
            }
            got, err := a.finalize()
            require.NoError(t, err)
            assert.EqualValues(t, tc.want, got)
        })
    }
}
```

Provide equivalent tests for `Sum`, `Min`, `Max`, `Avg`. Important:

- **Count/Sum partials are bare numbers** — e.g. `5`, `20442258`.
- **Avg partials are `{"sum": s, "count": n}`** — the finalizer computes `(Σs) / (Σn)`; return an empty result when `Σn == 0`.
- **Min/Max partials: pass `item2` when present, the direct `item` when not.** The aggregate operator is responsible for picking the right partial; the finalizer just receives whatever partial shape it expects (`{"min"|"max": v, "count": n}` for reliable form). Test both: feed multi-partition `item2` partials and verify correctness; feed single-partition direct scalars and verify correctness.
- **Test cross-partition merges with mixed types** (null < number < string per Cosmos ordering).

Include a **synthesized multi-partition test** for each aggregator: duplicate the single-range captured partial into 3 different "partitions" with different values; assert the finalized result is what multi-partition math dictates.

- [ ] **Step 2: Implement `agg.go` + `cmp.go`**

Top-level interface:

```go
type aggregator interface {
    feedPartial(raw json.RawMessage) error
    finalize() (json.RawMessage, error)
}

func newAggregator(kind string) aggregator {
    switch kind {
    case "Count":
        return &countAgg{}
    case "Sum":
        return &sumAgg{}
    case "Min":
        return &minAgg{}
    case "Max":
        return &maxAgg{}
    case "Average", "Avg":  // gateway may emit either; accept both
        return &avgAgg{}
    default:
        return nil
    }
}
```

Implement each struct with `feedPartial` decoding the partial shape, and `finalize` producing a `json.RawMessage` scalar (`1234` or `"AE"` or `null`). Returning RawMessage avoids extra marshaling in the caller.

Pull the typed comparator into its own file `cmp.go` — it will be reused by Stage 3 (ORDER BY). Cosmos item ordering: `undefined < null < bool < number < string`.

- [ ] **Step 3: Run tests, commit**

```bash
go test ./queryengine/internal/gonative/... -run "^Test.*Finalizer|^TestNewAggregator" -count=1 -v
# all PASS
git add sdk/data/azcosmos/queryengine/internal/gonative/agg.go sdk/data/azcosmos/queryengine/internal/gonative/agg_test.go
git commit -m "azcosmos/queryengine: add per-aggregate finalizers (Count, Sum, Min, Max, Avg)"
```

## Task B2: Implement the aggregate `QueryPipeline`

**Files:**
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/pipeline.go`
- Create: `sdk/data/azcosmos/queryengine/internal/gonative/pipeline_test.go`

Purpose: implement `queryengine.QueryPipeline` for the aggregate case. There are **two pipeline flavors** that share the same underlying aggregator state vector but differ in how they parse per-partition bodies and emit the final row:

- **`valuePipeline`** — plan has `aggregates: [...]` and `hasSelectValue: true`. Each partition's `Documents` is `[[{"item": <partial>}]]`. Final emit is a single scalar row: `<finalized value>`.
- **`aliasedPipeline`** — plan has `groupByAliases: [...]` and `groupByAliasToAggregateType: {...}` (and `aggregates: []`). Each partition's `Documents` is `[{"payload": {"<alias>": {"item": ..., "item2": ...}}}]`. Final emit wraps the finalized scalars back into `{"<alias>": <v>, ...}` in the original alias order (take from `groupByAliases` to preserve order).

Both pipelines expose the same interface:
- `Query()` returns the plan's rewritten per-partition query.
- `Run()` on first call emits one partition request per pk-range. Subsequent calls return nothing until `ProvideData` has been called for every partition; then emits the single final row and sets `completed = true`.
- `ProvideData()` routes each partition's partial into the right aggregator slot — for aliased, one slot per alias; for value-form, one slot per element in `aggregates`.
- `IsComplete()` flips true after the final row is emitted.
- `Close()` is a no-op.

For MIN/MAX in the aliased form, pipeline logic: prefer `aliasedItem.Item2` (the reliable partial with `count`) when present; fall back to wrapping `aliasedItem.Item` as a single-partition partial (`{"min"|"max": Item, "count": 1}`) when Item2 is nil. This handles cross-partition and single-partition cases uniformly.

- [ ] **Step 1: Write failing end-to-end pipeline tests**

Create `pipeline_test.go` with one `TestAggPipeline_<queryId>` per captured fixture (10 total). Each test:

```go
func TestAggPipeline_5_1(t *testing.T) {
    plan := mustReadFile(t, "testdata/query_5_1_plan.json")
    pkranges := mustReadFile(t, "testdata/query_5_1_pkranges.json")
    partitionBodies := mustReadPartitionDir(t, "testdata/query_5_1_partitions")

    engine := gonative.Default()
    pipe, err := engine.CreateQueryPipeline(`SELECT COUNT(1) AS totalRecords FROM c`, string(plan), string(pkranges))
    require.NoError(t, err)
    defer pipe.Close()

    // Drive the pipeline: Run() -> feed -> Run() until IsComplete.
    for !pipe.IsComplete() {
        res, err := pipe.Run()
        require.NoError(t, err)
        if len(res.Requests) > 0 {
            // Feed each request with its captured partition body.
            var results []queryengine.QueryResult
            for _, req := range res.Requests {
                body := partitionBodies[req.PartitionKeyRangeID]
                results = append(results, queryengine.NewQueryResult(req.PartitionKeyRangeID, body, ""))
            }
            require.NoError(t, pipe.ProvideData(results))
        }
        if len(res.Items) > 0 {
            // Validate: single row containing {"totalRecords": 18}.
            require.Len(t, res.Items, 1)
            var row map[string]any
            require.NoError(t, json.Unmarshal(res.Items[0], &row))
            assert.EqualValues(t, 18, row["totalRecords"])
        }
    }
}
```

Replicate the structure for each of the 10 queries. The expected values for each come from the `.NET SDK column` of `target-queries.md`.

- [ ] **Step 2: Implement the two pipelines + dispatch**

Shared structure in `pipeline.go`:

```go
// aggPipelineBase holds state common to both value and aliased pipelines.
type aggPipelineBase struct {
    rewrittenQuery string
    pkRangeIDs     []string
    aggregators    []aggregator       // len matches number of emitted aggregates
    partitionsDone map[string]bool
    issued         bool
    emitted        bool
}

// valuePipeline handles plans with `aggregates: [...]` and `hasSelectValue: true`.
// Emits a single scalar (or array) row — whatever the aggregate(s) finalize to.
type valuePipeline struct {
    aggPipelineBase
    kinds []string  // one per aggregate; positional match with aggregators[]
}

// aliasedPipeline handles plans with `groupByAliases: [...]` + `groupByAliasToAggregateType: {...}`.
// Emits one row: {"<alias1>": <v1>, "<alias2>": <v2>, ...} in original alias order.
type aliasedPipeline struct {
    aggPipelineBase
    aliases []string            // preserves order from queryInfo.groupByAliases
    kinds   []string             // positional: aggregate kind per alias
}
```

Method skeletons (same shape for both):
- `Query()` → `base.rewrittenQuery`
- `IsComplete()` → `base.emitted`
- `Close()` → no-op
- `Run()`:
  - If not `issued`: emit one `queryengine.QueryRequest` per pk-range with `Query = rewrittenQuery`. Set `issued = true`.
  - Else if all pk-ranges in `partitionsDone`: finalize aggregators, emit one item, set `emitted = true`.
  - Else: return empty — SDK waits for `ProvideData`.
- `ProvideData(results)`:
  - For each `QueryResult`, decode using the flavor-specific parser (`parseValuePartitionResponse` vs `parseAliasedPartitionResponse`).
  - Route each document's partials into the aggregator vector.
  - Mark the partition done.

**Aliased MIN/MAX partial selection**: when `aliasedItem.Item2 != nil`, feed `Item2` directly into the aggregator (`{"min"|"max": v, "count": n}` shape). When `Item2 == nil`, synthesize `{"min"|"max": Item, "count": 1}`.

**Dispatch in `engine.go`** (replace the Stage 0 stub):

```go
func (e *Engine) CreateQueryPipeline(query, plan, pkranges string) (queryengine.QueryPipeline, error) {
    p, err := parsePlan([]byte(plan))
    if err != nil {
        return nil, err
    }
    // Reject any plan feature we don't yet support in Stage 1.
    if p.QueryInfo.DistinctType != "" && p.QueryInfo.DistinctType != "None" {
        return nil, queryengine.ErrUnsupportedPlanFeature
    }
    if len(p.QueryInfo.OrderBy) > 0 || len(p.QueryInfo.GroupByExpressions) > 0 {
        return nil, queryengine.ErrUnsupportedPlanFeature
    }
    if p.QueryInfo.Top != nil || p.QueryInfo.Offset != nil || p.QueryInfo.Limit != nil {
        return nil, queryengine.ErrUnsupportedPlanFeature
    }
    if p.QueryInfo.HasNonStreamingOrderBy {
        return nil, queryengine.ErrUnsupportedPlanFeature
    }
    ranges, err := parsePKRangeIDs([]byte(pkranges))
    if err != nil {
        return nil, err
    }
    // VALUE form: aggregates is non-empty.
    if len(p.QueryInfo.Aggregates) > 0 {
        return newValuePipeline(p, ranges), nil
    }
    // Aliased form: aggregates empty, groupByAliasToAggregateType non-empty.
    if len(p.QueryInfo.GroupByAliasToAggregateType) > 0 {
        return newAliasedPipeline(p, ranges), nil
    }
    return nil, queryengine.ErrUnsupportedPlanFeature
}
```

`SupportedFeatures()` returns `"Aggregate,NonValueAggregate,MultipleAggregates"` — these three features together advertise what Stage 1 can actually handle (`NonValueAggregate` is the gateway's name for the aliased form; `MultipleAggregates` enables query 5.5's MIN+MAX in one SELECT).

- [ ] **Step 3: Run tests, commit**

```bash
go test -race ./queryengine/internal/gonative/... -count=1 -v
# all 10 TestAggPipeline_* tests PASS; plan parse tests still PASS
git add sdk/data/azcosmos/queryengine/internal/gonative/pipeline.go sdk/data/azcosmos/queryengine/internal/gonative/pipeline_test.go sdk/data/azcosmos/queryengine/internal/gonative/engine.go
git commit -m "azcosmos/queryengine: implement aggregate QueryPipeline and dispatch from CreateQueryPipeline"
```

## Task B3: Wire `SupportedFeatures` to "Aggregate"

Already done as part of B2's `engine.go` edit. Verify with:

- [ ] **Verification**

```bash
cd sdk/data/azcosmos && go test ./queryengine/... -count=1
```

Expected: every package green.

---

# Phase C — activation + Bytebase integration

## Task C1: Expose the engine path from `NewCrossPartitionQueryItemsPager` via engage-on-error

**Files:**
- Modify: `sdk/data/azcosmos/cosmos_container.go`
- Modify: `sdk/data/azcosmos/cosmos_container_query_engine.go`
- Modify: existing tests will need a skipping pattern; add a new test file:
- Create: `sdk/data/azcosmos/cosmos_container_engage_on_error_test.go`

Purpose: when `NewCrossPartitionQueryItemsPager` gets the specific `BadRequest`: `"cross partition query can not be directly served by the gateway"`, switch to the engine path. When it gets any other error, propagate as today.

- [ ] **Step 1: Write failing test with mock gateway responses**

Use the fork's existing mock infrastructure (`internal/mock`). Write a test where:

1. The first gateway call returns a crafted 400 response matching the real cross-partition-unsupported error.
2. Engine path then activates; the mock gateway returns a plan (use the captured `query_5_1_plan.json`) and per-partition payloads.
3. The pager emits the final aggregated row.

Assertion: pager.NextPage() returns the single row with `totalRecords: 18`.

- [ ] **Step 2: Refactor `NewCrossPartitionQueryItemsPager`**

Change the Fetcher closure to:
1. Before issuing the request, check `queryOptions.QueryEngine`. If `queryengine.Disabled`, always use the raw path (current behavior). If nil, continue to step 2.
2. Issue the raw cross-partition request.
3. If response error is a 400 with `"cross partition query can not be directly served by the gateway"` in the body, clear the error and pivot into `executeQueryWithEngine` path. Use `gonative.Default()` if no engine was provided. Cache the plan in `container.planCache` keyed by `(databaseID, containerID, query, engine.SupportedFeatures())`.
4. If response error is anything else, propagate.

Important: the pivot must preserve `ContinuationToken` semantics. Since the raw path never got past page-0, continuation is from fresh — no special handling needed.

- [ ] **Step 3: Verify and commit**

```bash
go test -race ./... -count=1
git add sdk/data/azcosmos/cosmos_container.go sdk/data/azcosmos/cosmos_container_query_engine.go sdk/data/azcosmos/cosmos_container_engage_on_error_test.go
git commit -m "azcosmos: engage-on-error activation for cross-partition queries (default engine)"
```

## Task C2: Wire plan cache into `getQueryPlanFromGateway`

**Files:**
- Modify: `sdk/data/azcosmos/cosmos_container_query_engine.go`
- Create: `sdk/data/azcosmos/cosmos_container_plan_cache_wiring_test.go`

Purpose: avoid re-fetching the query plan from the gateway for repeated queries within the same `ContainerClient`.

- [ ] **Step 1: Test**

Write a test that drives the engine path twice with the same query; assert the gateway plan-fetch endpoint is called only once (use a mock that counts calls).

- [ ] **Step 2: Implement**

In `executeQueryWithEngine`, replace the direct call to `getQueryPlanFromGateway` with:

```go
key := planCacheKey{
    databaseID:      c.database.id,
    containerID:     c.id,
    normalizedQuery: query,
    features:        queryEngine.SupportedFeatures(),
}
var plan []byte
if c.planCache != nil {
    if cached, ok := c.planCache.Get(key); ok {
        plan = cached
    }
}
if plan == nil {
    plan, err = c.getQueryPlanFromGateway(ctx, query, queryEngine.SupportedFeatures(), queryOptions, operationContext)
    if err != nil { return ... }
    if c.planCache != nil {
        c.planCache.Add(key, plan)
    }
}
```

- [ ] **Step 3: Run tests, commit**

```bash
go test -race ./... -count=1
git add sdk/data/azcosmos/cosmos_container_query_engine.go sdk/data/azcosmos/cosmos_container_plan_cache_wiring_test.go
git commit -m "azcosmos: use per-container plan cache around getQueryPlanFromGateway"
```

## Task C3: Update CHANGELOG + push + open fork PR

- [ ] **Step 1: Add CHANGELOG entries**

Under `1.5.0-beta.6 (Unreleased)` → `### Features Added`, append:

```markdown
* Added pure-Go distributed query engine with scalar aggregate support (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`), including multi-aggregate and `VALUE` + aliased forms. Engage-on-error: triggered only when the gateway returns the cross-partition-unsupported `BadRequest`. See BYT-9239.
* Per-container query plan cache now populated and consulted during engine activation.
```

- [ ] **Step 2: Commit, push, open PR**

```bash
cd ~/OpenSource/azure-sdk-for-go
git add sdk/data/azcosmos/CHANGELOG.md
git commit -m "azcosmos: changelog entry for Stage 1 scalar-aggregate support"
git push -u origin bytebase/cosmos-query-engine-stage-1
gh pr create --title "azcosmos: Stage 1 — scalar aggregates + engage-on-error activation (BYT-9239)" --body "$(cat <<'EOF'
Part 2 of 7 for [BYT-9239](https://linear.app/bytebase/issue/BYT-9239). Builds on Stage 0 (PR #2).

## What unlocks

Ten queries previously failing on the Go SDK now pass through the new query engine, matching the .NET reference:

| # | Query | Was | Now |
|---|---|---|---|
| 5.1  | `SELECT COUNT(1) AS totalRecords FROM c` | FAIL | PASS |
| 5.1b | `SELECT VALUE COUNT(1) FROM c` | FAIL | PASS |
| 5.2  | `SELECT COUNT(1) AS aeCount FROM c WHERE c.country = "AE"` | FAIL | PASS |
| 5.2b | `SELECT VALUE COUNT(1) FROM c WHERE c.country = "AE"` | FAIL | PASS |
| 5.3  | `SELECT SUM(...) AS totalPop FROM c` | FAIL | PASS |
| 5.3b | `SELECT VALUE SUM(...) FROM c` | FAIL | PASS |
| 5.4  | `SELECT AVG(...) AS avgPop FROM c` | FAIL | PASS |
| 5.4b | `SELECT VALUE AVG(...) FROM c` | FAIL | PASS |
| 5.5  | `SELECT MIN(...) AS minPop, MAX(...) AS maxPop FROM c WHERE ...` | FAIL | PASS |
| 11.2 | `SELECT VALUE COUNT(1) FROM c` | FAIL | PASS |

## Activation — engage-on-error, not auto

`NewCrossPartitionQueryItemsPager` still tries the gateway first. Only on the specific `BadRequest: cross partition query can not be directly served by the gateway` does it pivot into the engine path. Queries that succeed today continue to take the same code path and round-trip count as before — no regression.

## Test plan
- [x] `go test -race ./sdk/data/azcosmos/... -count=1 -timeout 180s` PASS
- [x] All 10 target queries verified against captured fixtures via `TestAggPipeline_*`
- [x] Engage-on-error path verified via `TestCrossPartitionEngageOnError`
- [x] Plan cache hit/miss verified
- [ ] Reviewer: confirm `SupportedFeatures()` advertises only `Aggregate`; any other feature string means later stages accidentally enabled themselves early.

Bytebase-side `go.mod` bump + kill-switch env var are in a separate PR against `bytebase/bytebase`. This fork PR is safe to merge independently — it introduces no new behavior until a caller explicitly uses the engine.
EOF
)"
```

## Task C4: Bytebase `go.mod` bump + kill-switch env var

**Files:**
- Modify (Bytebase): `go.mod`
- Modify (Bytebase): `go.sum`
- Modify (Bytebase): `backend/plugin/db/cosmosdb/cosmosdb.go`
- Create (Bytebase): `backend/plugin/db/cosmosdb/cosmosdb_kill_switch_test.go`

Purpose: pick up the new fork on the Bytebase side, and give operators a runtime escape hatch.

- [ ] **Step 1: Bump `go.mod` in Bytebase**

```bash
cd ~/OpenSource/bytebase
git checkout -b bytebase/cosmos-query-engine-stage-1-downstream main
# Capture the Stage 1 head SHA from the fork and form the pseudo-version:
SHA=$(cd ~/OpenSource/azure-sdk-for-go && git rev-parse --short=12 HEAD)
TS=$(cd ~/OpenSource/azure-sdk-for-go && git show -s --format=%cd --date=format-local:%Y%m%d%H%M%S HEAD)
go mod edit -replace github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos=github.com/bytebase/azure-sdk-for-go/sdk/data/azcosmos@v0.0.0-$TS-$SHA
go mod tidy
```

- [ ] **Step 2: Add kill-switch env var**

Edit `backend/plugin/db/cosmosdb/cosmosdb.go` in `QueryConn`, between container validation and pager construction (around line 170):

```go
if os.Getenv("BB_COSMOS_DISABLE_QUERY_ENGINE") != "" {
    queryOption = ensureQueryOption(queryOption)
    queryOption.QueryEngine = queryengine.Disabled
}
```

Add a helper `ensureQueryOption` that returns a non-nil `*azcosmos.QueryOptions`. Add `os` and `queryengine` imports.

- [ ] **Step 3: Test the kill switch**

Write a unit test that stubs a fake engine; with `BB_COSMOS_DISABLE_QUERY_ENGINE=1` set, assert the driver passes `queryengine.Disabled` through to the pager.

- [ ] **Step 4: Verify + commit + push**

```bash
cd ~/OpenSource/bytebase
go build ./backend/...
go test ./backend/plugin/db/cosmosdb/... -count=1
git add go.mod go.sum backend/plugin/db/cosmosdb/cosmosdb.go backend/plugin/db/cosmosdb/cosmosdb_kill_switch_test.go
git commit -m "fix(cosmosdb): pick up query engine SDK and add BB_COSMOS_DISABLE_QUERY_ENGINE kill switch [BYT-9239]"
git push -u origin bytebase/cosmos-query-engine-stage-1-downstream
gh pr create --title "fix(cosmosdb): pick up query engine SDK and add kill switch [BYT-9239]" --body "$(cat <<'EOF'
## Summary
- Bumps the forked azcosmos SDK to pick up the Stage 1 scalar-aggregate query engine (see `bytebase/azure-sdk-for-go` Stage 1 PR).
- Adds `BB_COSMOS_DISABLE_QUERY_ENGINE` env var: when set, the driver passes `queryengine.Disabled` via `QueryOptions.QueryEngine`, reverting to pre-BYT-9239 behavior as an operational escape hatch.
- Stage 1 unlocks these Cosmos queries end-to-end in the SQL viewer: 5.1, 5.1b, 5.2, 5.2b, 5.3, 5.3b, 5.4, 5.4b, 5.5, 11.2. See [BYT-9239](https://linear.app/bytebase/issue/BYT-9239).

## Test plan
- [x] `go build ./backend/...`
- [x] `go test ./backend/plugin/db/cosmosdb/... -count=1`
- [ ] Manual verification against Azure test account: the 10 Stage 1 queries now return correct results via Bytebase SQL viewer.
- [ ] Manual verification: with `BB_COSMOS_DISABLE_QUERY_ENGINE=1`, Bytebase behaves exactly as before this PR.
EOF
)"
```

## Task C5: End-to-end verification

- [ ] **Step 1: Run the .NET repro harness (baseline) and the Bytebase Cosmos driver (after SDK bump) against the Azure test account.**

Compare: each of 5.1, 5.1b, 5.2, 5.2b, 5.3, 5.3b, 5.4, 5.4b, 5.5, 11.2 now returns the same result via Bytebase as via .NET.

Document the measured pass rate in the fork's PR description.

- [ ] **Step 2: Run the Bytebase Cosmos integration test harness (if it exists) end-to-end with engine disabled (`BB_COSMOS_DISABLE_QUERY_ENGINE=1`) and confirm behavior matches pre-stage-1 exactly. Then with the env var unset and the 10 queries that Stage 1 unlocks.**

## Task C6: Final cross-repo review

Dispatch `superpowers:code-reviewer` against the combined diff of both PRs (fork Stage 1 + Bytebase downstream), framed as "does this stage deliver parity on the 10 target queries, does the activation pattern correctly bypass today's working queries, and does the kill switch actually disable the engine?"

---

## Self-review checklist

- [ ] **Spec coverage.** Spec § Stage 1 describes aggregate finalizers (Task B1), `VALUE` + aliased forms (B2), multi-aggregate (B2), engage-on-error (C1), plan cache use (C2), Bytebase kill switch (C4). Query 11.2 (same as 5.1b) is covered by the dedup note in Task A1.
- [ ] **Placeholder scan.** No "TBD" / "TODO" / "handle edge cases" / "fill in later". Tasks A2/A3/B1 explicitly note that the partial-shape assertions depend on captured fixtures — that is discovery work, not a placeholder.
- [ ] **Type / identifier consistency.**
  - `planDoc`, `planQueryInfo`, `queryRange` (Task A2) used by `parsePlan` and later by `CreateQueryPipeline`.
  - `aggregator` interface with `feedPartial(json.RawMessage) error` and `finalize() (any, error)` methods (B1) used by `aggPipeline` (B2).
  - `planCacheKey` fields `databaseID`, `containerID`, `normalizedQuery`, `features` match Stage 0's struct exactly (C2).
  - `queryengine.Disabled` sentinel (Stage 0 delivery) used by C4 Bytebase kill switch.
  - `BB_COSMOS_DISABLE_QUERY_ENGINE` env var name used consistently.
- [ ] **Path correctness.** Fork paths `sdk/data/azcosmos/...`; Bytebase paths `backend/plugin/db/cosmosdb/...`, `go.mod`. Plan file itself lives at `docs/superpowers/plans/2026-04-23-cosmos-query-engine-stage-1.md`.
- [ ] **Granularity.** Phase A tasks are discovery-shaped — capture, observe, model. Phase B is TDD for logic. Phase C is integration + deploy. Each step within a task is 2-5 minutes; exceptions (running the live capture against Azure) are called out.
- [ ] **Cross-repo coupling.** Bytebase PR depends on the fork PR landing first; the bump step uses the merged fork head SHA. C5 verification closes the loop.

## What Stage 2 depends on from Stage 1

- `planDoc` / `planQueryInfo` types (Task A2) — Stage 2 adds `DistinctType` reading (already modeled here).
- `aggregator` interface (B1) — Stage 6 (GROUP BY) reuses the same finalizers.
- `cmp.go` typed comparator extracted during B1 — Stage 3 (ORDER BY) uses it directly.
- Engage-on-error activation pattern (C1) — every subsequent stage extends `CreateQueryPipeline`'s dispatch but the pager wrapping stays.
- Plan cache wiring (C2) — unchanged for later stages.
- Bytebase kill switch (C4) — unchanged; continues to disable regardless of feature set.

---
