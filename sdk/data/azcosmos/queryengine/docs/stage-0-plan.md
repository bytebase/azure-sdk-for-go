# Cosmos Query Engine — Stage 0 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the plumbing for the pure-Go distributed query engine inside the Bytebase fork of `azcosmos` as a reviewable, zero-behavior-change PR: sentinels, the `gonative` package skeleton with an inert default engine, the plan cache on `ContainerClient`, and a configurable cache size — all without activating the engine yet.

**Architecture:** All changes land in the fork `github.com/bytebase/azure-sdk-for-go/sdk/data/azcosmos`. The new `queryengine/gonative/` sub-package holds the implementation; `queryengine` itself gets two sentinels (`Disabled`, `ErrUnsupportedPlanFeature`). `ContainerClient` gains an LRU plan cache field that is allocated but unused in Stage 0; `ClientOptions` gains a `QueryPlanCacheSize` knob. No change to any public API signature. `NewCrossPartitionQueryItemsPager` is deliberately untouched — activation and fallback are Stage 1 territory.

**Tech Stack:** Go 1.21+; `github.com/hashicorp/golang-lru/v2` (already a transitive dep in Bytebase; fork depends on stdlib only today — we add the LRU dep); standard `testing` + `testify` (already used in the fork's tests).

**Reference spec:** [`docs/superpowers/specs/2026-04-22-cosmos-cross-partition-query-engine-design.md`](../specs/2026-04-22-cosmos-cross-partition-query-engine-design.md)

**Authoritative target query list:** [`./target-queries.md`](./target-queries.md)

---

## Out of scope for Stage 0 (deferred to Stage 1)

- Auto-enabling the engine in `NewCrossPartitionQueryItemsPager`.
- Engage-on-error activation (the Codex-recommended strategy — chosen for Stage 1).
- Bytebase-side kill switch (env var propagation to `QueryOptions.QueryEngine = queryengine.Disabled`).
- Any query-operator logic (aggregates, DISTINCT, etc.).
- `go.mod` bump in `bytebase/bytebase` (no value until Stage 1 ships a first operator).

## File map

```
# Fork: github.com/bytebase/azure-sdk-for-go
sdk/data/azcosmos/
├── queryengine/
│   ├── cosmos_query_engine.go          # MODIFY — add Disabled, ErrUnsupportedPlanFeature
│   ├── cosmos_query_engine_test.go     # CREATE — sentinel tests
│   └── internal/
│       └── gonative/                   # CREATE (new sub-package, Go-internal)
│           ├── doc.go                  # CREATE
│           ├── engine.go               # CREATE — Default() + inert engine
│           └── engine_test.go          # CREATE
├── cosmos_client_options.go            # MODIFY — add QueryPlanCacheSize
├── cosmos_container.go                 # MODIFY — add planCache field
├── cosmos_container_plan_cache_test.go # CREATE — plan cache tests
├── go.mod                              # MODIFY — add golang-lru/v2 dep
├── go.sum                              # MODIFY — via `go mod tidy`
└── CHANGELOG.md                        # MODIFY — entry for Stage 0
```

## Conventions used throughout

- All Go code uses the existing fork's import ordering (stdlib → azure-sdk-core → azcosmos internal → queryengine).
- Tests use standard `testing` with `require`/`assert` from `github.com/stretchr/testify` — the fork already uses testify per `cosmos_client_retry_policy_test.go` (check that the module has it; if not, add it).
- Commit messages use the Azure SDK for Go prefix style the fork follows (e.g. `azcosmos: <what>`). One commit per task when the task has a testable deliverable; "prep" tasks may share commits.

---

## Task 1: Prepare fork development environment

**Files:**
- Clone: `github.com/bytebase/azure-sdk-for-go` → a local working directory the engineer controls
- Branch: `bytebase/cosmos-query-engine-stage-0`

- [ ] **Step 1: Clone the fork and create a feature branch**

```bash
# Pick a working directory OUTSIDE the Bytebase repo tree
cd ~/OpenSource
git clone git@github.com:bytebase/azure-sdk-for-go.git
cd azure-sdk-for-go
git checkout -b bytebase/cosmos-query-engine-stage-0
```

- [ ] **Step 2: Confirm current test baseline is green on this machine**

Run: `cd sdk/data/azcosmos && go test ./... -count=1`
Expected: All tests PASS. (Cosmos emulator-dependent tests are guarded by build tags and will skip if no emulator is running; that is fine.)

- [ ] **Step 3: Confirm the branch tracks the upstream fork**

Run: `git status` and `git log --oneline -3`
Expected: Clean working tree; last commit is `5b89f0889452` (the pseudo-version Bytebase currently pins to) or a descendant.

No commit in this task.

---

## Task 2: Add `Disabled` sentinel to the `queryengine` package

**Files:**
- Modify: `sdk/data/azcosmos/queryengine/cosmos_query_engine.go`
- Create: `sdk/data/azcosmos/queryengine/cosmos_query_engine_test.go`

Purpose: provide callers a way to explicitly opt out of the default engine that Stage 1+ will start auto-wiring.

- [ ] **Step 1: Write the failing test**

Create `sdk/data/azcosmos/queryengine/cosmos_query_engine_test.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package queryengine_test

import (
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisabledIsNonNilSentinel(t *testing.T) {
	// The Disabled sentinel must be non-nil so callers can distinguish
	// "no engine specified" (nil) from "explicitly disabled".
	require.NotNil(t, queryengine.Disabled)
}

func TestDisabledAdvertisesNoFeatures(t *testing.T) {
	assert.Equal(t, "", queryengine.Disabled.SupportedFeatures())
}

func TestDisabledCreateQueryPipelineReturnsUnsupported(t *testing.T) {
	_, err := queryengine.Disabled.CreateQueryPipeline("SELECT 1", "{}", "{}")
	require.Error(t, err)
	assert.True(t, errors.Is(err, queryengine.ErrUnsupportedPlanFeature),
		"Disabled.CreateQueryPipeline should return ErrUnsupportedPlanFeature; got %v", err)
}

func TestDisabledCreateReadManyPipelineReturnsUnsupported(t *testing.T) {
	_, err := queryengine.Disabled.CreateReadManyPipeline(nil, "{}", "", 0, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, queryengine.ErrUnsupportedPlanFeature))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd sdk/data/azcosmos/queryengine && go test -run "^TestDisabled" -count=1 -v`
Expected: compilation error — `Disabled` and `ErrUnsupportedPlanFeature` undefined.

- [ ] **Step 3: Implement the sentinels**

Edit `sdk/data/azcosmos/queryengine/cosmos_query_engine.go`. Append after the existing `QueryEngine` interface definition:

```go
// ErrUnsupportedPlanFeature is returned by a QueryEngine implementation when
// the query plan references a feature the engine has not implemented.
// Callers receiving this should treat it as "this engine cannot serve this query"
// and surface the original gateway error if any.
var ErrUnsupportedPlanFeature = errors.New("azcosmos/queryengine: unsupported plan feature")

// Disabled is a QueryEngine sentinel callers can use to explicitly opt out of
// any default engine the SDK might install. Every method on Disabled returns
// ErrUnsupportedPlanFeature.
var Disabled QueryEngine = disabledEngine{}

type disabledEngine struct{}

func (disabledEngine) SupportedFeatures() string {
	return ""
}

func (disabledEngine) CreateQueryPipeline(_ string, _ string, _ string) (QueryPipeline, error) {
	return nil, ErrUnsupportedPlanFeature
}

func (disabledEngine) CreateReadManyPipeline(_ []ItemIdentity, _ string, _ string, _ uint8, _ []string) (QueryPipeline, error) {
	return nil, ErrUnsupportedPlanFeature
}
```

Add `"errors"` to the imports at the top of the file.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd sdk/data/azcosmos/queryengine && go test -run "^TestDisabled" -count=1 -v`
Expected: all four tests PASS.

- [ ] **Step 5: Run the full package test to guard against unrelated regressions**

Run: `cd sdk/data/azcosmos && go test ./queryengine/... -count=1`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add sdk/data/azcosmos/queryengine/cosmos_query_engine.go \
        sdk/data/azcosmos/queryengine/cosmos_query_engine_test.go
git commit -m "azcosmos: add Disabled sentinel and ErrUnsupportedPlanFeature to queryengine"
```

---

## Task 3: Create the `gonative` package skeleton

**Files:**
- Create: `sdk/data/azcosmos/queryengine/gonative/doc.go`
- Create: `sdk/data/azcosmos/queryengine/gonative/engine.go`
- Create: `sdk/data/azcosmos/queryengine/gonative/engine_test.go`

Purpose: introduce the package where Stage 1+ operators will live. For Stage 0, the package exposes only `Default()`, which returns an engine that advertises no features and rejects every `CreateQueryPipeline` call with `ErrUnsupportedPlanFeature` — semantically identical to `queryengine.Disabled` today, but a distinct type so later stages can extend it in place.

- [ ] **Step 1: Write the failing tests**

Create `sdk/data/azcosmos/queryengine/gonative/engine_test.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative_test

import (
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine/gonative"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultReturnsNonNilEngine(t *testing.T) {
	e := gonative.Default()
	require.NotNil(t, e)
}

func TestDefaultAdvertisesNoFeaturesAtStage0(t *testing.T) {
	assert.Equal(t, "", gonative.Default().SupportedFeatures())
}

func TestDefaultCreateQueryPipelineReturnsUnsupportedAtStage0(t *testing.T) {
	_, err := gonative.Default().CreateQueryPipeline("SELECT * FROM c", "{}", "{}")
	require.Error(t, err)
	assert.True(t, errors.Is(err, queryengine.ErrUnsupportedPlanFeature),
		"Default() should reject all queries at Stage 0; got %v", err)
}

func TestDefaultCreateReadManyPipelineReturnsUnsupported(t *testing.T) {
	_, err := gonative.Default().CreateReadManyPipeline(nil, "{}", "", 0, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, queryengine.ErrUnsupportedPlanFeature))
}

func TestDefaultReturnsDistinctEngineInstances(t *testing.T) {
	// Each caller gets its own instance — enables per-engine caches in later stages.
	a := gonative.Default()
	b := gonative.Default()
	assert.NotSame(t, a, b)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd sdk/data/azcosmos && go test ./queryengine/gonative/... -run "^TestDefault" -count=1 -v`
Expected: compilation error — package `gonative` does not exist.

- [ ] **Step 3: Create the package doc file**

Create `sdk/data/azcosmos/queryengine/gonative/doc.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Package gonative implements queryengine.QueryEngine entirely in Go
// (no cgo, no native plugins, no external toolchain).
//
// It is the default query engine that the azcosmos SDK will auto-wire into
// cross-partition queries starting at Stage 1 of the BYT-9239 roll-out.
// In Stage 0 (this package's first release) the engine is inert:
// SupportedFeatures() returns "" and CreateQueryPipeline returns
// queryengine.ErrUnsupportedPlanFeature unconditionally.
package gonative
```

- [ ] **Step 4: Create the engine implementation**

Create `sdk/data/azcosmos/queryengine/gonative/engine.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative

import (
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
)

// Engine is the pure-Go implementation of queryengine.QueryEngine.
// It is constructed by Default(); callers should not instantiate Engine
// directly because future stages may add required initialization.
type Engine struct {
	// (future stages add fields: supportedFeatures set, cardinality caps, etc.)
}

// Default returns a new Engine with Stage-0 capabilities (none).
// Callers should treat the returned value as opaque.
func Default() *Engine {
	return &Engine{}
}

// SupportedFeatures implements queryengine.QueryEngine.
func (e *Engine) SupportedFeatures() string {
	// Stage 0: advertise no features. Stage 1 appends "Aggregate", etc.
	return ""
}

// CreateQueryPipeline implements queryengine.QueryEngine.
func (e *Engine) CreateQueryPipeline(_ string, _ string, _ string) (queryengine.QueryPipeline, error) {
	// Stage 0: every query plan is unsupported.
	return nil, queryengine.ErrUnsupportedPlanFeature
}

// CreateReadManyPipeline implements queryengine.QueryEngine.
func (e *Engine) CreateReadManyPipeline(_ []queryengine.ItemIdentity, _ string, _ string, _ uint8, _ []string) (queryengine.QueryPipeline, error) {
	return nil, queryengine.ErrUnsupportedPlanFeature
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd sdk/data/azcosmos && go test ./queryengine/gonative/... -count=1 -v`
Expected: all five tests PASS.

- [ ] **Step 6: Verify the engine satisfies the interface at compile time**

Add an interface-conformance assertion. Edit `sdk/data/azcosmos/queryengine/gonative/engine.go`, immediately after the `Engine` struct definition:

```go
// Compile-time check that *Engine satisfies the interface.
var _ queryengine.QueryEngine = (*Engine)(nil)
```

Run: `cd sdk/data/azcosmos && go build ./queryengine/gonative/...`
Expected: build succeeds.

- [ ] **Step 7: Commit**

```bash
git add sdk/data/azcosmos/queryengine/gonative/
git commit -m "azcosmos: add gonative package with inert Default engine"
```

---

## Task 4: Add `golang-lru/v2` dependency to the fork

**Files:**
- Modify: `sdk/data/azcosmos/go.mod`
- Modify: `sdk/data/azcosmos/go.sum`

Purpose: Tasks 5 and 6 need an LRU cache. `hashicorp/golang-lru/v2` is the de facto choice and is already in Bytebase's transitive deps (`go.mod:60`: `github.com/hashicorp/golang-lru/v2 v2.0.7`); using it in the fork keeps the downstream dep graph flat.

- [ ] **Step 1: Add the require directive**

Run: `cd sdk/data/azcosmos && go get github.com/hashicorp/golang-lru/v2@v2.0.7`
Expected: `go.mod` gets a new `require github.com/hashicorp/golang-lru/v2 v2.0.7` line; `go.sum` updates.

- [ ] **Step 2: Tidy the module**

Run: `cd sdk/data/azcosmos && go mod tidy`
Expected: no further changes (single-dep add).

- [ ] **Step 3: Verify the module still builds**

Run: `cd sdk/data/azcosmos && go build ./...`
Expected: build succeeds.

- [ ] **Step 4: Commit**

```bash
git add sdk/data/azcosmos/go.mod sdk/data/azcosmos/go.sum
git commit -m "azcosmos: add hashicorp/golang-lru/v2 v2.0.7 dependency"
```

---

## Task 5: Add `QueryPlanCacheSize` option to `ClientOptions`

**Files:**
- Modify: `sdk/data/azcosmos/cosmos_client_options.go`
- Modify: `sdk/data/azcosmos/cosmos_client_options_test.go` (create if it does not exist — verify with `ls` first)

Purpose: expose a per-client knob for the plan cache size so operators with large query libraries can raise it (or set to 0 to disable caching entirely in Stage 1+).

- [ ] **Step 1: Write the failing test**

First, check whether `cosmos_client_options_test.go` already exists:

Run: `ls sdk/data/azcosmos/cosmos_client_options_test.go 2>/dev/null || echo MISSING`

If MISSING, create the file; otherwise, append.

Add this test function to `sdk/data/azcosmos/cosmos_client_options_test.go`:

```go
func TestQueryPlanCacheSizeDefaultsToSensibleValue(t *testing.T) {
	var o azcosmos.ClientOptions
	// Zero value is the Go default; the SDK treats zero as "use the built-in default".
	// The built-in default is not user-facing, but it must be > 0 so caching is on by default.
	assert.Equal(t, 0, o.QueryPlanCacheSize,
		"zero value should map to the SDK default; the field itself stores zero")
}

func TestQueryPlanCacheSizeIsSettable(t *testing.T) {
	o := azcosmos.ClientOptions{QueryPlanCacheSize: 512}
	assert.Equal(t, 512, o.QueryPlanCacheSize)
}

func TestQueryPlanCacheSizeNegativeIsAllowed(t *testing.T) {
	// Interpretation of negative values is documented as "disabled".
	o := azcosmos.ClientOptions{QueryPlanCacheSize: -1}
	assert.Equal(t, -1, o.QueryPlanCacheSize)
}
```

If you created the file, add a package declaration and imports:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd sdk/data/azcosmos && go test -run "^TestQueryPlanCacheSize" -count=1 -v`
Expected: compilation error — `QueryPlanCacheSize` undefined on `ClientOptions`.

- [ ] **Step 3: Add the field**

In `sdk/data/azcosmos/cosmos_client_options.go`, find the `ClientOptions` struct and add:

```go
	// QueryPlanCacheSize bounds the per-client LRU cache of Cosmos query plans.
	// Zero (the default) uses the SDK default (256 entries).
	// Negative values disable the cache entirely.
	QueryPlanCacheSize int
```

Place it at the end of the struct body, directly before the closing brace, with a blank line separating it from the prior field.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd sdk/data/azcosmos && go test -run "^TestQueryPlanCacheSize" -count=1 -v`
Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sdk/data/azcosmos/cosmos_client_options.go sdk/data/azcosmos/cosmos_client_options_test.go
git commit -m "azcosmos: add QueryPlanCacheSize option to ClientOptions"
```

---

## Task 6: Add `planCache` field to `ContainerClient` with LRU init

**Files:**
- Modify: `sdk/data/azcosmos/cosmos_container.go`
- Create: `sdk/data/azcosmos/cosmos_container_plan_cache_test.go`

Purpose: the per-container LRU cache that Stage 1+ uses to amortize `getQueryPlanFromGateway` costs. Allocated here; unused this stage.

- [ ] **Step 1: Write the failing tests**

Create `sdk/data/azcosmos/cosmos_container_plan_cache_test.go`:

```go
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

// Tests in this file are in-package (not _test package) because planCache is
// intentionally unexported — only other files in this package need to see it.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainerPlanCacheAllocatedWithDefaultSize(t *testing.T) {
	c := newTestContainer(t, 0) // 0 -> SDK default
	require.NotNil(t, c.planCache)
	assert.Equal(t, 256, c.planCache.Size())
}

func TestContainerPlanCacheRespectsExplicitSize(t *testing.T) {
	c := newTestContainer(t, 64)
	require.NotNil(t, c.planCache)
	assert.Equal(t, 64, c.planCache.Size())
}

func TestContainerPlanCacheDisabledByNegativeSize(t *testing.T) {
	c := newTestContainer(t, -1)
	assert.Nil(t, c.planCache, "negative size should disable the cache entirely")
}

func TestContainerPlanCacheRoundTrip(t *testing.T) {
	c := newTestContainer(t, 4)
	key := planCacheKey{
		databaseID:      "db",
		containerID:     "coll",
		normalizedQuery: "SELECT 1",
		features:        "",
	}
	payload := []byte(`{"partitionedQueryExecutionInfoVersion":2}`)
	c.planCache.Add(key, payload)
	got, ok := c.planCache.Get(key)
	require.True(t, ok)
	assert.Equal(t, payload, got)
}

// newTestContainer constructs a minimal ContainerClient sufficient for cache
// tests; it does not make network calls.
func newTestContainer(t *testing.T, cacheSize int) *ContainerClient {
	t.Helper()
	return &ContainerClient{
		link:      "dbs/db/colls/coll",
		planCache: newPlanCache(cacheSize),
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd sdk/data/azcosmos && go test -run "^TestContainerPlanCache" -count=1 -v`
Expected: compilation error — `planCache`, `planCacheKey`, `newPlanCache` undefined.

- [ ] **Step 3: Implement the cache type and key**

At the top of `sdk/data/azcosmos/cosmos_container.go`, add to the imports:

```go
	lru "github.com/hashicorp/golang-lru/v2"
```

Below the `ContainerClient` struct declaration (after the closing `}`), add:

```go
// defaultQueryPlanCacheSize is the cache size used when
// ClientOptions.QueryPlanCacheSize is zero.
const defaultQueryPlanCacheSize = 256

// planCacheKey uniquely identifies a cached query plan within a container.
// The `features` component is part of the key because the same query text
// produces different plans depending on the features the engine advertises.
type planCacheKey struct {
	databaseID      string
	containerID     string
	normalizedQuery string
	features        string
}

// newPlanCache returns an initialized LRU cache for query plans, or nil if
// the caller asked for caching to be disabled (negative size).
func newPlanCache(size int) *lru.Cache[planCacheKey, []byte] {
	switch {
	case size < 0:
		return nil
	case size == 0:
		size = defaultQueryPlanCacheSize
	}
	// lru.New returns an error only when size <= 0, which we have ruled out.
	c, err := lru.New[planCacheKey, []byte](size)
	if err != nil {
		// Should be unreachable.
		panic(err)
	}
	return c
}
```

Add a field to the `ContainerClient` struct:

```go
	planCache *lru.Cache[planCacheKey, []byte]
```

Place it after the existing fields, with no blank line separator.

**Wiring the cache size through the client.** The fork's `Client` struct (`cosmos_client.go`) does **not** currently store `ClientOptions` — `NewClientWithKey` and `NewClient` destructure the options into a pipeline and discard the struct. We need to thread just the one value we care about. Do this in two edits:

1. In `sdk/data/azcosmos/cosmos_client.go`, add a field to the `Client` struct:

```go
type Client struct {
    endpoint    string
    internal    *azcore.Client
    gem         *globalEndpointManager
    endpointUrl *url.URL

    queryPlanCacheSize int
}
```

Then populate it at the end of `NewClientWithKey` (currently returns `&Client{endpoint: endpoint, endpointUrl: endpointUrl, internal: internalClient, gem: gem}, nil`):

```go
client := &Client{endpoint: endpoint, endpointUrl: endpointUrl, internal: internalClient, gem: gem}
if o != nil {
    client.queryPlanCacheSize = o.QueryPlanCacheSize
}
return client, nil
```

Do the same for `NewClient` (the token-credential variant). Search for the other `&Client{...}` literal and apply the same pattern.

2. In `sdk/data/azcosmos/cosmos_container.go`, update `newContainer` (currently 3 lines) to include the cache:

```go
func newContainer(id string, database *DatabaseClient) (*ContainerClient, error) {
    return &ContainerClient{
        id:        id,
        database:  database,
        link:      createLink(database.link, pathSegmentCollection, id),
        planCache: newPlanCache(database.client.queryPlanCacheSize),
    }, nil
}
```

`database.client` reaches the parent `*Client`; the field name is `client` per the existing `DatabaseClient` definition.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd sdk/data/azcosmos && go test -run "^TestContainerPlanCache" -count=1 -v`
Expected: all four tests PASS.

- [ ] **Step 5: Run the full package test suite to check for regressions**

Run: `cd sdk/data/azcosmos && go test ./... -count=1`
Expected: PASS. (Emulator-dependent tests skip when the emulator is unreachable — that is fine.)

- [ ] **Step 6: Commit**

```bash
git add sdk/data/azcosmos/cosmos_container.go \
        sdk/data/azcosmos/cosmos_container_plan_cache_test.go
git commit -m "azcosmos: add per-container LRU plan cache (allocated, unused in Stage 0)"
```

---

## Task 7: Update `CHANGELOG.md`

**Files:**
- Modify: `sdk/data/azcosmos/CHANGELOG.md`

- [ ] **Step 1: Add a CHANGELOG entry under `1.5.0-beta.6 (Unreleased)`**

At the top of the file, find the `## 1.5.0-beta.6 (Unreleased)` section. Under `### Other Changes` (if present, else under `### Features Added`), append:

```markdown
* Added `queryengine.Disabled` sentinel and `queryengine.ErrUnsupportedPlanFeature` error, plus a new `queryengine/gonative` sub-package housing the pure-Go query engine that later stages will flesh out. No behavior change in Stage 0 — the engine is inert and not yet auto-enabled. See BYT-9239.
* Added `ClientOptions.QueryPlanCacheSize` and a per-container LRU plan cache; unused in Stage 0.
```

- [ ] **Step 2: Verify markdown renders cleanly**

Run: `grep -n "^##" sdk/data/azcosmos/CHANGELOG.md | head`
Expected: the `## 1.5.0-beta.6 (Unreleased)` header still sits at the top and is well-formed.

- [ ] **Step 3: Commit**

```bash
git add sdk/data/azcosmos/CHANGELOG.md
git commit -m "azcosmos: changelog entry for queryengine Stage 0 plumbing"
```

---

## Task 8: Integration smoke test — fork still works end-to-end

**Files:**
- None new; this task is verification-only.

Purpose: make sure none of the above changes regressed the fork's existing tests, including the emulator-dependent ones if the emulator is reachable.

- [ ] **Step 1: Run the full fork test suite with the race detector**

Run: `cd sdk/data/azcosmos && go test -race ./... -count=1`
Expected: PASS (emulator tests may skip).

- [ ] **Step 2: If a Cosmos emulator is running locally, confirm emulator tests pick it up**

Check: `docker ps --filter name=cosmosdb --format '{{.Names}}'`

If the container is up, the emulator tests (files named `emulator_*_test.go`, e.g. `emulator_cosmos_container_test.go`) detect it automatically through the `newEmulatorTests(t)` helper and run inline with the Step 1 invocation. No separate command or build tag is needed — just re-read the Step 1 output and confirm that tests with names like `TestContainerCRUD` are not marked SKIP.

If the container is not up, those tests skip themselves. That is acceptable — the fork's CI runs the emulator separately.

- [ ] **Step 3: Confirm the engineered-in interface conformance still holds**

Run: `cd sdk/data/azcosmos && go vet ./...`
Expected: no diagnostics.

No commit in this task.

---

## Task 9: Push the fork branch and capture the pseudo-version for future use

**Files:**
- None in the repo; the deliverable is a pushed branch + a recorded pseudo-version string.

Purpose: Stage 1 and beyond will bump Bytebase's `go.mod` to this new pseudo-version. Record it now so future stages have a concrete anchor.

- [ ] **Step 1: Push the branch**

Run: `git push -u origin bytebase/cosmos-query-engine-stage-0`
Expected: branch pushed; GitHub print-out of the PR-creation URL.

- [ ] **Step 2: Open a pull request on the fork**

Run:
```bash
gh pr create \
  --title "azcosmos: Stage 0 — query engine plumbing (BYT-9239)" \
  --body "$(cat <<'EOF'
Part 1 of 7 for BYT-9239. This PR adds the sentinels, the `gonative` package skeleton,
the per-container plan cache, and the `QueryPlanCacheSize` option. No behavior change —
the engine is inert and not yet auto-enabled. See the spec for full context:
`docs/superpowers/specs/2026-04-22-cosmos-cross-partition-query-engine-design.md` in the
Bytebase repo.

## Test plan
- [x] `go test -race ./sdk/data/azcosmos/... -count=1`
- [x] `go vet ./sdk/data/azcosmos/...`
- [ ] Reviewer: sanity-check CHANGELOG entry and confirm no public API signature changed.
EOF
)"
```

Expected: PR URL printed.

- [ ] **Step 3: Record the head commit SHA for later go.mod bumps**

Run:
```bash
git rev-parse HEAD | head -c 12
```

Paste the 12-char prefix (e.g. `a1b2c3d4e5f6`) and the UTC commit timestamp into a scratch note for Stage 1:

Run: `git show -s --format='%cI %H' HEAD`
Expected: a UTC ISO timestamp followed by the full SHA.

The resulting `go.mod` pseudo-version for Stage 1+ will look like:
`v0.0.0-<timestamp compacted to YYYYMMDDhhmmss>-<12-char-SHA>`.

No commit in this task.

---

## Self-review checklist (run before handing the plan off to execution)

Each check runs against the rendered plan file.

- [ ] **Spec coverage.** The spec's Section 3.1 (package layout) maps to Tasks 3, 5, 6. Section 3.2 (activation) is deferred to Stage 1 and explicitly called out in the "Out of scope" block. Section 3.3 (data flow) has no Stage 0 work. Section 3.4 (memory / streaming) applies to operators — Stage 1+. Section 3.5 (error handling) — sentinels are delivered. Section 4 Stage 0 description — covered.

- [ ] **Placeholder scan.** No "TBD", no "TODO", no "similar to", no unshown code inside steps that change code.

- [ ] **Type / identifier consistency.**
  - Sentinel name: `Disabled` (Task 2) — reused in Task 3 test imports.
  - Error name: `ErrUnsupportedPlanFeature` (Task 2) — reused in Tasks 2 and 3.
  - Constructor: `gonative.Default()` (Task 3) — reused in Task 3 tests.
  - Cache key struct: `planCacheKey` with fields `databaseID, containerID, normalizedQuery, features` (Task 6) — the spec uses the same field names in §3.2 and §5.1.
  - Cache size constant: `defaultQueryPlanCacheSize = 256` (Task 6) — matches spec §5.1.
  - Field on `ContainerClient`: `planCache *lru.Cache[planCacheKey, []byte]` (Task 6) — matches spec §5.5.

- [ ] **Path correctness.** All fork paths start with `sdk/data/azcosmos/`; the Bytebase plan doc lives under `docs/superpowers/plans/` per the writing-plans skill default.

- [ ] **Granularity.** Each step is a 2–5 minute action; every code change is fully shown; every test precedes its implementation.

- [ ] **Out-of-scope is explicit.** The Stage 0 plan explicitly defers Stage 1 activation, engage-on-error, and the Bytebase kill switch, so later plans won't get lost.

---

## What Stage 1 depends on from Stage 0

Bookmarks for the Stage 1 author:

1. `queryengine.Disabled` — Stage 1 uses this when the Bytebase env var opts out.
2. `queryengine.ErrUnsupportedPlanFeature` — Stage 1's aggregate operator returns this if a plan sneaks an unsupported aggregate past `SupportedFeatures()`.
3. `gonative.Default()` — Stage 1 extends the `Engine` struct with aggregate support; the constructor signature stays stable.
4. `ContainerClient.planCache` — Stage 1 fills it.
5. `ClientOptions.QueryPlanCacheSize` — Stage 1 reads it at client build time (already done).
6. Stage 0 head commit SHA (from Task 9) — Stage 1 bumps Bytebase `go.mod` past this point once Stage 1's own commits are pushed.

---
