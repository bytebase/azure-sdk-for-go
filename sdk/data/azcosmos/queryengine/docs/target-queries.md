# BYT-9239 — Cosmos DB Query Parsing / Cross-Partition Investigation

## Context

Linear issue BYT-9239 reports that Bytebase's Cosmos DB SQL viewer fails on several query types with cross-partition errors. The attached functional test plan (`Bytebase-Cosmos-Parsing.pdf`) ran 13 feature areas against a `WorldCities` container; many queries fail at the gateway with:

> "The provided cross partition query can not be directly served by the gateway. This is a first chance (internal) exception that all newer clients will know how to handle gracefully... but unless you see it bubble up as an exception (which only happens on older SDK clients), then you can safely ignore this message."

Aggregates additionally fail with: *"Cross partition query only supports 'VALUE \<AggregateFunc\>' for aggregates."*

This points at the Bytebase Cosmos driver using an older/gateway-mode SDK path that does not transparently handle cross-partition queries (TOP, ORDER BY, aggregates, GROUP BY, DISTINCT, OFFSET/LIMIT).

## Query Inventory from PDF

> **.NET SDK column**: measured by running each query against the same Azure account (`bytebase-cosmostest` → `testdb` → `WorldCities`, partition key `/country`) via the Microsoft .NET SDK `Microsoft.Azure.Cosmos` v3.47.1 (harness: `scripts/cosmos-repro-dotnet/`). Results are identical in both `ConnectionMode.Gateway` and `ConnectionMode.Direct`, so a single column is shown.

### 1. Basic SELECT
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 1.1 | `SELECT * FROM c` | Success | PASS (18 rows) | — |
| 1.2 | `SELECT c.id, c.name, c.country, c.population FROM c` | Success | PASS (18 rows) | — |
| 1.3 | `SELECT TOP 10 * FROM c` | **Failed** (cross-partition) | PASS (10 rows) | — |

### 2. Filtering — WHERE Clause
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 2.1 | `SELECT * FROM c WHERE c.country = "AE"` | Success | PASS (5 rows) | — |
| 2.2 | `SELECT * FROM c WHERE c._ts > 1743702439` | Success | PASS (18 rows) | — |
| 2.3 | `SELECT * FROM c WHERE c._ts >= 1743702439` | Success | PASS (18 rows) | — |
| 2.4 | `SELECT * FROM c WHERE c.country = "AE" OR c.country = "AD"` | Success | PASS (8 rows) | — |
| 2.5 | `SELECT * FROM c WHERE c.country IN ("AE", "US", "GB", "AD")` | Success | PASS (15 rows) | — |
| 2.6 | `SELECT * FROM c WHERE CONTAINS(c.name, "Fujayrah")` | Success | PASS (1 row) | — |
| 2.7 | `SELECT * FROM c WHERE STARTSWITH(c.name, "Al")` | Success | PASS (1 row) | — |
| 2.8 | `SELECT * FROM c WHERE c.countryRegion != "4"` | Success | PASS (11 rows) | — |
| 2.9 | `SELECT * FROM c WHERE STRINGTONUMBER(c.population) BETWEEN 10000 AND 100000` | Success | PASS (2 rows) | — |

### 3. Projection & Aliasing
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 3.1 | `SELECT c.name AS cityName, c.country AS countryCode, c.population AS pop FROM c` | Success | PASS (18 rows) | — |
| 3.2 | `SELECT c.name, c.population, STRINGTONUMBER(c.population) / 1000 AS populationInThousands FROM c` | Success | PASS (18 rows) | — |
| 3.3 | `SELECT CONCAT(c.name, " (", c.country, ")") AS label FROM c` | Success | PASS (18 rows) | — |

### 4. Sorting — ORDER BY
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 4.1 | `SELECT c.name, c.population FROM c ORDER BY c.population ASC` | **Failed** (cross-partition) | PASS (18 rows) | — |
| 4.2 | `SELECT c.name, c.population FROM c ORDER BY c.population DESC` | **Failed** (cross-partition) | PASS (18 rows) | — |
| 4.3 | `SELECT c.country, c.name, c.population FROM c ORDER BY c.country ASC, c.population DESC` | **Failed** (cross-partition) | **FAIL** | Server-side 400 BadRequest: *"The order by query does not have a corresponding composite index that it can be served from."* Multi-key ORDER BY requires a Cosmos composite index on `(country ASC, population DESC)` to be provisioned on the container. This is a real server constraint that affects every SDK, not an SDK limitation. |

### 5. Aggregation
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 5.1 | `SELECT COUNT(1) AS totalRecords FROM c` | **Failed** (must use VALUE) | PASS (1 row) | — |
| 5.1b | `SELECT VALUE COUNT(1) FROM c` | **Failed** (cross-partition) | PASS (1 row) | — |
| 5.2 | `SELECT COUNT(1) AS aeCount FROM c WHERE c.country = "AE"` | **Failed** (must use VALUE) | PASS (1 row) | — |
| 5.2b | `SELECT VALUE COUNT(1) FROM c WHERE c.country = "AE"` | **Failed** (cross-partition) | PASS (1 row) | — |
| 5.3 | `SELECT SUM(STRINGTONUMBER(c.population)) AS totalPop FROM c` | **Failed** (must use VALUE) | PASS (1 row) | — |
| 5.3b | `SELECT VALUE SUM(STRINGTONUMBER(c.population)) FROM c` | **Failed** (cross-partition) | PASS (1 row) | — |
| 5.4 | `SELECT AVG(STRINGTONUMBER(c.population)) AS avgPop FROM c` | **Failed** (must use VALUE) | PASS (1 row) | — |
| 5.4b | `SELECT VALUE AVG(STRINGTONUMBER(c.population)) FROM c` | **Failed** (cross-partition) | PASS (1 row) | — |
| 5.5 | `SELECT MIN(...) AS minPop, MAX(...) AS maxPop FROM c WHERE StringToNumber(c.population) > 0` | **Failed** (multi-agg not allowed) | PASS (1 row) | — |

### 6. GROUP BY
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 6.1 | `SELECT c.country, COUNT(1) AS cityCount FROM c GROUP BY c.country` | **Failed** (cross-partition) | PASS (6 rows) | — |
| 6.2 | `SELECT c.countryRegion, COUNT(1) AS count, SUM(StringToNumber(c.population)) AS totalPop FROM c GROUP BY c.countryRegion` | **Failed** (cross-partition) | PASS (6 rows) | — |

### 7. Built-in Functions — String
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 7.1 | `SELECT UPPER(c.name) AS upperName, LOWER(c.country) AS lowerCountry, LENGTH(c.name) AS nameLen FROM c` | Success | PASS (18 rows) | — |

### 8. Built-in Functions — Math
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 8.1 | `SELECT c.name, ROUND(StringToNumber(c.latitude)) AS lat, ROUND(StringToNumber(c.longitude)) AS lon FROM c` | Success | PASS (18 rows) | — |

### 9. Built-in Functions — Type Checking
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 9.1 | `SELECT c.name, IS_STRING(c.name) AS isStr, IS_NUMBER(c.population) AS isNum FROM c` | Success | PASS (18 rows) | — |
| 9.2 | `SELECT c.id, IS_DEFINED(c.region) AS hasRegion, IS_NULL(c.code) AS codeNull FROM c` | Success | PASS (18 rows) | — |

### 10. DISTINCT
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 10.1 | `SELECT DISTINCT c.country FROM c` | **Failed** (cross-partition) | PASS (6 rows) | — |
| 10.2 | `SELECT DISTINCT VALUE c.countryRegion FROM c` | **Failed** (cross-partition) | PASS (6 rows) | — |

### 11. VALUE Keyword
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 11.1 | `SELECT VALUE c.name FROM c WHERE c.country = "AE"` | Success | PASS (5 rows) | — |
| 11.2 | `SELECT VALUE COUNT(1) FROM c` | **Failed** (scalar aggregate, cross-partition) | PASS (1 row) | — |

### 12. Pagination — OFFSET / LIMIT
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 12.1 | `SELECT * FROM c ORDER BY c.name OFFSET 0 LIMIT 10` | **Failed** (cross-partition) | PASS (10 rows) | — |
| 12.2 | `SELECT * FROM c ORDER BY c.name OFFSET 10 LIMIT 10` | **Failed** (cross-partition) | PASS (8 rows) | — |

### 13. Geo-spatial
| # | Query | Go fork (PDF) | .NET SDK | .NET Failure Reason |
|---|-------|--------|---------|---------------------|
| 13.1 | `SELECT c.name, ST_DISTANCE({"type":"Point","coordinates":[StringToNumber(c.longitude), StringToNumber(c.latitude)]}, {"type":"Point","coordinates":[55.2708, 25.2048]}) AS distFromDubaiInMeters FROM c` | Success | PASS (18 rows) | — |

## Summary

| Feature Area | Go fork (PDF) | .NET SDK | Notes |
|---|---|---|---|
| 1. Basic SELECT | PARTIAL | PASS (3/3) | Go fails on `TOP`; .NET passes all |
| 2. WHERE Filtering | GOOD | PASS (9/9) | Both handle single-partition queries fine |
| 3. Projection & Aliasing | GOOD | PASS (3/3) | — |
| 4. ORDER BY | FAIL | PARTIAL (2/3) | .NET passes single-key ORDER BY; 4.3 multi-key fails on both SDKs because the container lacks a composite index on `(country ASC, population DESC)` — server constraint, not SDK |
| 5. Aggregation | FAIL | PASS (9/9) | .NET transparently supports aliased and `VALUE` aggregates, single and multi-agg |
| 6. GROUP BY | FAIL | PASS (2/2) | — |
| 7. String Functions | GOOD | PASS (1/1) | — |
| 8. Math Functions | GOOD | PASS (1/1) | — |
| 9. Type Checking | GOOD | PASS (2/2) | — |
| 10. DISTINCT | FAIL | PASS (2/2) | .NET handles both `DISTINCT` and `DISTINCT VALUE` |
| 11. VALUE Keyword | PARTIAL | PASS (2/2) | .NET handles scalar and scalar-aggregate forms |
| 12. OFFSET / LIMIT | FAIL | PASS (2/2) | .NET handles pagination across partitions |
| 13. Geo-spatial | GOOD | PASS (1/1) | — |

**Bottom line:** 39/40 queries PASS on the .NET SDK; the single .NET failure (4.3) is a server-side composite-index requirement that affects every SDK. All failures attributed to "older SDK clients" in the PDF — i.e. anything involving cross-partition orchestration (TOP, ORDER BY, aggregates, GROUP BY, DISTINCT, OFFSET/LIMIT, scalar VALUE aggregates) — succeed transparently on the .NET SDK. The root cause of BYT-9239 is that `azcosmos` (Go) lacks the client-side distributed query engine that the .NET/Java/Python/JS SDKs ship; syncing the fork with upstream does not help because the upstream SDK itself does not implement it.

## Current Bytebase Cosmos Driver (Phase 1 Findings)

### SDK versions

| Side | Package | Version | Notes |
|---|---|---|---|
| Bytebase (Go) | `github.com/bytebase/azure-sdk-for-go/sdk/data/azcosmos` (fork, replaces upstream `v1.4.2`) | pseudo-version `v0.0.0-20260414094732-5b89f0889452` | Commit `5b89f0889452` at 2026-04-14; CHANGELOG positions this on the upstream `1.5.0-beta.6 (Unreleased)` branch, just past `1.5.0-beta.5 (2026-03-09)` |
| Reproduction harness (.NET) | `Microsoft.Azure.Cosmos` (NuGet) | **3.47.1** | Targets `net10.0`; host runs .NET SDK 10.0.106 installed via Homebrew |

### Driver behavior

- **Driver**: [backend/plugin/db/cosmosdb/cosmosdb.go](backend/plugin/db/cosmosdb/cosmosdb.go)
  - Query path: [cosmosdb.go:146](backend/plugin/db/cosmosdb/cosmosdb.go:146) `QueryConn` → `container.NewCrossPartitionQueryItemsPager(statement, queryOption)` at [cosmosdb.go:172](backend/plugin/db/cosmosdb/cosmosdb.go:172)
  - Only `PageSizeHint` is set; no consistency level, no direct-mode opts, no partition-key hint
  - Uses SDK default connection (gateway mode)
- **Query engine gap** — the Go fork *does* expose a `queryengine.QueryEngine` interface on `QueryOptions.QueryEngine` (preview, not-for-production per the SDK doc comment). Bytebase's driver does **not** set it, and Microsoft's reference implementation (`azcosmoscx`, Rust-based) is **not imported**. So the cross-partition orchestration hook exists but is wired to `nil` — the SDK just forwards queries to the gateway and returns whatever the gateway says.
- **User report**: forked SDK synced with upstream — problem persists. Confirmed root cause: the Go SDK lacks a production client-side distributed query engine (interface only); a sync does not change that. The .NET SDK ships a working one in-box, which is why the .NET reproduction passes 39/40 queries.

## Available Test Targets (verified)

| Target | State | Notes |
|---|---|---|
| Local emulator (Docker `cosmosdb`) | Running | vnext Rust gateway, HTTP on `127.0.0.1:8081`, explorer on `1234`, 5 partitions, uses emulator master key |
| Azure `bytebase-cosmostest` | Live | DB `testdb`, container `WorldCities`, partition key `/country` — matches the PDF schema; cross-partition queries will trigger the same failures |

## .NET Reproduction Plan

### Why .NET
Microsoft's flagship Cosmos DB SDK is `Microsoft.Azure.Cosmos` (C# / .NET). It is the reference implementation — if queries that fail on the Go fork succeed on .NET, the root cause is Go-SDK-side (gateway codepath / lack of cross-partition orchestration); if .NET also fails, it's server/gateway-level and will need a different fix (e.g., require partition-key filter, surface better error).

### Scope
Replay the **failing** queries from the PDF (the PASS groups don't need re-validation):

- Basic SELECT (TOP only)
- ORDER BY (×3)
- Aggregation with and without `VALUE` (×9 incl. MIN/MAX multi-agg)
- GROUP BY (×2)
- DISTINCT (×2)
- VALUE scalar aggregate (×1)
- OFFSET/LIMIT (×2)

For each query record: `{ success, row_count | error, elapsed_ms, mode }` where `mode ∈ {Gateway, Direct}`.

### Approach

1. **Host setup** — install .NET SDK via Homebrew: `brew install --cask dotnet-sdk`. Verify with `dotnet --version`.
2. **Project** — create `scripts/cosmos-repro-dotnet/` (throwaway), `dotnet new console`, add package `Microsoft.Azure.Cosmos` (latest stable, currently 3.47.x).
3. **Test harness** — single `Program.cs` that:
   - Reads endpoint / key / db / container / connection-mode from env vars
   - Defines the ~19 failing queries as a list
   - For each query and each `ConnectionMode` ∈ `{Gateway, Direct}`:
     - `container.GetItemQueryIterator<JsonElement>(sql, requestOptions: new QueryRequestOptions { MaxItemCount = 1000 })`
     - Drain pages, capture row count or exception message
   - Print a markdown table identical in shape to the PDF summary
4. **Target** — Azure `bytebase-cosmostest` → `testdb` → `WorldCities` (already provisioned, `/country` PK). Auth via master key retrieved with `az cosmosdb keys list`.
5. **Compare** — cross-reference .NET results with the PDF's Go-SDK results. Identify which queries succeed in .NET but fail in Go → that is the Bytebase-actionable set. Any queries failing in both point to gateway-level constraints unrelated to SDK choice.

### Critical files to author (outside plan mode)
- `scripts/cosmos-repro-dotnet/CosmosRepro.csproj`
- `scripts/cosmos-repro-dotnet/Program.cs`
- `scripts/cosmos-repro-dotnet/README.md` (how to run against emulator / Azure)

### Verification
- `dotnet run -- --mode gateway`
- `dotnet run -- --mode direct`
- Produce a side-by-side Go-fork-vs-.NET markdown table; paste into Linear BYT-9239 as the diagnosis.

### Out of scope (for this step)
- Changing Bytebase driver code
- Upgrading/modifying the forked Go SDK
- Adding integration tests to the backend
