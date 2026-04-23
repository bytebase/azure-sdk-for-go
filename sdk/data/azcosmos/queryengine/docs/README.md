# queryengine design docs

Design and implementation artifacts for the pure-Go distributed query engine
Bytebase is adding to `sdk/data/azcosmos/queryengine`. Tracked upstream of this
work in [Linear issue BYT-9239](https://linear.app/bytebase/issue/BYT-9239).

| File | What it is |
|---|---|
| [`design.md`](./design.md) | The authoritative design spec — architecture, activation strategy, operator decomposition, rollout plan across seven stages, open concerns from adversarial review. |
| [`stage-0-plan.md`](./stage-0-plan.md) | The TDD-grade implementation plan for Stage 0 (this PR's scope — plumbing only, zero behavior change). |
| [`target-queries.md`](./target-queries.md) | The 40-query inventory that defines "parity with the Microsoft .NET SDK" concretely. Every stage's tests validate against this list. |

## Reproduction harnesses

The design docs reference two reproduction harnesses:

- `scripts/cosmos-repro-dotnet/` — runs the 40-query inventory against the
  Microsoft .NET SDK (`Microsoft.Azure.Cosmos 3.47.1`). Used to produce the
  reference pass/fail column in `target-queries.md`.
- `scripts/cosmos-repro-go/` — (Stage 7) Go twin of the above. Validates
  that the Bytebase SDK reaches .NET parity.

**Both harnesses live in the Bytebase monorepo**, not in this repository.
They are test infrastructure, not part of the SDK itself. The SDK's own
tests live in `sdk/data/azcosmos/**_test.go` as usual.

## Stage landing order

This PR (#2) is Stage 0 of 7. Each subsequent stage will add its own
implementation plan to this directory:

- Stage 0 — plumbing (this PR)
- Stage 1 — activation + scalar aggregates
- Stage 2 — DISTINCT
- Stage 3 — single-key ORDER BY
- Stage 4 — TOP
- Stage 5 — OFFSET / LIMIT
- Stage 6 — GROUP BY
- Stage 7 — Go reproduction harness + end-to-end parity validation
