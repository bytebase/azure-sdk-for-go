// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

// Plan-cache wiring tests run in-package because planCache + planCacheKey are
// unexported. The tests here verify the pure cache semantics; the full
// round-trip through getQueryPlanFromGateway is exercised by the engage-on-
// error integration test (Stage 1 Task C5).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanCacheKey_DistinctFeaturesGivesDistinctEntries(t *testing.T) {
	c := newPlanCache(8)
	require.NotNil(t, c)
	k1 := planCacheKey{databaseID: "db", containerID: "coll", normalizedQuery: "SELECT 1", features: "Aggregate"}
	k2 := planCacheKey{databaseID: "db", containerID: "coll", normalizedQuery: "SELECT 1", features: "Distinct"}
	c.Add(k1, []byte(`{"aggregates":["Count"]}`))
	c.Add(k2, []byte(`{"distinctType":"Ordered"}`))
	v1, ok := c.Get(k1)
	require.True(t, ok)
	v2, ok := c.Get(k2)
	require.True(t, ok)
	assert.NotEqual(t, v1, v2, "same query with different features should cache as separate entries")
}

func TestPlanCacheKey_DistinctContainersGivesDistinctEntries(t *testing.T) {
	c := newPlanCache(8)
	require.NotNil(t, c)
	k1 := planCacheKey{databaseID: "db1", containerID: "coll", normalizedQuery: "SELECT 1", features: ""}
	k2 := planCacheKey{databaseID: "db2", containerID: "coll", normalizedQuery: "SELECT 1", features: ""}
	c.Add(k1, []byte("a"))
	c.Add(k2, []byte("b"))
	v1, _ := c.Get(k1)
	v2, _ := c.Get(k2)
	assert.NotEqual(t, v1, v2)
}
