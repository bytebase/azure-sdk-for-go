// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

// Tests in this file are in-package (not _test package) because planCache is
// intentionally unexported — only other files in this package need to see it.

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPlanCacheDefaultSize(t *testing.T) {
	// Passing 0 must resolve to the 256-entry default. We verify by inserting
	// 257 distinct keys and checking the LRU evicted down to 256 — that way
	// the test fails if newPlanCache forgets to apply the default, rather than
	// just asserting a constant against its own literal.
	c := newPlanCache(0)
	require.NotNil(t, c)
	for i := 0; i < defaultQueryPlanCacheSize+1; i++ {
		key := planCacheKey{normalizedQuery: strconv.Itoa(i)}
		c.Add(key, []byte("{}"))
	}
	assert.Equal(t, defaultQueryPlanCacheSize, c.Len(),
		"newPlanCache(0) must produce a cache bounded at defaultQueryPlanCacheSize")
}

func TestNewPlanCacheExplicitSize(t *testing.T) {
	c := newPlanCache(64)
	require.NotNil(t, c)
	assert.Equal(t, 0, c.Len())
}

func TestNewPlanCacheDisabled(t *testing.T) {
	c := newPlanCache(-1)
	assert.Nil(t, c, "negative size should disable the cache entirely")
}

func TestPlanCacheRoundTrip(t *testing.T) {
	c := newPlanCache(4)
	require.NotNil(t, c)
	key := planCacheKey{
		databaseID:      "db",
		containerID:     "coll",
		normalizedQuery: "SELECT 1",
		features:        "",
	}
	payload := []byte(`{"partitionedQueryExecutionInfoVersion":2}`)
	c.Add(key, payload)
	got, ok := c.Get(key)
	require.True(t, ok)
	assert.Equal(t, payload, got)
}
