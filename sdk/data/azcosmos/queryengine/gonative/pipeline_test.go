// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine/gonative"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustReadFile loads a fixture path and fails the test on error.
func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err, "read %s", path)
	return b
}

// partitionBodies returns a map of pk-range-id -> response body for each file
// found under `dir`. Expects filenames of the form `range_<id>.json`.
func partitionBodies(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "read dir %s", dir)
	out := make(map[string][]byte, len(entries))
	for _, e := range entries {
		name := e.Name()
		// file is `range_<id>.json` — strip prefix/suffix
		id := name[len("range_") : len(name)-len(".json")]
		out[id] = mustReadFile(t, filepath.Join(dir, name))
	}
	return out
}

// driveAggPipeline runs a pipeline end-to-end against captured fixtures and
// returns the single emitted row.
func driveAggPipeline(t *testing.T, queryID, originalQuery string) json.RawMessage {
	t.Helper()
	base := filepath.Join("testdata", "query_"+queryID)
	plan := mustReadFile(t, base+"_plan.json")
	pkranges := mustReadFile(t, base+"_pkranges.json")
	bodies := partitionBodies(t, base+"_partitions")

	engine := gonative.Default()
	pipe, err := engine.CreateQueryPipeline(originalQuery, string(plan), string(pkranges))
	require.NoError(t, err, "CreateQueryPipeline")
	defer pipe.Close()

	var emitted json.RawMessage
	for i := 0; i < 10 && !pipe.IsComplete(); i++ { // bounded loop — 10 turns is a safe upper bound for single-partition aggregates
		res, err := pipe.Run()
		require.NoError(t, err, "Run turn %d", i)
		if len(res.Requests) > 0 {
			results := make([]queryengine.QueryResult, 0, len(res.Requests))
			for _, req := range res.Requests {
				body, ok := bodies[req.PartitionKeyRangeID]
				require.True(t, ok, "missing fixture for pk-range %s", req.PartitionKeyRangeID)
				results = append(results, queryengine.NewQueryResult(req.PartitionKeyRangeID, body, ""))
			}
			require.NoError(t, pipe.ProvideData(results))
		}
		if len(res.Items) > 0 {
			require.Len(t, res.Items, 1, "aggregate query emits exactly one row")
			emitted = append(json.RawMessage(nil), res.Items[0]...)
		}
	}
	require.True(t, pipe.IsComplete(), "pipeline did not converge")
	require.NotNil(t, emitted, "no row emitted")
	return emitted
}

func TestAggPipeline_5_1_AliasedCount(t *testing.T) {
	row := driveAggPipeline(t, "5_1", `SELECT COUNT(1) AS totalRecords FROM c`)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(row, &obj))
	assert.EqualValues(t, 18, obj["totalRecords"])
}

func TestAggPipeline_5_1b_ValueCount(t *testing.T) {
	row := driveAggPipeline(t, "5_1b", `SELECT VALUE COUNT(1) FROM c`)
	// VALUE form emits the scalar directly.
	var n float64
	require.NoError(t, json.Unmarshal(row, &n))
	assert.EqualValues(t, 18, n)
}

func TestAggPipeline_5_2_AliasedCountWithFilter(t *testing.T) {
	row := driveAggPipeline(t, "5_2", `SELECT COUNT(1) AS aeCount FROM c WHERE c.country = "AE"`)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(row, &obj))
	assert.EqualValues(t, 5, obj["aeCount"])
}

func TestAggPipeline_5_2b_ValueCountWithFilter(t *testing.T) {
	row := driveAggPipeline(t, "5_2b", `SELECT VALUE COUNT(1) FROM c WHERE c.country = "AE"`)
	var n float64
	require.NoError(t, json.Unmarshal(row, &n))
	assert.EqualValues(t, 5, n)
}

func TestAggPipeline_5_3_AliasedSum(t *testing.T) {
	row := driveAggPipeline(t, "5_3", `SELECT SUM(StringToNumber(c.population)) AS totalPop FROM c`)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(row, &obj))
	assert.Positive(t, obj["totalPop"])
}

func TestAggPipeline_5_3b_ValueSum(t *testing.T) {
	row := driveAggPipeline(t, "5_3b", `SELECT VALUE SUM(StringToNumber(c.population)) FROM c`)
	var n float64
	require.NoError(t, json.Unmarshal(row, &n))
	assert.Positive(t, n)
}

func TestAggPipeline_5_4_AliasedAvg(t *testing.T) {
	row := driveAggPipeline(t, "5_4", `SELECT AVG(StringToNumber(c.population)) AS avgPop FROM c`)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(row, &obj))
	assert.Positive(t, obj["avgPop"])
}

func TestAggPipeline_5_4b_ValueAvg(t *testing.T) {
	row := driveAggPipeline(t, "5_4b", `SELECT VALUE AVG(StringToNumber(c.population)) FROM c`)
	var n float64
	require.NoError(t, json.Unmarshal(row, &n))
	assert.Positive(t, n)
}

func TestAggPipeline_5_5_MultiAggMinMax(t *testing.T) {
	row := driveAggPipeline(t, "5_5", `SELECT MIN(StringToNumber(c.population)) AS minPop, MAX(StringToNumber(c.population)) AS maxPop FROM c WHERE StringToNumber(c.population) > 0`)
	// Aliased multi-agg preserves alias order from the plan.
	assert.Contains(t, string(row), `"minPop"`)
	assert.Contains(t, string(row), `"maxPop"`)
	// Order assertion: minPop appears before maxPop because the plan lists it first.
	idxMin := indexOf(row, []byte(`"minPop"`))
	idxMax := indexOf(row, []byte(`"maxPop"`))
	assert.Less(t, idxMin, idxMax, "aliases emitted in plan order")

	var obj map[string]any
	require.NoError(t, json.Unmarshal(row, &obj))
	minV, ok := obj["minPop"].(float64)
	require.True(t, ok)
	maxV, ok := obj["maxPop"].(float64)
	require.True(t, ok)
	assert.LessOrEqual(t, minV, maxV)
}

func TestAggPipeline_11_2_ValueCount(t *testing.T) {
	row := driveAggPipeline(t, "11_2", `SELECT VALUE COUNT(1) FROM c`)
	var n float64
	require.NoError(t, json.Unmarshal(row, &n))
	assert.EqualValues(t, 18, n)
}

// indexOf is a tiny helper around bytes.Index to keep the test file imports minimal.
func indexOf(haystack, needle []byte) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := range needle {
			if haystack[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
