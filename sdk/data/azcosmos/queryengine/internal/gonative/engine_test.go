// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package gonative_test

import (
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos/queryengine/internal/gonative"
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
