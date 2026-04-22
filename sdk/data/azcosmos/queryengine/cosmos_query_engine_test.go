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
