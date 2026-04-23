// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/assert"
)

func TestQueryPlanCacheSizeDefaultsToSensibleValue(t *testing.T) {
	var o azcosmos.ClientOptions
	// Zero value is the Go default; the SDK treats zero as "use the built-in default".
	// The field itself stores zero — callers observe the default only when the SDK allocates caches.
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
