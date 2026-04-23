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

// Compile-time check that *Engine satisfies the interface.
var _ queryengine.QueryEngine = (*Engine)(nil)

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
