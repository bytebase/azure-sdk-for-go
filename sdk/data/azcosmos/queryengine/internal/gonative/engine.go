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

// stage1Features enumerates the gateway-known feature names Stage 1 can serve:
//
//	Aggregate           — VALUE-form and aliased-form aggregate queries
//	NonValueAggregate   — aliased-form (SELECT agg(...) AS alias FROM c)
//	MultipleAggregates  — multi-aggregate in a single SELECT (query 5.5)
const stage1Features = "Aggregate,NonValueAggregate,MultipleAggregates"

// SupportedFeatures implements queryengine.QueryEngine.
func (e *Engine) SupportedFeatures() string {
	return stage1Features
}

// CreateQueryPipeline implements queryengine.QueryEngine. It parses the plan,
// rejects any feature Stage 1 does not yet support, then dispatches to either
// the VALUE-form or aliased-form aggregate pipeline.
func (e *Engine) CreateQueryPipeline(query string, plan string, pkranges string) (queryengine.QueryPipeline, error) {
	p, err := parsePlan([]byte(plan))
	if err != nil {
		return nil, err
	}
	if err := rejectUnsupportedPlan(p); err != nil {
		return nil, err
	}
	rangeIDs, err := parsePKRangeIDs([]byte(pkranges))
	if err != nil {
		return nil, err
	}
	switch {
	case len(p.QueryInfo.Aggregates) > 0:
		pipe, err := newValuePipeline(p, rangeIDs)
		if err != nil {
			return nil, err
		}
		return pipe, nil
	case len(p.QueryInfo.GroupByAliasToAggregateType) > 0:
		pipe, err := newAliasedPipeline(p, rangeIDs)
		if err != nil {
			return nil, err
		}
		return pipe, nil
	default:
		return nil, queryengine.ErrUnsupportedPlanFeature
	}
}

// rejectUnsupportedPlan returns ErrUnsupportedPlanFeature when the plan uses
// any feature beyond Stage 1's scope. Later stages will prune entries from
// this guard as their operators come online.
func rejectUnsupportedPlan(p *planDoc) error {
	qi := p.QueryInfo
	if qi.DistinctType != "" && qi.DistinctType != "None" {
		return queryengine.ErrUnsupportedPlanFeature
	}
	if len(qi.OrderBy) > 0 || len(qi.GroupByExpressions) > 0 {
		return queryengine.ErrUnsupportedPlanFeature
	}
	if qi.Top != nil || qi.Offset != nil || qi.Limit != nil {
		return queryengine.ErrUnsupportedPlanFeature
	}
	if qi.HasNonStreamingOrderBy {
		return queryengine.ErrUnsupportedPlanFeature
	}
	return nil
}

// CreateReadManyPipeline implements queryengine.QueryEngine.
func (e *Engine) CreateReadManyPipeline(_ []queryengine.ItemIdentity, _ string, _ string, _ uint8, _ []string) (queryengine.QueryPipeline, error) {
	return nil, queryengine.ErrUnsupportedPlanFeature
}
