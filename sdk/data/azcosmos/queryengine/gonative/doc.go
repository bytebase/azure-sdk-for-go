// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

// Package gonative implements queryengine.QueryEngine entirely in Go
// (no cgo, no native plugins, no external toolchain).
//
// It is the default query engine that the azcosmos SDK will auto-wire into
// cross-partition queries starting at Stage 1 of the BYT-9239 roll-out.
// In Stage 0 (this package's first release) the engine is inert:
// SupportedFeatures() returns "" and CreateQueryPipeline returns
// queryengine.ErrUnsupportedPlanFeature unconditionally.
package gonative
