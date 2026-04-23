// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azcosmos

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/stretchr/testify/assert"
)

// mkResponseError builds an *azcore.ResponseError backed by a real http.Response
// whose body carries the canonical gateway message. The Error() implementation
// reads RawResponse.Body to format its string, so we must provide readable bytes.
func mkResponseError(t *testing.T, statusCode int, body string) *azcore.ResponseError {
	t.Helper()
	reqURL, _ := url.Parse("https://example.documents.azure.com:443/dbs/db/colls/coll/docs")
	resp := &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request: &http.Request{
			Method: "POST",
			URL:    reqURL,
		},
	}
	return &azcore.ResponseError{
		ErrorCode:   "BadRequest",
		StatusCode:  statusCode,
		RawResponse: resp,
	}
}

const xpMsg = `{"code":"BadRequest","message":"The provided cross partition query can not be directly served by the gateway"}`
const xpAggMsg = `{"code":"BadRequest","message":"Cross partition query only supports 'VALUE <AggregateFunc>' for aggregates."}`

func TestIsCrossPartitionUnsupported_GeneralBadRequest(t *testing.T) {
	re := mkResponseError(t, 400, xpMsg)
	assert.True(t, isCrossPartitionUnsupported(re))
}

func TestIsCrossPartitionUnsupported_AggregateBadRequest(t *testing.T) {
	re := mkResponseError(t, 400, xpAggMsg)
	assert.True(t, isCrossPartitionUnsupported(re))
}

func TestIsCrossPartitionUnsupported_Non400(t *testing.T) {
	re := mkResponseError(t, 429, xpMsg)
	assert.False(t, isCrossPartitionUnsupported(re))
}

func TestIsCrossPartitionUnsupported_400WithOtherMessage(t *testing.T) {
	re := mkResponseError(t, 400, `{"code":"BadRequest","message":"some other bad request"}`)
	assert.False(t, isCrossPartitionUnsupported(re))
}

func TestIsCrossPartitionUnsupported_NonResponseError(t *testing.T) {
	err := errors.New("cross partition query can not be directly served by the gateway")
	assert.False(t, isCrossPartitionUnsupported(err))
}

func TestIsCrossPartitionUnsupported_NilError(t *testing.T) {
	assert.False(t, isCrossPartitionUnsupported(nil))
}
