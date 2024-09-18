//go:build go1.18
// +build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
// Code generated by Microsoft (R) AutoRest Code Generator. DO NOT EDIT.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

package armbilling

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
)

// ReservationOrdersClient contains the methods for the ReservationOrders group.
// Don't use this type directly, use NewReservationOrdersClient() instead.
type ReservationOrdersClient struct {
	internal *arm.Client
}

// NewReservationOrdersClient creates a new instance of ReservationOrdersClient with the specified values.
//   - credential - used to authorize requests. Usually a credential from azidentity.
//   - options - pass nil to accept the default values.
func NewReservationOrdersClient(credential azcore.TokenCredential, options *arm.ClientOptions) (*ReservationOrdersClient, error) {
	cl, err := arm.NewClient(moduleName, moduleVersion, credential, options)
	if err != nil {
		return nil, err
	}
	client := &ReservationOrdersClient{
		internal: cl,
	}
	return client, nil
}

// GetByBillingAccount - Get the details of the ReservationOrder in the billing account.
// If the operation fails it returns an *azcore.ResponseError type.
//
// Generated from API version 2024-04-01
//   - billingAccountName - The ID that uniquely identifies a billing account.
//   - reservationOrderID - Order Id of the reservation
//   - options - ReservationOrdersClientGetByBillingAccountOptions contains the optional parameters for the ReservationOrdersClient.GetByBillingAccount
//     method.
func (client *ReservationOrdersClient) GetByBillingAccount(ctx context.Context, billingAccountName string, reservationOrderID string, options *ReservationOrdersClientGetByBillingAccountOptions) (ReservationOrdersClientGetByBillingAccountResponse, error) {
	var err error
	const operationName = "ReservationOrdersClient.GetByBillingAccount"
	ctx = context.WithValue(ctx, runtime.CtxAPINameKey{}, operationName)
	ctx, endSpan := runtime.StartSpan(ctx, operationName, client.internal.Tracer(), nil)
	defer func() { endSpan(err) }()
	req, err := client.getByBillingAccountCreateRequest(ctx, billingAccountName, reservationOrderID, options)
	if err != nil {
		return ReservationOrdersClientGetByBillingAccountResponse{}, err
	}
	httpResp, err := client.internal.Pipeline().Do(req)
	if err != nil {
		return ReservationOrdersClientGetByBillingAccountResponse{}, err
	}
	if !runtime.HasStatusCode(httpResp, http.StatusOK) {
		err = runtime.NewResponseError(httpResp)
		return ReservationOrdersClientGetByBillingAccountResponse{}, err
	}
	resp, err := client.getByBillingAccountHandleResponse(httpResp)
	return resp, err
}

// getByBillingAccountCreateRequest creates the GetByBillingAccount request.
func (client *ReservationOrdersClient) getByBillingAccountCreateRequest(ctx context.Context, billingAccountName string, reservationOrderID string, options *ReservationOrdersClientGetByBillingAccountOptions) (*policy.Request, error) {
	urlPath := "/providers/Microsoft.Billing/billingAccounts/{billingAccountName}/reservationOrders/{reservationOrderId}"
	if billingAccountName == "" {
		return nil, errors.New("parameter billingAccountName cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{billingAccountName}", url.PathEscape(billingAccountName))
	if reservationOrderID == "" {
		return nil, errors.New("parameter reservationOrderID cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{reservationOrderId}", url.PathEscape(reservationOrderID))
	req, err := runtime.NewRequest(ctx, http.MethodGet, runtime.JoinPaths(client.internal.Endpoint(), urlPath))
	if err != nil {
		return nil, err
	}
	reqQP := req.Raw().URL.Query()
	reqQP.Set("api-version", "2024-04-01")
	if options != nil && options.Expand != nil {
		reqQP.Set("expand", *options.Expand)
	}
	req.Raw().URL.RawQuery = reqQP.Encode()
	req.Raw().Header["Accept"] = []string{"application/json"}
	return req, nil
}

// getByBillingAccountHandleResponse handles the GetByBillingAccount response.
func (client *ReservationOrdersClient) getByBillingAccountHandleResponse(resp *http.Response) (ReservationOrdersClientGetByBillingAccountResponse, error) {
	result := ReservationOrdersClientGetByBillingAccountResponse{}
	if err := runtime.UnmarshalAsJSON(resp, &result.ReservationOrder); err != nil {
		return ReservationOrdersClientGetByBillingAccountResponse{}, err
	}
	return result, nil
}

// NewListByBillingAccountPager - List all the `ReservationOrders in the billing account.
//
// Generated from API version 2024-04-01
//   - billingAccountName - The ID that uniquely identifies a billing account.
//   - options - ReservationOrdersClientListByBillingAccountOptions contains the optional parameters for the ReservationOrdersClient.NewListByBillingAccountPager
//     method.
func (client *ReservationOrdersClient) NewListByBillingAccountPager(billingAccountName string, options *ReservationOrdersClientListByBillingAccountOptions) *runtime.Pager[ReservationOrdersClientListByBillingAccountResponse] {
	return runtime.NewPager(runtime.PagingHandler[ReservationOrdersClientListByBillingAccountResponse]{
		More: func(page ReservationOrdersClientListByBillingAccountResponse) bool {
			return page.NextLink != nil && len(*page.NextLink) > 0
		},
		Fetcher: func(ctx context.Context, page *ReservationOrdersClientListByBillingAccountResponse) (ReservationOrdersClientListByBillingAccountResponse, error) {
			ctx = context.WithValue(ctx, runtime.CtxAPINameKey{}, "ReservationOrdersClient.NewListByBillingAccountPager")
			nextLink := ""
			if page != nil {
				nextLink = *page.NextLink
			}
			resp, err := runtime.FetcherForNextLink(ctx, client.internal.Pipeline(), nextLink, func(ctx context.Context) (*policy.Request, error) {
				return client.listByBillingAccountCreateRequest(ctx, billingAccountName, options)
			}, nil)
			if err != nil {
				return ReservationOrdersClientListByBillingAccountResponse{}, err
			}
			return client.listByBillingAccountHandleResponse(resp)
		},
		Tracer: client.internal.Tracer(),
	})
}

// listByBillingAccountCreateRequest creates the ListByBillingAccount request.
func (client *ReservationOrdersClient) listByBillingAccountCreateRequest(ctx context.Context, billingAccountName string, options *ReservationOrdersClientListByBillingAccountOptions) (*policy.Request, error) {
	urlPath := "/providers/Microsoft.Billing/billingAccounts/{billingAccountName}/reservationOrders"
	if billingAccountName == "" {
		return nil, errors.New("parameter billingAccountName cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{billingAccountName}", url.PathEscape(billingAccountName))
	req, err := runtime.NewRequest(ctx, http.MethodGet, runtime.JoinPaths(client.internal.Endpoint(), urlPath))
	if err != nil {
		return nil, err
	}
	reqQP := req.Raw().URL.Query()
	reqQP.Set("api-version", "2024-04-01")
	if options != nil && options.Filter != nil {
		reqQP.Set("filter", *options.Filter)
	}
	if options != nil && options.OrderBy != nil {
		reqQP.Set("orderBy", *options.OrderBy)
	}
	if options != nil && options.Skiptoken != nil {
		reqQP.Set("skiptoken", strconv.FormatFloat(float64(*options.Skiptoken), 'f', -1, 32))
	}
	req.Raw().URL.RawQuery = reqQP.Encode()
	req.Raw().Header["Accept"] = []string{"application/json"}
	return req, nil
}

// listByBillingAccountHandleResponse handles the ListByBillingAccount response.
func (client *ReservationOrdersClient) listByBillingAccountHandleResponse(resp *http.Response) (ReservationOrdersClientListByBillingAccountResponse, error) {
	result := ReservationOrdersClientListByBillingAccountResponse{}
	if err := runtime.UnmarshalAsJSON(resp, &result.ReservationOrderList); err != nil {
		return ReservationOrdersClientListByBillingAccountResponse{}, err
	}
	return result, nil
}
