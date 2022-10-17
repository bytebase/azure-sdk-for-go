//go:build go1.18
// +build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.
// DO NOT EDIT.

package armsecurity

import (
	"context"
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	armruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"net/http"
	"net/url"
	"strings"
)

// SettingsClient contains the methods for the Settings group.
// Don't use this type directly, use NewSettingsClient() instead.
type SettingsClient struct {
	host           string
	subscriptionID string
	pl             runtime.Pipeline
}

// NewSettingsClient creates a new instance of SettingsClient with the specified values.
// subscriptionID - Azure subscription ID
// credential - used to authorize requests. Usually a credential from azidentity.
// options - pass nil to accept the default values.
func NewSettingsClient(subscriptionID string, credential azcore.TokenCredential, options *arm.ClientOptions) (*SettingsClient, error) {
	if options == nil {
		options = &arm.ClientOptions{}
	}
	ep := cloud.AzurePublic.Services[cloud.ResourceManager].Endpoint
	if c, ok := options.Cloud.Services[cloud.ResourceManager]; ok {
		ep = c.Endpoint
	}
	pl, err := armruntime.NewPipeline(moduleName, moduleVersion, credential, runtime.PipelineOptions{}, options)
	if err != nil {
		return nil, err
	}
	client := &SettingsClient{
		subscriptionID: subscriptionID,
		host:           ep,
		pl:             pl,
	}
	return client, nil
}

// Get - Settings of different configurations in Microsoft Defender for Cloud
// If the operation fails it returns an *azcore.ResponseError type.
// Generated from API version 2022-05-01
// settingName - The name of the setting
// options - SettingsClientGetOptions contains the optional parameters for the SettingsClient.Get method.
func (client *SettingsClient) Get(ctx context.Context, settingName SettingName, options *SettingsClientGetOptions) (SettingsClientGetResponse, error) {
	req, err := client.getCreateRequest(ctx, settingName, options)
	if err != nil {
		return SettingsClientGetResponse{}, err
	}
	resp, err := client.pl.Do(req)
	if err != nil {
		return SettingsClientGetResponse{}, err
	}
	if !runtime.HasStatusCode(resp, http.StatusOK) {
		return SettingsClientGetResponse{}, runtime.NewResponseError(resp)
	}
	return client.getHandleResponse(resp)
}

// getCreateRequest creates the Get request.
func (client *SettingsClient) getCreateRequest(ctx context.Context, settingName SettingName, options *SettingsClientGetOptions) (*policy.Request, error) {
	urlPath := "/subscriptions/{subscriptionId}/providers/Microsoft.Security/settings/{settingName}"
	if client.subscriptionID == "" {
		return nil, errors.New("parameter client.subscriptionID cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{subscriptionId}", url.PathEscape(client.subscriptionID))
	if settingName == "" {
		return nil, errors.New("parameter settingName cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{settingName}", url.PathEscape(string(settingName)))
	req, err := runtime.NewRequest(ctx, http.MethodGet, runtime.JoinPaths(client.host, urlPath))
	if err != nil {
		return nil, err
	}
	reqQP := req.Raw().URL.Query()
	reqQP.Set("api-version", "2022-05-01")
	req.Raw().URL.RawQuery = reqQP.Encode()
	req.Raw().Header["Accept"] = []string{"application/json"}
	return req, nil
}

// getHandleResponse handles the Get response.
func (client *SettingsClient) getHandleResponse(resp *http.Response) (SettingsClientGetResponse, error) {
	result := SettingsClientGetResponse{}
	if err := runtime.UnmarshalAsJSON(resp, &result); err != nil {
		return SettingsClientGetResponse{}, err
	}
	return result, nil
}

// NewListPager - Settings about different configurations in Microsoft Defender for Cloud
// Generated from API version 2022-05-01
// options - SettingsClientListOptions contains the optional parameters for the SettingsClient.List method.
func (client *SettingsClient) NewListPager(options *SettingsClientListOptions) *runtime.Pager[SettingsClientListResponse] {
	return runtime.NewPager(runtime.PagingHandler[SettingsClientListResponse]{
		More: func(page SettingsClientListResponse) bool {
			return page.NextLink != nil && len(*page.NextLink) > 0
		},
		Fetcher: func(ctx context.Context, page *SettingsClientListResponse) (SettingsClientListResponse, error) {
			var req *policy.Request
			var err error
			if page == nil {
				req, err = client.listCreateRequest(ctx, options)
			} else {
				req, err = runtime.NewRequest(ctx, http.MethodGet, *page.NextLink)
			}
			if err != nil {
				return SettingsClientListResponse{}, err
			}
			resp, err := client.pl.Do(req)
			if err != nil {
				return SettingsClientListResponse{}, err
			}
			if !runtime.HasStatusCode(resp, http.StatusOK) {
				return SettingsClientListResponse{}, runtime.NewResponseError(resp)
			}
			return client.listHandleResponse(resp)
		},
	})
}

// listCreateRequest creates the List request.
func (client *SettingsClient) listCreateRequest(ctx context.Context, options *SettingsClientListOptions) (*policy.Request, error) {
	urlPath := "/subscriptions/{subscriptionId}/providers/Microsoft.Security/settings"
	if client.subscriptionID == "" {
		return nil, errors.New("parameter client.subscriptionID cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{subscriptionId}", url.PathEscape(client.subscriptionID))
	req, err := runtime.NewRequest(ctx, http.MethodGet, runtime.JoinPaths(client.host, urlPath))
	if err != nil {
		return nil, err
	}
	reqQP := req.Raw().URL.Query()
	reqQP.Set("api-version", "2022-05-01")
	req.Raw().URL.RawQuery = reqQP.Encode()
	req.Raw().Header["Accept"] = []string{"application/json"}
	return req, nil
}

// listHandleResponse handles the List response.
func (client *SettingsClient) listHandleResponse(resp *http.Response) (SettingsClientListResponse, error) {
	result := SettingsClientListResponse{}
	if err := runtime.UnmarshalAsJSON(resp, &result.SettingsList); err != nil {
		return SettingsClientListResponse{}, err
	}
	return result, nil
}

// Update - updating settings about different configurations in Microsoft Defender for Cloud
// If the operation fails it returns an *azcore.ResponseError type.
// Generated from API version 2022-05-01
// settingName - The name of the setting
// setting - Setting object
// options - SettingsClientUpdateOptions contains the optional parameters for the SettingsClient.Update method.
func (client *SettingsClient) Update(ctx context.Context, settingName SettingName, setting SettingClassification, options *SettingsClientUpdateOptions) (SettingsClientUpdateResponse, error) {
	req, err := client.updateCreateRequest(ctx, settingName, setting, options)
	if err != nil {
		return SettingsClientUpdateResponse{}, err
	}
	resp, err := client.pl.Do(req)
	if err != nil {
		return SettingsClientUpdateResponse{}, err
	}
	if !runtime.HasStatusCode(resp, http.StatusOK) {
		return SettingsClientUpdateResponse{}, runtime.NewResponseError(resp)
	}
	return client.updateHandleResponse(resp)
}

// updateCreateRequest creates the Update request.
func (client *SettingsClient) updateCreateRequest(ctx context.Context, settingName SettingName, setting SettingClassification, options *SettingsClientUpdateOptions) (*policy.Request, error) {
	urlPath := "/subscriptions/{subscriptionId}/providers/Microsoft.Security/settings/{settingName}"
	if client.subscriptionID == "" {
		return nil, errors.New("parameter client.subscriptionID cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{subscriptionId}", url.PathEscape(client.subscriptionID))
	if settingName == "" {
		return nil, errors.New("parameter settingName cannot be empty")
	}
	urlPath = strings.ReplaceAll(urlPath, "{settingName}", url.PathEscape(string(settingName)))
	req, err := runtime.NewRequest(ctx, http.MethodPut, runtime.JoinPaths(client.host, urlPath))
	if err != nil {
		return nil, err
	}
	reqQP := req.Raw().URL.Query()
	reqQP.Set("api-version", "2022-05-01")
	req.Raw().URL.RawQuery = reqQP.Encode()
	req.Raw().Header["Accept"] = []string{"application/json"}
	return req, runtime.MarshalAsJSON(req, setting)
}

// updateHandleResponse handles the Update response.
func (client *SettingsClient) updateHandleResponse(resp *http.Response) (SettingsClientUpdateResponse, error) {
	result := SettingsClientUpdateResponse{}
	if err := runtime.UnmarshalAsJSON(resp, &result); err != nil {
		return SettingsClientUpdateResponse{}, err
	}
	return result, nil
}