//go:build go1.18
// +build go1.18

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.
// DO NOT EDIT.

package armnetwork_test

import (
	"context"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2"
)

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/expressRouteProviderPortList.json
func ExampleExpressRouteProviderPortsLocationClient_List() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	res, err := clientFactory.NewExpressRouteProviderPortsLocationClient().List(ctx, &armnetwork.ExpressRouteProviderPortsLocationClientListOptions{Filter: nil})
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteProviderPortListResult = armnetwork.ExpressRouteProviderPortListResult{
	// 	Value: []*armnetwork.ExpressRouteProviderPort{
	// 		{
	// 			Type: to.Ptr("Microsoft.Network/expressRouteProviderPort"),
	// 			ID: to.Ptr("/subscriptions/subid/providers/Microsoft.Network/ExpressRoutePortsLocations/SiliconValley/bvtazureixpportpair1"),
	// 			Location: to.Ptr("uswest"),
	// 			Etag: to.Ptr("W/\"c0e6477e-8150-4d4f-9bf6-bb10e6acb63a\""),
	// 			Properties: &armnetwork.ExpressRouteProviderPortProperties{
	// 				OverprovisionFactor: to.Ptr[int32](4),
	// 				PeeringLocation: to.Ptr("SiliconValley"),
	// 				PortBandwidthInMbps: to.Ptr[int32](4000),
	// 				PortPairDescriptor: to.Ptr("bvtazureixpportpair1"),
	// 				PrimaryAzurePort: to.Ptr("bvtazureixp01"),
	// 				RemainingBandwidthInMbps: to.Ptr[int32](1500),
	// 				SecondaryAzurePort: to.Ptr("bvtazureixp01"),
	// 				UsedBandwidthInMbps: to.Ptr[int32](2500),
	// 			},
	// 		},
	// 		{
	// 			Type: to.Ptr("Microsoft.Network/expressRouteProviderPort"),
	// 			ID: to.Ptr("/subscriptions/subid/providers/Microsoft.Network/ExpressRoutePortsLocations/SiliconValley/bvtazureixpportpair2"),
	// 			Location: to.Ptr("uswest"),
	// 			Etag: to.Ptr("W/\"c0e6477e-8150-4d4f-9bf6-bb10e6acb63a\""),
	// 			Properties: &armnetwork.ExpressRouteProviderPortProperties{
	// 				OverprovisionFactor: to.Ptr[int32](4),
	// 				PeeringLocation: to.Ptr("seattle"),
	// 				PortBandwidthInMbps: to.Ptr[int32](4000),
	// 				PortPairDescriptor: to.Ptr("bvtazureixpportpair2"),
	// 				PrimaryAzurePort: to.Ptr("bvtazureixp02"),
	// 				RemainingBandwidthInMbps: to.Ptr[int32](1800),
	// 				SecondaryAzurePort: to.Ptr("bvtazureixp02"),
	// 				UsedBandwidthInMbps: to.Ptr[int32](1200),
	// 			},
	// 	}},
	// }
}
