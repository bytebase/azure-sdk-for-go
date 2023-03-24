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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v2"
)

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayListBySubscription.json
func ExampleExpressRouteGatewaysClient_ListBySubscription() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	res, err := clientFactory.NewExpressRouteGatewaysClient().ListBySubscription(ctx, nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteGatewayList = armnetwork.ExpressRouteGatewayList{
	// 	Value: []*armnetwork.ExpressRouteGateway{
	// 		{
	// 			Name: to.Ptr("expressRouteGatewayName"),
	// 			Type: to.Ptr("Microsoft.Network/expressRouteGateways"),
	// 			ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName"),
	// 			Location: to.Ptr("westus"),
	// 			Etag: to.Ptr("w/\\00000000-0000-0000-0000-000000000000\\"),
	// 			Properties: &armnetwork.ExpressRouteGatewayProperties{
	// 				AllowNonVirtualWanTraffic: to.Ptr(false),
	// 				AutoScaleConfiguration: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfiguration{
	// 					Bounds: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfigurationBounds{
	// 						Min: to.Ptr[int32](2),
	// 					},
	// 				},
	// 				ExpressRouteConnections: []*armnetwork.ExpressRouteConnection{
	// 					{
	// 						ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName/expressRouteConnections/connectionName"),
	// 						Name: to.Ptr("connectionName"),
	// 						Properties: &armnetwork.ExpressRouteConnectionProperties{
	// 							AuthorizationKey: to.Ptr("f28e9c99-78d8-4248-a855-c54cf6beb99d"),
	// 							EnableInternetSecurity: to.Ptr(false),
	// 							ExpressRouteCircuitPeering: &armnetwork.ExpressRouteCircuitPeeringID{
	// 								ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteCircuits/circuitName/peerings/AzurePrivatePeering"),
	// 							},
	// 							ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 							RoutingConfiguration: &armnetwork.RoutingConfiguration{
	// 								AssociatedRouteTable: &armnetwork.SubResource{
	// 									ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable1"),
	// 								},
	// 								PropagatedRouteTables: &armnetwork.PropagatedRouteTable{
	// 									IDs: []*armnetwork.SubResource{
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable1"),
	// 										},
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable2"),
	// 										},
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/hvirtualHubNameub1/hubRouteTables/hubRouteTable3"),
	// 									}},
	// 									Labels: []*string{
	// 										to.Ptr("label1"),
	// 										to.Ptr("label2")},
	// 									},
	// 									VnetRoutes: &armnetwork.VnetRoute{
	// 										StaticRoutes: []*armnetwork.StaticRoute{
	// 										},
	// 									},
	// 								},
	// 								RoutingWeight: to.Ptr[int32](1),
	// 							},
	// 					}},
	// 					ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 					VirtualHub: &armnetwork.VirtualHubID{
	// 						ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName"),
	// 					},
	// 				},
	// 		}},
	// 	}
}

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayListByResourceGroup.json
func ExampleExpressRouteGatewaysClient_ListByResourceGroup() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	res, err := clientFactory.NewExpressRouteGatewaysClient().ListByResourceGroup(ctx, "resourceGroupName", nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteGatewayList = armnetwork.ExpressRouteGatewayList{
	// 	Value: []*armnetwork.ExpressRouteGateway{
	// 		{
	// 			Name: to.Ptr("expressRouteGatewayName"),
	// 			Type: to.Ptr("Microsoft.Network/expressRouteGateways"),
	// 			ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName"),
	// 			Location: to.Ptr("westus"),
	// 			Etag: to.Ptr("w/\\00000000-0000-0000-0000-000000000000\\"),
	// 			Properties: &armnetwork.ExpressRouteGatewayProperties{
	// 				AllowNonVirtualWanTraffic: to.Ptr(false),
	// 				AutoScaleConfiguration: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfiguration{
	// 					Bounds: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfigurationBounds{
	// 						Min: to.Ptr[int32](2),
	// 					},
	// 				},
	// 				ExpressRouteConnections: []*armnetwork.ExpressRouteConnection{
	// 					{
	// 						ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName/expressRouteConnections/connectionName"),
	// 						Name: to.Ptr("connectionName"),
	// 						Properties: &armnetwork.ExpressRouteConnectionProperties{
	// 							AuthorizationKey: to.Ptr("f28e9c99-78d8-4248-a855-c54cf6beb99d"),
	// 							EnableInternetSecurity: to.Ptr(false),
	// 							ExpressRouteCircuitPeering: &armnetwork.ExpressRouteCircuitPeeringID{
	// 								ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteCircuits/circuitName/peerings/AzurePrivatePeering"),
	// 							},
	// 							ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 							RoutingConfiguration: &armnetwork.RoutingConfiguration{
	// 								AssociatedRouteTable: &armnetwork.SubResource{
	// 									ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable1"),
	// 								},
	// 								PropagatedRouteTables: &armnetwork.PropagatedRouteTable{
	// 									IDs: []*armnetwork.SubResource{
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable1"),
	// 										},
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName/hubRouteTables/hubRouteTable2"),
	// 										},
	// 										{
	// 											ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/hvirtualHubNameub1/hubRouteTables/hubRouteTable3"),
	// 									}},
	// 									Labels: []*string{
	// 										to.Ptr("label1"),
	// 										to.Ptr("label2")},
	// 									},
	// 									VnetRoutes: &armnetwork.VnetRoute{
	// 										StaticRoutes: []*armnetwork.StaticRoute{
	// 										},
	// 									},
	// 								},
	// 								RoutingWeight: to.Ptr[int32](1),
	// 							},
	// 					}},
	// 					ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 					VirtualHub: &armnetwork.VirtualHubID{
	// 						ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName"),
	// 					},
	// 				},
	// 		}},
	// 	}
}

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayCreate.json
func ExampleExpressRouteGatewaysClient_BeginCreateOrUpdate() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	poller, err := clientFactory.NewExpressRouteGatewaysClient().BeginCreateOrUpdate(ctx, "resourceGroupName", "gateway-2", armnetwork.ExpressRouteGateway{
		Location: to.Ptr("westus"),
		Properties: &armnetwork.ExpressRouteGatewayProperties{
			AllowNonVirtualWanTraffic: to.Ptr(false),
			AutoScaleConfiguration: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfiguration{
				Bounds: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfigurationBounds{
					Min: to.Ptr[int32](3),
				},
			},
			VirtualHub: &armnetwork.VirtualHubID{
				ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupId/providers/Microsoft.Network/virtualHubs/virtualHubName"),
			},
		},
	}, nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	res, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		log.Fatalf("failed to pull the result: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteGateway = armnetwork.ExpressRouteGateway{
	// 	Name: to.Ptr("gateway-2"),
	// 	Type: to.Ptr("Microsoft.Network/expressRouteGateways"),
	// 	ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/gateway-2"),
	// 	Location: to.Ptr("westus"),
	// 	Etag: to.Ptr("w/\\00000000-0000-0000-0000-000000000000\\"),
	// 	Properties: &armnetwork.ExpressRouteGatewayProperties{
	// 		AllowNonVirtualWanTraffic: to.Ptr(false),
	// 		AutoScaleConfiguration: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfiguration{
	// 			Bounds: &armnetwork.ExpressRouteGatewayPropertiesAutoScaleConfigurationBounds{
	// 				Min: to.Ptr[int32](3),
	// 			},
	// 		},
	// 		ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 		VirtualHub: &armnetwork.VirtualHubID{
	// 			ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName"),
	// 		},
	// 	},
	// }
}

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayUpdateTags.json
func ExampleExpressRouteGatewaysClient_BeginUpdateTags() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	poller, err := clientFactory.NewExpressRouteGatewaysClient().BeginUpdateTags(ctx, "resourceGroupName", "expressRouteGatewayName", armnetwork.TagsObject{
		Tags: map[string]*string{
			"tag1": to.Ptr("value1"),
			"tag2": to.Ptr("value2"),
		},
	}, nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	res, err := poller.PollUntilDone(ctx, nil)
	if err != nil {
		log.Fatalf("failed to pull the result: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteGateway = armnetwork.ExpressRouteGateway{
	// 	Name: to.Ptr("expressRouteGatewayName"),
	// 	Type: to.Ptr("Microsoft.Network/expressRouteGateways"),
	// 	ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName"),
	// 	Location: to.Ptr("westus"),
	// 	Tags: map[string]*string{
	// 		"tag1": to.Ptr("value1"),
	// 		"tag2": to.Ptr("value2"),
	// 	},
	// 	Etag: to.Ptr("w/\\00000000-0000-0000-0000-000000000000\\"),
	// 	Properties: &armnetwork.ExpressRouteGatewayProperties{
	// 		AllowNonVirtualWanTraffic: to.Ptr(false),
	// 		ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 		VirtualHub: &armnetwork.VirtualHubID{
	// 			ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName"),
	// 		},
	// 	},
	// }
}

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayGet.json
func ExampleExpressRouteGatewaysClient_Get() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	res, err := clientFactory.NewExpressRouteGatewaysClient().Get(ctx, "resourceGroupName", "expressRouteGatewayName", nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	// You could use response here. We use blank identifier for just demo purposes.
	_ = res
	// If the HTTP response code is 200 as defined in example definition, your response structure would look as follows. Please pay attention that all the values in the output are fake values for just demo purposes.
	// res.ExpressRouteGateway = armnetwork.ExpressRouteGateway{
	// 	Name: to.Ptr("expressRouteGatewayName"),
	// 	Type: to.Ptr("Microsoft.Network/expressRouteGateways"),
	// 	ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/expressRouteGateways/expressRouteGatewayName"),
	// 	Location: to.Ptr("westus"),
	// 	Etag: to.Ptr("w/\\00000000-0000-0000-0000-000000000000\\"),
	// 	Properties: &armnetwork.ExpressRouteGatewayProperties{
	// 		AllowNonVirtualWanTraffic: to.Ptr(false),
	// 		ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
	// 		VirtualHub: &armnetwork.VirtualHubID{
	// 			ID: to.Ptr("/subscriptions/subid/resourceGroups/resourceGroupName/providers/Microsoft.Network/virtualHubs/virtualHubName"),
	// 		},
	// 	},
	// }
}

// Generated from example definition: https://github.com/Azure/azure-rest-api-specs/blob/a60468a0c5e2beb054680ae488fb9f92699f0a0d/specification/network/resource-manager/Microsoft.Network/stable/2022-09-01/examples/ExpressRouteGatewayDelete.json
func ExampleExpressRouteGatewaysClient_BeginDelete() {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Fatalf("failed to obtain a credential: %v", err)
	}
	ctx := context.Background()
	clientFactory, err := armnetwork.NewClientFactory("<subscription-id>", cred, nil)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	poller, err := clientFactory.NewExpressRouteGatewaysClient().BeginDelete(ctx, "resourceGroupName", "expressRouteGatewayName", nil)
	if err != nil {
		log.Fatalf("failed to finish the request: %v", err)
	}
	_, err = poller.PollUntilDone(ctx, nil)
	if err != nil {
		log.Fatalf("failed to pull the result: %v", err)
	}
}
