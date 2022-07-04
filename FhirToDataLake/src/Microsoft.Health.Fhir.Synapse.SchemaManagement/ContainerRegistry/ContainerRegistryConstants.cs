// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------


namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry
{
    public class ContainerRegistryConstants
    {
        /// <summary>
        /// ArmResourceManagerId to acquire AAD token for ACR access token since ACR is not an AAD resource.
        /// The value is "https://management.azure.com/" for AzureCloud and DogFood.
        /// Could be changed to "https://management.usgovcloudapi.net/" for Azure Government and "https://management.chinacloudapi.cn/ " for Azure China.
        /// </summary>
        public const string ArmResourceManagerIdForAzureCloud = "https://management.azure.com/";
    }
}