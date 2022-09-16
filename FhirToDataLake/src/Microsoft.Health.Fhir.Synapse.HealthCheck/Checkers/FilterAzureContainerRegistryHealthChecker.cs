// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.ContainerRegistry;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Checkers
{
    public class FilterAzureContainerRegistryHealthChecker : AzureContainerRegistryHealthChecker
    {
        public FilterAzureContainerRegistryHealthChecker(
            IOptions<FilterLocation> filterLocation,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            ILogger<AzureContainerRegistryHealthChecker> logger)
            : base("Filter", filterLocation?.Value?.FilterImageReference, containerRegistryTokenProvider, logger)
        {
        }
    }
}
