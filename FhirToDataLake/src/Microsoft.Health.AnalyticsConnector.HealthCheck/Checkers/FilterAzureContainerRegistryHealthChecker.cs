// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Logging;
using Microsoft.Health.AnalyticsConnector.SchemaManagement.ContainerRegistry;

namespace Microsoft.Health.AnalyticsConnector.HealthCheck.Checkers
{
    public class FilterAzureContainerRegistryHealthChecker : AzureContainerRegistryHealthChecker
    {
        public FilterAzureContainerRegistryHealthChecker(
            IOptions<FilterLocation> filterLocation,
            IContainerRegistryTokenProvider containerRegistryTokenProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<AzureContainerRegistryHealthChecker> logger)
            : base("Filter", filterLocation?.Value?.FilterImageReference, containerRegistryTokenProvider, diagnosticLogger, logger)
        {
        }
    }
}
