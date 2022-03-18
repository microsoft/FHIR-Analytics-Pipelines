// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------
using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    /// <summary>
    /// Fhir data source from FHIR search API.
    /// </summary>
    public class FhirApiDataSource : IFhirApiDataSource
    {
        public FhirApiDataSource(IOptions<FhirServerConfiguration> config)
        {
            EnsureArg.IsNotNull(config, nameof(config));
            EnsureArg.IsNotNullOrEmpty(config.Value.ServerUrl, nameof(config.Value.ServerUrl));

            FhirServerUrl = config.Value.ServerUrl;
            Authentication = config.Value.Authentication;
        }

        public string FhirServerUrl { get; }

        public AuthenticationType Authentication { get; }
    }
}
