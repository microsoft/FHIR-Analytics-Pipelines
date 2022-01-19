// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public interface IFhirApiDataSource
    {
        /// <summary>
        /// Server url.
        /// </summary>
        public string FhirServerUrl { get; }

        /// <summary>
        /// Authentication method to access FHIR server.
        /// </summary>
        public AuthenticationType Authentication { get; }
    }
}
