// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using EnsureThat;
using Microsoft.Health.Fhir.Synapse.DataClient.Api.Fhir;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Models.FhirApiOption
{
    public class ResourceIdSearchOptions : BaseSearchOptions
    {
        public ResourceIdSearchOptions(
            string resourceType,
            string resourceId,
            List<KeyValuePair<string, string>> queryParameters)
            : base(resourceType, queryParameters)
        {
            ResourceId = EnsureArg.IsNotNullOrEmpty(resourceId, nameof(resourceId));

            QueryParameters.Add(new KeyValuePair<string, string>(FhirApiConstants.IdKey, ResourceId));
        }

        public string ResourceId { get; }
    }
}
