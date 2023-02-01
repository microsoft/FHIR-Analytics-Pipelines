// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using EnsureThat;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Models.FhirApiOption
{
    public class CompartmentSearchOptions : BaseSearchOptions
    {
        // The ResourceType here could be *, which means get all the resource types
        public CompartmentSearchOptions(
            string compartmentType,
            string compartmentId,
            string resourceType,
            List<KeyValuePair<string, string>> queryParameters)
            : base(resourceType, queryParameters)
        {
            EnsureArg.IsNotNullOrEmpty(compartmentType, nameof(compartmentType));
            EnsureArg.IsNotNullOrEmpty(compartmentId, nameof(compartmentId));

            CompartmentType = compartmentType;
            CompartmentId = compartmentId;
        }

        public string CompartmentType { get; set; }

        public string CompartmentId { get; set; }

        public override string RelativeUri()
        {
            return $"{CompartmentType}/{CompartmentId}/{ResourceType}";
        }
    }
}
