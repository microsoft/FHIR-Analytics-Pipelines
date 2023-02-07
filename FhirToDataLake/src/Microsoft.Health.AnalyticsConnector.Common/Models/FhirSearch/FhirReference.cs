// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;

namespace Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch
{
    /// <summary>
    /// Represents a Fhir reference.
    /// </summary>
    public class FhirReference
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FhirReference"/> class.
        /// </summary>
        /// <param name="resourceType">The resource type.</param>
        /// <param name="resourceId">The resource id.</param>
        public FhirReference(string resourceType, string resourceId)
        {
            ResourceType = EnsureArg.IsNotNullOrWhiteSpace(resourceType, nameof(resourceType));
            ResourceId = EnsureArg.IsNotNullOrWhiteSpace(resourceId, nameof(resourceId));
        }

        /// <summary>
        /// Gets the resource type.
        /// </summary>
        public string ResourceType { get; }

        /// <summary>
        /// Gets the resource id.
        /// </summary>
        public string ResourceId { get; }
    }
}
