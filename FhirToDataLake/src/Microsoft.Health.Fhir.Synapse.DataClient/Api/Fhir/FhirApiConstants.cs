// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api.Fhir
{
    /// <summary>
    /// Constants for FHIR search API parameters.
    /// See https://www.hl7.org/fhir/STU3/search.html#2.21.1.1.
    /// </summary>
    public static class FhirApiConstants
    {
        /// <summary>
        /// Last updated search parameter.
        /// </summary>
        public const string LastUpdatedKey = "_lastUpdated";

        /// <summary>
        /// Id search parameter.
        /// </summary>
        public const string IdKey = "_id";

        /// <summary>
        /// Page count search parameter.
        /// </summary>
        public const string PageCountKey = "_count";

        /// <summary>
        /// Continuation token search parameter.
        /// </summary>
        public const string ContinuationKey = "ct";

        /// <summary>
        /// Type search parameter.
        /// </summary>
        public const string TypeKey = "_type";

        /// <summary>
        /// Metadata
        /// </summary>
        public const string MetadataKey = "metadata";

    }
}