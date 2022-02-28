// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Fhir
{
    /// <summary>
    /// Constants defined in FHIR bundle structure.
    /// See https://www.hl7.org/fhir/bundle.html.
    /// </summary>
    public static class FhirBundleConstants
    {
        public const string BundleResourceType = "Bundle";

        public const string EntryKey = "entry";

        public const string EntryResourceKey = "resource";

        public const string ResourceTypeKey = "resourceType";

        public const string MetaKey = "meta";

        public const string LastUpdatedKey = "lastUpdated";

        public const string LinkKey = "link";

        public const string LinkRelationKey = "relation";

        public const string LinkUrlKey = "url";

        public const string NextLinkValue = "next";
    }
}
