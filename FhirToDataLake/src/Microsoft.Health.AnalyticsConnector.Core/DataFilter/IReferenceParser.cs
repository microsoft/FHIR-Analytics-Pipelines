// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;

namespace Microsoft.Health.AnalyticsConnector.Core.DataFilter
{
    public interface IReferenceParser
    {
        /// <summary>
        /// There are several reference values defined in https://www.hl7.org/fhir/references.html.
        /// We only handle the literal references https://www.hl7.org/fhir/references-definitions.html#Reference.reference,
        /// and ignore the logical references https://www.hl7.org/fhir/references-definitions.html#Reference.identifier and reference description https://www.hl7.org/fhir/references-definitions.html#Reference.display.
        /// For literal reference, relative URL and absolute URL pointing to an internal resource are handled, while absolute URL pointing to an external resource is ignored.
        /// For all the ignored or invalid cases, we will log warning for it and return null.
        /// </summary>
        /// <param name="reference">the reference string.</param>
        /// <returns>the parsed fhirReference.</returns>
        public FhirReference Parse(string reference);
    }
}
