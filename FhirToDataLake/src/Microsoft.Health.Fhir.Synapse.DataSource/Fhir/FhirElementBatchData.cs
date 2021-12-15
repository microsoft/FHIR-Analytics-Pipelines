// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Hl7.Fhir.ElementModel;

namespace Microsoft.Health.Fhir.Synapse.DataSource.Fhir
{
    /// <summary>
    /// Batch data deserialized to ITypedElement objects.
    /// </summary>
    public class FhirElementBatchData
    {
        public FhirElementBatchData(
            IEnumerable<ITypedElement> values,
            string continuationToken)
        {
            Values = values;
            ContinuationToken = continuationToken;
        }

        public IEnumerable<ITypedElement> Values { get; set; }

        public string ContinuationToken { get; set; }
    }
}
