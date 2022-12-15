// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Security.Policy;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobBatchInput
    {
        /// <summary>
        /// Job type
        /// </summary>
        public FilterScope FilterScope { get; set; }

        public string ResourceType { get; set; }
        /// <summary>
        /// Trigger sequence id
        /// </summary>
        public List<FhirToDataLakeProcessingJobInputData> FhirToDataLakeProcessingJobInputDatas { get; set; }
    }
}