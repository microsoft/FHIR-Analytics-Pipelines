// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobInputData
    {
        public JobType JobType { get; set; }

        /// <summary>
        /// Processing sequence id
        /// </summary>
        [JsonProperty("processingJobSequenceId")]
        public long ProcessingJobSequenceId { get; set; }

        /// <summary>
        /// The start timestamp specified in job configuration.
        /// </summary>
        [JsonProperty("since")]
        public DateTimeOffset Since { get; set; }

        public DateTimeOffset DataStartTime { get; set; }

        public DateTimeOffset DataEndTime { get; set; }

        /// <summary>
        /// Filter Scope.
        /// </summary>
        [JsonProperty("filterScope")]
        public FilterScope FilterScope { get; set;  }

        /// <summary>
        /// Type filter list.
        /// </summary>
        [JsonProperty("typeFilters")]
        public IList<TypeFilter> TypeFilters { get; set; }

        public BaseProcessingInputMetadata InputMetadata { get; set; }
    }
}
