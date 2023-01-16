// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobParameters
    {
        /// <summary>
        /// Job type
        /// </summary>
        public string ResourceType { get; set; }

        /// <summary>
        /// Resource type and time range parameters for system scope.
        /// Parameters will be null for group scope.
        /// </summary>
        public TimeRange TimeRange { get; set; }

        public int JobSize { get; set; }
    }
}