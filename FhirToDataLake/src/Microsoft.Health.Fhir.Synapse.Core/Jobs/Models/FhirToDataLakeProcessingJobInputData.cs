// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeProcessingJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Trigger sequence id
        /// </summary>
        public long TriggerSequenceId { get; set; }

        /// <summary>
        /// Processing sequence id
        /// </summary>
        public long ProcessingJobSequenceId { get; set; }

        /// <summary>
        /// The start timestamp specified in job configuration.
        /// </summary>
        public DateTimeOffset? Since { get; set; }

        /// <summary>
        /// Start time, process all data if not specified,
        /// </summary>
        public DateTimeOffset? DataStartTime { get; set; }

        /// <summary>
        /// End time
        /// </summary>
        public DateTimeOffset DataEndTime { get; set; }

        /// <summary>
        /// The patients to be processed, patient id hash and processed patient version are provided for each patient, used for Group scope
        /// </summary>
        public List<PatientWrapper> ToBeProcessedPatients { get; set; }
    }
}