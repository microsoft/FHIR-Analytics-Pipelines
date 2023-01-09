// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeOrchestratorJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Trigger sequence id.
        /// </summary>
        public long TriggerSequenceId { get; set; }

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
        /// Is Incremental
        /// </summary>
        public bool IsIncremental { get; set; }
    }
}