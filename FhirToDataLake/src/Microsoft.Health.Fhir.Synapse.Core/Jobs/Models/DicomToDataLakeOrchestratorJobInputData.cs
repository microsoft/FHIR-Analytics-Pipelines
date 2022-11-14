// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class DicomToDataLakeOrchestratorJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Trigger sequence id.
        /// </summary>
        public long TriggerSequenceId { get; set; }

        public long StartOffset { get; set; }
    }
}