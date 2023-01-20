// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class DicomToDataLakeOrchestratorJobInputData
    {
        /// <summary>
        /// Job type
        /// </summary>
        public JobType JobType { get; set; }

        /// <summary>
        /// Job version, used for version update
        /// </summary>
        public JobVersion JobVersion { get; set; } = DicomToDatalakeJobVersionManager.DefaultJobVersion;

        /// <summary>
        /// Trigger sequence id.
        /// </summary>
        public long TriggerSequenceId { get; set; }

        /// <summary>
        /// Start offset in DICOM Server changefeed
        /// </summary>
        public long StartOffset { get; set; }

        /// <summary>
        /// End offset in DICOM Server changefeed
        /// </summary>
        public long EndOffset { get; set; }
    }
}