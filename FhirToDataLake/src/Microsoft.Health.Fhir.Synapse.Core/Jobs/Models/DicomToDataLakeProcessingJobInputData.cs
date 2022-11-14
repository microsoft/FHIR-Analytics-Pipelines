// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class DicomToDataLakeProcessingJobInputData
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

        public long StartOffset { get; set; }

        public long EndOffset { get; set; }
    }
}