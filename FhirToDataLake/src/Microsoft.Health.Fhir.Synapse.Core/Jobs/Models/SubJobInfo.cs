// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class SubJobInfo
    {
        /// <summary>
        /// Resource type
        /// </summary>
        public string ResourceType { get; set; }

        /// <summary>
        /// Time range of sub job.
        /// </summary>
        public TimeRange TimeRange { get; set; }

        /// <summary>
        /// Resource count of the sub job.
        /// </summary>
        public int ResourceCount { get; set; }
    }
}