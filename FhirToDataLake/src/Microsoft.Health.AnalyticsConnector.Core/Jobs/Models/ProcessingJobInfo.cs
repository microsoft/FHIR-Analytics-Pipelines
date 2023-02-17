// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public class ProcessingJobInfo
    {
        /// <summary>
        /// Total resource count.
        /// </summary>
        public int ResourceCount { get; set; } = 0;

        /// <summary>
        /// List of sub jobs.
        /// </summary>
        public List<SubJobInfo> SubJobInfos { get; set; } = new List<SubJobInfo>();
    }
}