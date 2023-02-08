// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs.Models
{
    public enum JobType
    {
        /// <summary>
        /// Orchestrator job
        /// </summary>
        Orchestrator = 0,

        /// <summary>
        /// Processing job
        /// </summary>
        Processing = 1,
    }
}