// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Models
{
    public class BaseDataLakeOptions
    {
        public DateTimeOffset? StartTime { get; set; } = null;

        public DateTimeOffset? EndTime { get; set; } = null;
    }
}
