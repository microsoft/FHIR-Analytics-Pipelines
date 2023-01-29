// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;

namespace Microsoft.Health.AnalyticsConnector.Core.DataFilter
{
    public interface IFilterProvider
    {
        public Task<FilterConfiguration> GetFilterAsync(CancellationToken cancellationToken);
    }
}
