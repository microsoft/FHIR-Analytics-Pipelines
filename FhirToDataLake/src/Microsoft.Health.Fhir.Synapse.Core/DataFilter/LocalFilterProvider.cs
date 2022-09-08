// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public class LocalFilterProvider : IFilterProvider
    {
        private FilterConfiguration _content;

        public LocalFilterProvider(IOptions<FilterConfiguration> filterConfiguration)
        {
            EnsureArg.IsNotNull(filterConfiguration, nameof(filterConfiguration));

            _content = filterConfiguration.Value;
        }

        public Task<FilterConfiguration> GetFilterAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(_content);
        }
    }
}
