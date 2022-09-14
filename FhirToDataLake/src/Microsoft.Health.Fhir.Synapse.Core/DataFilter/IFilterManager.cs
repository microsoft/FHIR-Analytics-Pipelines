// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.FhirSearch;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.DataFilter
{
    public interface IFilterManager
    {
        public Task<FilterScope> GetFilterScopeAsync(CancellationToken cancellationToken);

        public Task<string> GetGroupIdAsync(CancellationToken cancellationToken);

        public Task<List<TypeFilter>> GetTypeFiltersAsync(CancellationToken cancellationToken);
    }
}