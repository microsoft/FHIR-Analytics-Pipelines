// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public interface IDicomDataClient
    {
        public Task<List<string>> GetMetadataAsync(
            ChangeFeedOptions changeFeedOptions,
            CancellationToken cancellationToken = default);

        public Task<long> GetLatestSequenceAsync(
            bool includeMetadata,
            CancellationToken cancellationToken = default);
    }
}
