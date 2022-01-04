// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.DataClient.Fhir;

namespace Microsoft.Health.Fhir.Synapse.DataClient
{
    public interface IFhirDataClient
    {
        public Task<FhirElementBatchData> GetAsync(
            string resourceType,
            DateTimeOffset startTime,
            DateTimeOffset endTime,
            string continuationToken,
            CancellationToken cancellationToken = default);
    }
}
