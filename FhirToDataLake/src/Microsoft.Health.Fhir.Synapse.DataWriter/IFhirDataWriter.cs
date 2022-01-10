// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Common.Models.Data;
using Microsoft.Health.Fhir.Synapse.Common.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.DataWriter
{
    public interface IFhirDataWriter
    {
        public Task<string> WriteAsync(
            StreamBatchData data,
            TaskContext context,
            DateTime dateTime,
            CancellationToken cancellationToken = default);
    }
}
