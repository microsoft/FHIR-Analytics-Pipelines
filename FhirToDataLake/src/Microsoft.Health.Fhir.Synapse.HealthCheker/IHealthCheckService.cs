// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;

namespace Microsoft.Health.Fhir.Synapse.HealthCheker
{
    public interface IHealthCheckService
    {
        public void Execute(CancellationToken cancellationToken);

        public void HealthCheckCallbackResultProcess(IAsyncResult result);
    }
}
