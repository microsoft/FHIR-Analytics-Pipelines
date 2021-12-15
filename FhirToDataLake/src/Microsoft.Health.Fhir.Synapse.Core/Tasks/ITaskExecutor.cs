// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Health.Fhir.Synapse.Core.Models.Tasks;

namespace Microsoft.Health.Fhir.Synapse.Core.Tasks
{
    public interface ITaskExecutor
    {
        public Task<TaskResult> ExecuteAsync(
            TaskContext taskContext,
            IProgress<TaskContext> progress,
            CancellationToken cancellationToken = default);
    }
}
