// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Health.AnalyticsConnector.Core.Jobs
{
    public interface IExternalDependencyChecker
    {
        /// <summary>
        /// Check if external dependency ready.
        /// </summary>
        /// <param name="cancellationToken">cancellation token.</param>
        /// <returns>return true if all the external dependencies are ready, otherwise return false.</returns>
        public Task<bool> IsExternalDependencyReady(CancellationToken cancellationToken);
    }
}