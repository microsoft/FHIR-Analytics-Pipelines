// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Models
{
    public enum HealthCheckStatus
    {
        /// <summary>
        /// Health status
        /// </summary>
        HEALTHY,

        /// <summary>
        /// Unhealthy status
        /// </summary>
        UNHEALTHY,

        /// <summary>
        /// Unknown status
        /// </summary>
        UNKNOWN,
    }
}
