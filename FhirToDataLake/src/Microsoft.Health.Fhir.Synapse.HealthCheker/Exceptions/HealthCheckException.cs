// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.HealthCheck.Exceptions
{
    public class HealthCheckException : Exception
    {
        public HealthCheckException(string message)
            : base(message)
        {
        }

        public HealthCheckException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
