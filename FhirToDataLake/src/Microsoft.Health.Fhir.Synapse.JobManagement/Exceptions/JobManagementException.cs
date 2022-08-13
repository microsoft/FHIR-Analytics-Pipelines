// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement.Exceptions
{
    public class JobManagementException : Exception
    {
        public JobManagementException(string message)
            : base(message)
        {
        }

        public JobManagementException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}