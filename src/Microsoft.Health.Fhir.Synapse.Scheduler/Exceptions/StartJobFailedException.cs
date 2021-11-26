// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions
{
    /// <summary>
    /// Failed to start a job.
    /// </summary>
    public class StartJobFailedException : Exception
    {
        public StartJobFailedException(string message, JobErrorCode code)
            : base(message)
        {
            Code = code;
        }

        public StartJobFailedException(string message, Exception inner, JobErrorCode code)
            : base(message, inner)
        {
            Code = code;
        }

        public JobErrorCode Code { get; set; }
    }
}
