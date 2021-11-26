// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Exceptions
{
    public enum JobErrorCode
    {
        // Error in reading/parsing job files.
        JobFileReadError = 1101,

        // Job has been scheduled to the end time.
        NoJobToSchedule = 1102,

        // Resume job failed because running job is active.
        ResumeJobConflict = 1103,

        // Save job failed
        SaveJobFailed = 1104,

        UnhandledError = 1201,
    }
}
