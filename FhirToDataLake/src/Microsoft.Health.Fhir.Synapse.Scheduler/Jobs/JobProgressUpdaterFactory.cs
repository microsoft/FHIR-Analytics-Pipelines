// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Scheduler.Jobs
{
    public class JobProgressUpdaterFactory
    {
        public readonly IJobStore _jobStore;

        public JobProgressUpdaterFactory(IJobStore jobStore)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));

            _jobStore = jobStore;
        }

        public JobProgressUpdater Create(Job job)
        {
            return new JobProgressUpdater(_jobStore, job);
        }
    }
}
