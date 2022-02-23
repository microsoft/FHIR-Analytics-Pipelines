// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs
{
    public class JobProgressUpdaterFactory
    {
        private readonly IJobStore _jobStore;
        private readonly ILoggerFactory _loggerFactory;

        public JobProgressUpdaterFactory(
            IJobStore jobStore,
            ILoggerFactory loggerFactory)
        {
            EnsureArg.IsNotNull(jobStore, nameof(jobStore));

            _jobStore = jobStore;
            _loggerFactory = loggerFactory;
        }

        public JobProgressUpdater Create(Job job)
        {
            return new JobProgressUpdater(_jobStore, job, _loggerFactory.CreateLogger<JobProgressUpdater>());
        }
    }
}
