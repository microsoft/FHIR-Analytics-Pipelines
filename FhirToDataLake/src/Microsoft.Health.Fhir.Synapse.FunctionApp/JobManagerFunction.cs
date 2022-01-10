// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Synapse.Core.Jobs;

namespace Microsoft.Health.Fhir.Synapse.FunctionApp
{
    public class JobManagerFunction
    {
        private JobManager _jobManager;

        public JobManagerFunction(JobManager jobManager)
        {
            _jobManager = jobManager;
        }

        [Function("JobManagerFunction")]
        public async Task Run(
            [TimerTrigger("0 */5 * * * *", RunOnStartup = true)] MyInfo myTimer,
            FunctionContext context)
        {
            var logger = context.GetLogger("JobManagerFunction");
            logger.LogInformation("C# Timer trigger function executed at: {time}", DateTime.Now);
            logger.LogInformation("Next timer schedule at: {time}", myTimer.ScheduleStatus.Next);

            await _jobManager.Run();
        }
    }

    public class MyInfo
    {
        public MyScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

    public class MyScheduleStatus
    {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}
