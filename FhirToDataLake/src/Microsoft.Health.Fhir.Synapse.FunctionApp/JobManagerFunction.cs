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
        private readonly JobManager _jobManager;

        public JobManagerFunction(JobManager jobManager)
        {
            _jobManager = jobManager;
        }

        [Function("JobManagerFunction")]
        public async Task Run(
            [TimerTrigger("0 */5 * * * *", RunOnStartup = false)] MyInfo myTimer,
            FunctionContext context)
        {
            var logger = context.GetLogger("JobManagerFunction");
            logger.LogInformation("C# Timer trigger function executed at: {time}", DateTime.Now);
            logger.LogInformation("Next timer schedule at: {time}", myTimer.ScheduleStatus.Next);

            try
            {
                await _jobManager.RunAsync();
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Function execution failed.");
                _jobManager.Dispose();
                throw;
            }
        }
    }

#pragma warning disable SA1402 // File may only contain a single type
    public class MyInfo
#pragma warning restore SA1402 // File may only contain a single type
    {
        public MyScheduleStatus ScheduleStatus { get; set; }

        public bool IsPastDue { get; set; }
    }

#pragma warning disable SA1402 // File may only contain a single type
    public class MyScheduleStatus
#pragma warning restore SA1402 // File may only contain a single type
    {
        public DateTime Last { get; set; }

        public DateTime Next { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}
