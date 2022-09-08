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
        public async Task Run(FunctionContext context)
        {
            var logger = context.GetLogger("JobManagerFunction");
            logger.LogInformation("C# Timer trigger function executed at: {time}", DateTime.Now);

            try
            {
                await _jobManager.RunAsync();
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Function execution failed.");
                throw;
            }
        }
    }
}