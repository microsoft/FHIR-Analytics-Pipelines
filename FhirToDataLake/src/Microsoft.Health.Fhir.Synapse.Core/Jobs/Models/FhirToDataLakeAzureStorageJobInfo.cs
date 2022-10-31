// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        private readonly DateTimeOffset _fakeDataEndTime = DateTimeOffset.MinValue;

        public override string JobIdentifier()
        {
            string identifierString = Definition;
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    inputData.DataEndTime = _fakeDataEndTime;
                    identifierString = JsonConvert.SerializeObject(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    inputData.DataEndTime = _fakeDataEndTime;
                    identifierString = JsonConvert.SerializeObject(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return identifierString.ComputeHash();
        }
    }
}