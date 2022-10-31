﻿// -------------------------------------------------------------------------------------------------
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

        // Remove data end time field in Definition to generate job identifier,
        // as the data end time is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public override string JobIdentifier()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    inputData.DataEndTime = _fakeDataEndTime;
                    return JsonConvert.SerializeObject(inputData).ComputeHash();
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
                    return JsonConvert.SerializeObject(inputData).ComputeHash();
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return Definition.ComputeHash();
        }
    }
}