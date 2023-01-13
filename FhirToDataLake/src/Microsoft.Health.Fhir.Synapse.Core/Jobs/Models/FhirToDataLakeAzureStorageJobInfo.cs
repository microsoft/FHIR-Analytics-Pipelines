// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Microsoft.Health.Fhir.Synapse.JobManagement.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Synapse.Core.Jobs.Models
{
    public class FhirToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        private readonly DateTimeOffset _fakeDataEndTime = DateTimeOffset.MinValue;

        // Remove job version field and data end time field in Definition to generate job identifier,
        // as the data end time is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public override string JobIdentifier()
        {
            string result = (TryParseAsFhirToDataLakeOrchestratorJobInputData() ??
                             TryParseAsFhirToDataLakeProcessingJobInputData()) ?? Definition;

            return result.ComputeHash();
        }

        private string TryParseAsFhirToDataLakeOrchestratorJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    // the data end time is removed when calculate JobIdentifier since job version v2
                    if (inputData.JobVersion > SupportedJobVersion.V1)
                    {
                        inputData.DataEndTime = _fakeDataEndTime;
                    }

                    return GetIdentifierText(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }

        private string TryParseAsFhirToDataLakeProcessingJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<FhirToDataLakeProcessingJobInputData>(Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    // the data end time is removed when calculate JobIdentifier since job version v2
                    if (inputData.JobVersion > SupportedJobVersion.V1)
                    {
                        inputData.DataEndTime = _fakeDataEndTime;
                    }

                    return GetIdentifierText(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }

        private static string GetIdentifierText(object inputData)
        {
            // the job version is added in input data to handle possible version compatibility issues when the version is updated. It is not related to the job definition, so we need to remove the job version property when calculate job identifier.
            var jobject = JObject.FromObject(inputData);
            jobject[JobVersionManager.JobVersionKey].Parent.Remove();

            return jobject.ToString(Formatting.None);
        }
    }
}