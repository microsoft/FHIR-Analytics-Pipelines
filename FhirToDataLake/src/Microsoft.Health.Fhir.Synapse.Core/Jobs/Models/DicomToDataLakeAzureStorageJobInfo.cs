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
    public class DicomToDataLakeAzureStorageJobInfo : AzureStorageJobInfo
    {
        private readonly long _fakeEndOffset = 0;

        // Remove end offset field in Definition to generate job identifier,
        // as the end offset is related to the trigger created time, and may be different if there are two instances try to create a new trigger simultaneously
        public override string JobIdentifier()
        {
            string result = (TryParseAsDicomToDataLakeOrchestratorJobInputData() ??
                             TryParseAsDicomToDataLakeProcessingJobInputData()) ?? Definition.ComputeHash();

            return result.ComputeHash();
        }

        private string TryParseAsDicomToDataLakeOrchestratorJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeOrchestratorJobInputData>(Definition);
                if (inputData is { JobType: JobType.Orchestrator })
                {
                    inputData.EndOffset = _fakeEndOffset;
                    return GetIdentifierText(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }

        private string TryParseAsDicomToDataLakeProcessingJobInputData()
        {
            try
            {
                var inputData = JsonConvert.DeserializeObject<DicomToDataLakeProcessingJobInputData>(Definition);
                if (inputData is { JobType: JobType.Processing })
                {
                    inputData.EndOffset = _fakeEndOffset;
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