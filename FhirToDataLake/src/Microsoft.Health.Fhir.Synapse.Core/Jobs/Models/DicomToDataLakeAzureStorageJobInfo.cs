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
                    return JsonConvert.SerializeObject(inputData);
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
                    return JsonConvert.SerializeObject(inputData);
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return null;
        }
    }
}