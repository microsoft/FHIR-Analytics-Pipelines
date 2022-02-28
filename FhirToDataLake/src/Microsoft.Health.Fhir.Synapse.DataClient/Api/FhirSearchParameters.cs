// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class FhirSearchParameters
    {
        public FhirSearchParameters(string resourceType, DateTimeOffset startTime, DateTimeOffset endTime, string continuationToken)
        {
            ResourceType = resourceType;
            StartTime = startTime;
            EndTime = endTime;
            ContinuationToken = continuationToken;
        }

        public string ResourceType { get; }

        public DateTimeOffset StartTime { get; }

        public DateTimeOffset EndTime { get; }

        public string ContinuationToken { get; }
    }
}
