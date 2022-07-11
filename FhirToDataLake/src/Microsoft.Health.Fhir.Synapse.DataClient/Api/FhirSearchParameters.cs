// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class FhirSearchParameters
    {
        public FhirSearchParameters(IEnumerable<string> resourceTypes, DateTimeOffset startTime, DateTimeOffset endTime, string continuationToken)
        {
            ResourceTypes = resourceTypes;
            StartTime = startTime;
            EndTime = endTime;
            ContinuationToken = continuationToken;
        }

        public IEnumerable<string> ResourceTypes { get; }

        public DateTimeOffset StartTime { get; }

        public DateTimeOffset EndTime { get; }

        public string ContinuationToken { get; }
    }
}
