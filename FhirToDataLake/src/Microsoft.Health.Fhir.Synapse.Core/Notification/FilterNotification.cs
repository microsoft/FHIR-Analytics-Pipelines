// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using MediatR;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;

namespace Microsoft.Health.Fhir.Synapse.Core.Notification
{
    public class FilterNotification : INotification
    {
        public FilterNotification(Job job)
        {
            Id = job.Id;
            DataPeriod = job.DataPeriod;
            FilterInfo = job.FilterInfo;
            ProcessedResourceCounts = job.ProcessedResourceCounts;
        }

        /// <summary>
        /// Job id.
        /// </summary>
        public string Id { get; }

        /// <summary>
        /// Will process all data with timestamp greater than or equal to DataPeriod.Start and less than DataPeriod.End.
        /// For FHIR data, we use the lastUpdated field as the record timestamp.
        /// </summary>
        public DataPeriod DataPeriod { get; }

        /// <summary>
        /// The filter information.
        /// </summary>
        public FilterInfo FilterInfo { get; }

        /// <summary>
        /// Processed resource count for each schema type.
        /// </summary>
        public Dictionary<string, int> ProcessedResourceCounts { get; set; }

    }
}
