// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.Common.Models.Data
{
    public class BatchDataResult
    {
        public BatchDataResult(
            string resourceType,
            string continuationToken,
            string blobUrl,
            int resourceCount,
            int processedCount)
        {
            ResourceType = resourceType;
            ContinuationToken = continuationToken;
            BlobUrl = blobUrl;
            ResourceCount = resourceCount;
            ProcessedCount = processedCount;
        }

        /// <summary>
        /// Resource count for this batch.
        /// </summary>
        public int ResourceCount { get; }

        /// <summary>
        /// Processed resource count for this batch.
        /// </summary>
        public int ProcessedCount { get; }

        /// <summary>
        /// Resource type for this batch.
        /// </summary>
        public string ResourceType { get; }

        /// <summary>
        /// ContinuationToken for this batch.
        /// </summary>
        public string ContinuationToken { get; }

        /// <summary>
        /// Output blob url for this batch.
        /// </summary>
        public string BlobUrl { get; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
