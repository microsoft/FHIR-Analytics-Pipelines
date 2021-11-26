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
            int resourceCount,
            string resourceType,
            string continuationToken,
            string blobUrl)
        {
            ResourceCount = resourceCount;
            ResourceType = resourceType;
            ContinuationToken = continuationToken;
            BlobUrl = blobUrl;
        }

        /// <summary>
        /// Resource count for this batch.
        /// </summary>
        public int ResourceCount { get; set; }

        /// <summary>
        /// Resource type for this batch.
        /// </summary>
        public string ResourceType { get; set; }

        /// <summary>
        /// ContinuationToken for this batch.
        /// </summary>
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Written blob url for this batch.
        /// </summary>
        public string BlobUrl { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
