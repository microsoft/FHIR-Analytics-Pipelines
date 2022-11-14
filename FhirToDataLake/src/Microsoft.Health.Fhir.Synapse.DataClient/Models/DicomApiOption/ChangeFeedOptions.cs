// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption
{
    public class ChangeFeedOptions
    {
        public ChangeFeedOptions(long offset, long limit, bool includeMetadata)
        {
            Offset = offset;
            Limit = limit;
            IncludeMetadata = includeMetadata;
        }

        public long Offset { get; set; }

        public long Limit { get; set; }

        public bool IncludeMetadata { get; set; }
    }
}
