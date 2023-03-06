// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement
{
    public class DataLakeConstants
    {
        /// <summary>
        /// Blob name property key in Parquet data.
        /// </summary>
        public const string BlobNameColumnKey = "_blobName";

        /// <summary>
        /// ETag property key in Parquet data.
        /// </summary>
        public const string ETagColumnKey = "_eTag";

        /// <summary>
        /// Index property key in Parquet data.
        /// </summary>
        public const string IndexColumnKey = "_index";
    }
}
