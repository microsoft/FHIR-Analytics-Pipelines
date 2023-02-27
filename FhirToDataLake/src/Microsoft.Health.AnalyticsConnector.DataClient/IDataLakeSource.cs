// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.DataClient
{
    public interface IDataLakeSource
    {
        /// <summary>
        /// Storage url.
        /// </summary>
        public string StorageUrl { get; }

        /// <summary>
        /// Storage container name.
        /// </summary>
        public string Location { get; }
    }
}
