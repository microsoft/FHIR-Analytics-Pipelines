// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Azure;

namespace Microsoft.Health.AnalyticsConnector.DataWriter.Azure
{
    public class AzureBlobInfo
    {
        public AzureBlobInfo(string name, string etag)
        {
            Name = name;
            ETag = etag;
        }

        public AzureBlobInfo(string name, string etag, DateTimeOffset? lastModified)
        {
            Name = name;
            ETag = etag;
            LastModified = lastModified;
        }

        public string Name { get; set; }

        public string ETag { get; set; }

        public DateTimeOffset? LastModified { get; set; }
    }
}
