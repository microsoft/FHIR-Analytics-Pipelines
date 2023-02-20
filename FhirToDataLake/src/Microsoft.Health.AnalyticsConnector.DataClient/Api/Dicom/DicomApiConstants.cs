// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.AnalyticsConnector.Common;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Api.Dicom
{
    public class DicomApiConstants
    {
        /// <summary>
        /// Version dictionary in DICOM url.
        /// Reference: https://github.com/microsoft/dicom-server/blob/main/docs/api-versioning.md
        /// </summary>
        public static readonly Dictionary<DicomApiVersion, string> VersionMap = new Dictionary<DicomApiVersion, string>()
        {
            { DicomApiVersion.V1, "v1" },
            { DicomApiVersion.V1_0_Prerelease, "v1.0-prerelease" },
        };

        /// <summary>
        /// Change feed key in DICOM url.
        /// Reference: https://github.com/microsoft/dicom-server/blob/main/docs/concepts/change-feed.md
        /// </summary>
        public const string ChangeFeedKey = "changefeed";

        /// <summary>
        /// Offset search parameter.
        /// </summary>
        public const string OffsetKey = "offset";

        /// <summary>
        /// Limit search parameter.
        /// </summary>
        public const string LimitKey = "limit";

        /// <summary>
        /// IncludeMetadata search parameter.
        /// </summary>
        public const string IncludeMetadataKey = "includeMetadata";

        /// <summary>
        /// Latest key in DICOM url.
        /// </summary>
        public const string LatestKey = "latest";

        /// <summary>
        /// The managed identity audiences to get access token for DICOM service.
        /// </summary>
        public static readonly Dictionary<ServerEnvironmentGroup, string> DicomServerAudienceMap = new Dictionary<ServerEnvironmentGroup, string>()
        {
            { ServerEnvironmentGroup.PROD, "https://dicom.healthcareapis.azure.net" },
            { ServerEnvironmentGroup.TEST, "https://dicom.healthcareapis.azure-test.net" },
            { ServerEnvironmentGroup.LOCAL, "https://dicom.healthcareapis.azure-test.net" },
        };
    }
}
