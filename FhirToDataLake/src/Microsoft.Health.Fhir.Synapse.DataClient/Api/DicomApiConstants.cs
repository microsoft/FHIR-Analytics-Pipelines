// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class DicomApiConstants
    {
        /// <summary>
        /// Change feed key in DICOM url.
        /// </summary>
        public const string ChangeFeedKey = "changefeed";

        /// <summary>
        /// Studies key in DICOM url.
        /// </summary>
        public const string StudiesKey = "studies";

        /// <summary>
        /// Series key in DICOM url.
        /// </summary>
        public const string SeriesKey = "series";

        /// <summary>
        /// Instances key in DICOM url.
        /// </summary>
        public const string InstancesKey = "instances";

        /// <summary>
        /// Metadata key in DICOM url.
        /// </summary>
        public const string MetadataKey = "metadata";

        /// <summary>
        /// Version dictionary in DICON url.
        /// </summary>
        public static readonly Dictionary<DicomApiVersion, string> VersionMap = new Dictionary<DicomApiVersion, string>()
        {
            { DicomApiVersion.V1, "v1" },
            { DicomApiVersion.V1_0_Prerelease, "v1_0_prerelease" },
        };

        /// <summary>
        /// Limit search parameter.
        /// </summary>
        public const string LimitKey = "limit";

        /// <summary>
        /// IncludeMetadata search parameter.
        /// </summary>
        public const string IncludeMetadataKey = "includeMetadata";

        /// <summary>
        /// Offset search parameter.
        /// </summary>
        public const string OffsetKey = "offset";

        /// <summary>
        /// Latest search parameter.
        /// </summary>
        public const string LatestKey = "latest";

        /// <summary>
        /// The managed identity url to get access token for DICOM service.
        /// </summary>
        public const string DicomManagedIdentityUrl = "https://dicom.healthcareapis.azure.com";
    }
}
