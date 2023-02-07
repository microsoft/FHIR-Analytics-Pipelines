// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Common.Configurations
{
    /// <summary>
    /// Supported versions for configurations.
    /// </summary>
    public enum SupportedConfigVersion
    {
        /// <summary>
        /// Config Version 1, support FHIR as data source.
        /// </summary>
        V1 = 1,

        /// <summary>
        /// Config Version 2, support FHIR, DICOM as data source.
        /// </summary>
        V2 = 2,
    }
}
