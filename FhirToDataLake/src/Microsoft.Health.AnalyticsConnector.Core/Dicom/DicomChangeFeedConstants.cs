// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.AnalyticsConnector.Core.Dicom
{
    public class DicomChangeFeedConstants
    {
        /// <summary>
        /// State property key in change feed content.
        /// </summary>
        public const string StateKey = "state";

        /// <summary>
        /// Action property key in change feed content.
        /// </summary>
        public const string ActionKey = "action";

        /// <summary>
        /// Sequence property key in change feed content.
        /// </summary>
        public const string SequenceKey = "sequence";

        /// <summary>
        /// Timestamp property key in change feed content.
        /// </summary>
        public const string TimestampKey = "timestamp";

        /// <summary>
        /// Metadata property key in change feed content.
        /// </summary>
        public const string MetadataKey = "metadata";
    }
}
