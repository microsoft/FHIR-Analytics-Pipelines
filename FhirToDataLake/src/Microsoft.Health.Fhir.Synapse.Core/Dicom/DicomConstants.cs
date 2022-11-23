// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Dicom
{
    public class DicomConstants
    {
        /// <summary>
        /// Dicom data resource type.
        /// </summary>
        public const string DicomResourceType = "dicom";

        /// <summary>
        /// The prefix for additional properties in Dicom medata parquet data, E.g. "_sequence", "_timestamp".
        /// </summary>
        public const char AdditionalColumnPrefix = '_';

        /// <summary>
        /// Sequence property key in Dicom Parquet data.
        /// </summary>
        public const string SequenceColumnKey = "_sequence";

        /// <summary>
        /// Timestamp property key in Dicom Parquet data.
        /// </summary>
        public const string TimestampColumnKey = "_timestamp";

        public const string Vr = "vr";

        public const string Value = "Value";
    }
}
