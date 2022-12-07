// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.Dicom
{
    public class DicomConstants
    {
        /// <summary>
        /// DICOM data resource type.
        /// </summary>
        public const string DicomResourceType = "Dicom";

        /// <summary>
        /// The prefix for additional properties in DICOM metadata parquet data, E.g. "_sequence", "_timestamp".
        /// </summary>
        public const char AdditionalColumnPrefix = '_';

        /// <summary>
        /// Sequence property key in DICOM Parquet data.
        /// </summary>
        public const string SequenceColumnKey = "_sequence";

        /// <summary>
        /// Timestamp property key in DICOM Parquet data.
        /// </summary>
        public const string TimestampColumnKey = "_timestamp";

        /// <summary>
        /// The value representation key in DICOM JSON model object structure
        /// Reference: https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.2.html
        /// </summary>
        public const string Vr = "vr";

        /// <summary>
        /// The value key in DICOM JSON model object structure
        /// Reference: https://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_F.2.2.html
        /// </summary>
        public const string Value = "Value";
    }
}
