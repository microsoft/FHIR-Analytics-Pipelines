// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public static class FhirParquetSchemaNodeConstants
    {
        /// <summary>
        /// Fields with this type will be wrapped into single Json string.
        /// </summary>
        public const string JsonStringType = "JSONSTRING";

        /// <summary>
        /// Map for primitive data types to different arrow parquet types. Will be used in CLR wrapper to generate arrow schema.
        /// </summary>
        public static readonly HashSet<string> IntTypes = new HashSet<string> { "positiveInt", "integer", "unsignedInt" };
        public static readonly HashSet<string> DecimalTypes = new HashSet<string> { "decimal", "number" };
        public static readonly HashSet<string> BooleanTypes = new HashSet<string> { "boolean" };
    }
}
