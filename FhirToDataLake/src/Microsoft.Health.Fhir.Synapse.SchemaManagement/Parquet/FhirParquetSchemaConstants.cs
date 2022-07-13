// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Newtonsoft.Json.Schema;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public static class FhirParquetSchemaConstants
    {
        public const string DefaultSchemaProviderKey = "default";
        public const string CustomSchemaProviderKey = "custom";

        public const string JsonSchemaTemplateDirectory = "Schema";
        public const string JsonSchemaTemplateFileExtension = ".schema.json";
        public const string CustomizedSchemaSuffix = "_Customized";

        /// <summary>
        /// Fields with this schema type will be wrapped into single Json string.
        /// </summary>
        public const string JsonStringType = "JSONSTRING";

        /// <summary>
        /// Map for primitive data types to different arrow parquet types. Will be used in CLR wrapper to generate arrow schema.
        /// </summary>
        public static readonly HashSet<string> IntTypes = new HashSet<string> { "positiveInt", "integer", "unsignedInt" };
        public static readonly HashSet<string> DecimalTypes = new HashSet<string> { "decimal", "number" };
        public static readonly HashSet<string> BooleanTypes = new HashSet<string> { "boolean" };

        // Basic types refer to Json schema document https://cswr.github.io/JsonSchema/spec/basic_types/
        public static readonly Dictionary<JSchemaType, string> BasicJSchemaTypeMap = new Dictionary<JSchemaType, string>()
        {
            { JSchemaType.String, "string" },
            { JSchemaType.Number, "number" },
            { JSchemaType.Integer, "integer" },
            { JSchemaType.Boolean, "boolean" },
        };
    }
}
