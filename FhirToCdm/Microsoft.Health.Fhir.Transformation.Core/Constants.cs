// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public static class ReservedColumnName
    {
        public const string ResourceId = "ResourceId";
        public const string FhirPath = "FhirPath";
        public const string ParentPath = "ParentPath";
        public const string RowId = "RowId";
    }

    public static class ConfigurationConstants
    {
        public const string PropertiesGroupFolderName = "PropertiesGroup";
    }

    public static class FhirTypeNames
    {
        public const string String = "string";
        public const string Number = "number";
        public const string Boolean = "boolean";
        public const string PositiveInt = "positiveInt";
        public const string Date = "date";
        public const string DateTime = "dateTime";
        public const string Integer = "integer";
        public const string UnsignedInt = "unsignedInt";
        public const string Base64Binary = "base64Binary";
        public const string Canonical = "canonical";
        public const string Code = "code";
        public const string Id = "id";
        public const string Oid = "oid";
        public const string Uri = "uri";
        public const string Url = "url";
        public const string Uuid = "uuid";
        public const string Markdown = "markdown";
        public const string Xhtml = "xhtml";
        public const string Instant = "instant";
        public const string Decimal = "decimal";
        public const string Time = "time";
        public const string Array = "array";
    }
         
    public static class ExecutionConstants
    {
        public const int DefaultConcurrentCount = 3;
    }
}
