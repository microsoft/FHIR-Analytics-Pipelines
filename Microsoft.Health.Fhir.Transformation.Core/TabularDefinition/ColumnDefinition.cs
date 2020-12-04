// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class ColumnDefinition
    {
        public ColumnDefinition(string name, string type) : this(name, type, null, null) { }

        public ColumnDefinition(string name, string type, string fhirExpression, string comments)
        {
            Name = name;
            Type = type;
            FhirExpression = fhirExpression;
            Comments = comments;
        }

        public string Name { get; set; } = null;

        public string Type { get; set; } = null;

        public string FhirExpression { get; set; } = null;

        public string Comments { get; set; } = null;
    }
}
