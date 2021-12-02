// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Runtime.Serialization;

namespace Microsoft.Health.Fhir.Transformation.Core.TabularDefinition.Contracts
{
    [DataContract]
    public class Property
    {
        [DataMember(Name = "name")]
        public string Name { get; set; }

        [DataMember(Name = "path")]
        public string Path { get; set; }

        [DataMember(Name = "propertiesGroupRef")]
        public string PropertiesGroup { get; set; }

        [DataMember(Name = "type")]
        public string Type { get; set; }

        [DataMember(Name = "fhirExpression")]
        public string FhirExpression { get; set; }

        [DataMember(Name = "comments")]
        public string Comments { get; set; }
    }
}
