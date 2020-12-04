// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Microsoft.Health.Fhir.Transformation.Core.TabularDefinition.Contracts
{
    [DataContract]
    public class PropertiesGroup
    {
        [DataMember(Name = "propertiesGroupName")]
        public string PropertiesGroupName { get; set; }

        [DataMember(Name = "properties")]
        public List<Property> Properties { get; set; }
    }
}
