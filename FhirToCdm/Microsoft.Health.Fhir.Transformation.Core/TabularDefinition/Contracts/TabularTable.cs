// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Microsoft.Health.Fhir.Transformation.Core.TabularDefinition.Contracts
{
    public class TabularTable
    {
        [DataMember(Name = "name")]
        public string Name { get; set; }

        [DataMember(Name = "resourceType")]
        public string ResourceType { get; set; }

        [DataMember(Name = "unrollPath")]
        public string UnrollPath { get; set; }

        [DataMember(Name = "properties")]
        public List<Property> Properties { get; set; }
    }
}
