// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement
{
    public class FhirSchemaNode
    {
        /// <summary>
        /// Gets or sets node name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets depth of current FHIR schema node.
        /// Choice root node doesn't have additional depth, and share same depth with its children nodes.
        /// </summary>
        public int Depth { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether current node can be repeated. Like the "Cardinality" ("Card.") in FHIR format https://www.hl7.org/fhir/formats.html#table
        /// A leaf or a struct schema node both can be a repeated node.
        /// E.g.
        /// Type of "given" in FHIR property "HumanName" is primitive type "string", and it can be repeated so its shcmea node is leaf and repeated.
        /// Type of "name" in FHIR property "Patient" is struct type "HumanName", and it can be repeated so its shemea node is repeated but not leaf.
        /// See https://www.hl7.org/fhir/datatypes.html#HumanName,
        /// https://www.hl7.org/fhir/patient.html.
        /// </summary>
        public bool IsRepeated { get; set; }

        /// <summary>
        /// Gets or sets node path for current FHIR schema node.
        /// </summary>
        public List<string> NodePaths { get; set; }

        /// <summary>
        /// Get path for current FHIR schema node.
        /// </summary>
        /// <returns>Node path string for current FHIR schema node.</returns>
        public string GetNodePath()
        {
            return string.Join('.', this.NodePaths);
        }
    }
}
