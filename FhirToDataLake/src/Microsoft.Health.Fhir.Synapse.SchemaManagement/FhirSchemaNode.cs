// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
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
        /// Gets or sets node type.
        /// Array node type should be FhirSchemaNodeConstants.ArrayFlag.
        /// Type for node that be wrapped into single JSON string should be "JSONSTRING".
        /// Type for Choice type node should be "CHOICE".
        /// Otherwise type is fetched from FHIR definitions. https://www.hl7.org/fhir/downloads.html.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets depth of current FHIR schema node.
        /// Choice root node doesn't have additional depth, and share same depth with it's children nodes.
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
        /// Gets a value indicating whether current FHIR schema node is leaf node.
        /// A leaf node should not have subNodes, and its type should be "JSONSTRING" or normal primitive types.
        /// </summary>
        public bool IsLeaf => this.SubNodes == null;

        /// <summary>
        /// Gets or sets node path for current FHIR schema node.
        /// </summary>
        public List<string> NodePaths { get; set; }

        /// <summary>
        /// Gets or sets subnodes under the current FHIR schema node, node that have subnodes should not be leaf node.
        /// </summary>
        public Dictionary<string, FhirSchemaNode> SubNodes { get; set; } = null;

        /// <summary>
        /// Gets or sets choice type nodes under the current FHIR schema node.
        /// E.g. "Patient" schema node have {"deceasedBoolean": <deceased, boolean>} and {"deceasedDateTime": <deceased, datetime>} two choice type node.
        /// </summary>
        public Dictionary<string, Tuple<string, string>> ChoiceTypeNodes { get; set; } = null;

        /// <summary>
        /// Get path for current FHIR schema node.
        /// </summary>
        /// <returns>Node path string for current FHIR schema node.</returns>
        public string GetNodePath()
        {
            return string.Join('.', this.NodePaths);
        }

        /// <summary>
        /// Check if current FHIR schema node contains given choice data type node.
        /// </summary>
        /// <param name="choiceDataType">Choice data type string.</param>
        /// <returns>A boolean value indicating whether current FHIR schema node contains given choice data type node.</returns>
        public bool ContainsChoiceDataType(string choiceDataType)
        {
            return this.ChoiceTypeNodes != null && this.ChoiceTypeNodes.ContainsKey(choiceDataType);
        }
    }
}
