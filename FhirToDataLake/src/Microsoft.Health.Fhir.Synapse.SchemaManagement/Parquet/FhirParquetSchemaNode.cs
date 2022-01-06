// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet
{
    public class FhirParquetSchemaNode : FhirSchemaNode
    {
        /// <summary>
        /// Gets or sets node type.
        /// Type for node that be wrapped into single JSON string should be "JSONSTRING".
        /// Type for Choice type node should be "CHOICE".
        /// Otherwise type is fetched from FHIR definitions. https://www.hl7.org/fhir/downloads.html.
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Gets a value indicating whether current FHIR schema node is leaf node.
        /// A leaf node should not have subNodes, and its type should be "JSONSTRING" or normal primitive types.
        /// </summary>
        public bool IsLeaf => SubNodes == null;

        /// <summary>
        /// Gets or sets subnodes under the current FHIR schema node, node that have subnodes should not be leaf node.
        /// </summary>
        public Dictionary<string, FhirParquetSchemaNode> SubNodes { get; set; } = null;

        /// <summary>
        /// Gets or sets choice type nodes under the current FHIR schema node.
        /// E.g. "Patient" schema node have {"deceasedBoolean": <deceased, boolean>} and {"deceasedDateTime": <deceased, datetime>} two choice type node.
        /// </summary>
        public Dictionary<string, Tuple<string, string>> ChoiceTypeNodes { get; set; } = null;

        /// <summary>
        /// Check if current FHIR schema node contains given choice data type node.
        /// </summary>
        /// <param name="choiceDataType">Choice data type string.</param>
        /// <returns>A boolean value indicating whether current FHIR schema node contains given choice data type node.</returns>
        public bool ContainsChoiceDataType(string choiceDataType)
        {
            return ChoiceTypeNodes != null && ChoiceTypeNodes.ContainsKey(choiceDataType);
        }
    }
}
