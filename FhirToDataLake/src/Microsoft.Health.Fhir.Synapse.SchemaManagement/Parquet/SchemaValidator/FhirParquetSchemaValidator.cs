// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaValidator
{
    public class FhirParquetSchemaValidator
    {
        public static ValidationResult Validate(string schemaKey, FhirParquetSchemaNode rootNode)
        {
            bool success;
            string error = string.Empty;

            if (string.IsNullOrWhiteSpace(schemaKey))
            {
                success = false;
                error = "The schema key cannot be null or empty.";
            }
            else
            {
                success = ValidateInternal(schemaKey, rootNode, new Stack<string>(), ref error);
            }

            return new ValidationResult() { Success = success, ErrorMessage = error };
        }

        private static bool ValidateInternal(string schemaKey, FhirParquetSchemaNode schemaNode, Stack<string> nodePath, ref string error)
        {
            nodePath.Push(schemaKey);

            if (schemaNode == null)
            {
                error = string.Format("The schema node '{0}' cannot be null.", string.Join('.', nodePath.Reverse()));
                return false;
            }

            if (schemaNode.IsLeaf)
            {
                // The SubNodes of leaf node should be null.
                if (schemaNode.SubNodes != null)
                {
                    error = string.Format("The leaf schema node '{0}' shouldn't have sub nodes.", string.Join('.', nodePath.Reverse()));
                    return false;
                }

                // The Type of Leaf node should be primitive data type.
                if (schemaNode.Type == null || !(
                    FhirParquetSchemaConstants.IntTypes.Contains(schemaNode.Type) ||
                    FhirParquetSchemaConstants.BooleanTypes.Contains(schemaNode.Type) ||
                    FhirParquetSchemaConstants.DecimalTypes.Contains(schemaNode.Type) ||
                    FhirParquetSchemaConstants.StringTypes.Contains(schemaNode.Type)))
                {
                    error = string.Format("The leaf schema node '{0}' type '{1}' is not primitive.", string.Join('.', nodePath.Reverse()), schemaNode.Type);
                    return false;
                }
            }
            else
            {
                if (schemaNode.SubNodes == null)
                {
                    error = string.Format("The schema node '{0}' sub nodes cannot be null.", string.Join('.', nodePath.Reverse()));
                    return false;
                }

                foreach (var subNodeItem in schemaNode.SubNodes)
                {
                    // Return false and related error message for first invalid schema node.
                    if (!ValidateInternal(subNodeItem.Key, subNodeItem.Value, nodePath, ref error))
                    {
                        return false;
                    }
                }
            }

            nodePath.Pop();
            return true;
        }
    }
}
