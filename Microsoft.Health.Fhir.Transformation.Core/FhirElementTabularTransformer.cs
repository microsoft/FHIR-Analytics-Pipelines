// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Model;
using Hl7.FhirPath;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public abstract class FhirElementTabularTransformer
    {
        private readonly ILogger _logger = TransformationLogging.CreateLogger<FhirElementTabularTransformer>();

        public static Func<string> IdGenerator { get; set; } = () =>
          {
              return Guid.NewGuid().ToString("N");
          };

        public IEnumerable<Dictionary<string, (object valueObj, object typeObj)>> ToTabular(Resource resource, TabularMappingDefinition defination)
        {
            ElementNode root = ElementNode.FromElement(resource.ToTypedElement());
            if (string.IsNullOrEmpty(defination.Unrollpath))
            {
                IEnumerable<(string name, object valueObj, object typeObj)> result = ToTabularInternal(root, defination.Root);
                Dictionary<string, (object valueObj, object typeObj)> output = result.ToDictionary(k => k.name, v => (v.valueObj, v.typeObj));
                
                output[ReservedColumnName.ResourceId] = (resource.Id, FhirTypeNames.String);
                yield return output;
            }
            else
            {
                foreach (var node in root.Select(defination.Unrollpath))
                {
                    IEnumerable<(string name, object valueObj, object typeObj)> result = ToTabularInternal(ElementNode.FromElement(node), defination.Root);
                    Dictionary<string, (object valueObj, object typeObj)> output = result.ToDictionary(k => k.name, v => (v.valueObj, v.typeObj));

                    output[ReservedColumnName.RowId] = (IdGenerator(), FhirTypeNames.String);
                    output[ReservedColumnName.ResourceId] = (resource.Id, FhirTypeNames.String);
                    output[ReservedColumnName.FhirPath] = (node.Location, FhirTypeNames.String);
                    output[ReservedColumnName.ParentPath] = (GetParentLocation(node), FhirTypeNames.String);

                    yield return output;
                }
            }
        }

        public Dictionary<string, (object valueObj, object typeObj)> ToTabular(ElementNode fhirElement, TabularMappingDefinition defination)
        {
            IEnumerable<(string name, object valueObj, object typeObj)> result = ToTabularInternal(fhirElement, defination.Root);

            Dictionary<string, (object valueObj, object typeObj)> output = result.ToDictionary(k => k.name, v => (v.valueObj, v.typeObj));
            return output;
        }

        // Different data source should have customized convert method.
        public abstract (object valueObj, object typeObj) ConvertElementNode(ElementNode fhirElement, string type);

        private IEnumerable<(string name, object valueObj, object typeObj)> ToTabularInternal(ElementNode fhirRoot, DefinitionNode definationRoot)
        {
            foreach (ColumnDefinition column in definationRoot.ColumnDefinitions)
            {
                if (!string.IsNullOrEmpty(column.Type))
                {
                    ElementNode valueNode = fhirRoot;
                    if (!string.IsNullOrEmpty(column.FhirExpression))
                    {
                        var valueFromExpression = fhirRoot.Select(column.FhirExpression).FirstOrDefault();
                        valueFromExpression = valueFromExpression ?? ElementNode.ForPrimitive(string.Empty);
                        valueNode = ElementNode.FromElement(ElementNode.FromElement(valueFromExpression));
                    }

                    (object valueObj, object typeObj) = ConvertElementNode(valueNode, column.Type);
                    yield return (column.Name, valueObj, typeObj);
                }
            }
            
            foreach (var (propertyName, definationNode) in definationRoot.Children)
            {
                var fhirChildNode = fhirRoot;
                if (fhirRoot != null && !string.IsNullOrEmpty(propertyName))
                {
                    fhirChildNode = fhirRoot[propertyName]?.FirstOrDefault();
                }
                foreach (var subResult in ToTabularInternal(fhirChildNode, definationNode))
                {
                    yield return subResult;
                }
            }
        }

        private string GetParentLocation(ITypedElement node)
        {
            string parentLocation = string.Empty;
            try
            {
                string[] locationSplited = node?.Location.Split('.');
                parentLocation = string.Join('.', locationSplited, 0, locationSplited.Length - 1);
            }
            catch(Exception ex)
            {
                _logger.LogCritical("Error: Invalid node {0}, or node location: {1}. Exception Message: {2}", node?.Name, node?.Location, ex);
            }
            return parentLocation;
        }
    }
}
