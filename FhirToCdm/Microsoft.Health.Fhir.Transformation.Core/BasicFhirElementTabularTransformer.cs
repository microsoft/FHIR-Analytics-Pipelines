// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

﻿using System;
using System.Linq;
using System.Text.RegularExpressions;
using Hl7.Fhir.ElementModel;
using Hl7.Fhir.Serialization;
using Hl7.FhirPath.Sprache;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class BasicFhirElementTabularTransformer : FhirElementTabularTransformer
    {
        private readonly ILogger _logger = TransformationLogging.CreateLogger<BasicFhirElementTabularTransformer>();

        public override (object valueObj, object typeObj) ConvertElementNode(ElementNode fhirElement, string type)
        {
            if (fhirElement == null)
            {
                return (valueObj: null, typeObj: type);
            }

            try
            {
                switch (type)
                {
                    case FhirTypeNames.String:
                    case FhirTypeNames.Canonical:
                    case FhirTypeNames.Code:
                    case FhirTypeNames.Id:
                    case FhirTypeNames.Markdown:
                    case FhirTypeNames.Oid:
                    case FhirTypeNames.Url:
                    case FhirTypeNames.Uri:
                    case FhirTypeNames.Uuid:
                    case FhirTypeNames.Xhtml:
                    case FhirTypeNames.Base64Binary:
                        return (valueObj: fhirElement?.Value?.ToString() ?? fhirElement?.ToJson() ?? string.Empty, type);

                    case FhirTypeNames.Boolean:
                        return (valueObj: TryParseFhirBooleanName(fhirElement), type);

                    case FhirTypeNames.Integer:
                    case FhirTypeNames.PositiveInt:
                    case FhirTypeNames.UnsignedInt:
                        return (valueObj: TryParseIntOrFloat<int>(fhirElement), type);

                    case FhirTypeNames.Decimal:
                    case FhirTypeNames.Number:
                        return (valueObj: TryParseIntOrFloat<float>(fhirElement), type);

                    case FhirTypeNames.Date:
                    case FhirTypeNames.DateTime:
                    case FhirTypeNames.Instant:
                        return (valueObj: TryParseFhirDateTimeRelatedNode(fhirElement), type);

                    case FhirTypeNames.Time:
                        return (valueObj: TryParseFhirTimeNode(fhirElement), type);

                    case FhirTypeNames.Array:
                        return (valueObj: TryParseFhirArrayNode(fhirElement), type);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Not support resource type: {0}. Exception Message: {1}", type, ex);
            }
            return (valueObj: null, typeObj: type);
        }

        private object TryParseIntOrFloat<T>(ElementNode node)
        {
            string dataValue = node.Value.ToString();
            if (string.IsNullOrEmpty(dataValue))
            {
                return null;
            }
            try
            {
                return (T)Convert.ChangeType(dataValue, typeof(T));
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Error: Invalid resource value: {0}. Exception Message: {1}", dataValue, ex);
            }

            return null;
        }

        /*
        In Fhir document https://www.hl7.org/fhir/datatypes.html#time, Regex for "time" is
        ([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?
        */
        private Object TryParseFhirTimeNode(ElementNode node)
        {
            try
            {
                string valueString = node.Value.ToString();
                if (string.IsNullOrEmpty(valueString))
                {
                    return null;
                }
                TimeSpan timeSpan = TimeSpan.Parse(valueString);
                
                return DateTime.MinValue + timeSpan;
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Error: Invalid resource value: {0}. Exception Message: {1}", node.Value, ex);
            }
            return null;
        }

        private Object TryParseFhirBooleanName(ElementNode node)
        {
            try
            {
                string valueString = node?.Value?.ToString();
                if (string.IsNullOrEmpty(valueString))
                {
                    return null;
                }
                return Boolean.Parse(valueString);
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Error: Invalid resource value: {0}. Exception Message: {1}", node.Value, ex);
            }
            return null;
        }

        /*
        In Fhir document https://www.hl7.org/fhir/datatypes.html, Regex for DateTime related value are
        date: YYYY, YYYY-MM, YYYY-MM-DD
        ([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1]))?)?
        dateTime: YYYY, YYYY-MM, YYYY-MM-DD or YYYY-MM-DDThh:mm:ss+zz:zz
        ([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)))?)?)?
        instant: YYYY-MM-DDThh:mm:ss.sss+zz:zz
        ([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))
        */
        private object TryParseFhirDateTimeRelatedNode(ElementNode node)
        {
            try
            {
                string valueString = node.Value.ToString();
                if (string.IsNullOrEmpty(valueString))
                {
                    return null;
                }

                // 1. valueString like format "YYYY"
                if (Regex.IsMatch(valueString, @"^([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)$"))
                {
                    return new DateTime(int.Parse(valueString), 01, 01);
                }

                // 2. valueString like format "YYYY-MM", "YYYY-MM-DD", "YYYY-MM-DDThh:mm:ss"
                // Using the DateTimeOffset to handle the time zone offset, will return the the DateTime in its UTC time zone.

                DateTimeOffset dateTimeOffset = DateTimeOffset.Parse(valueString);

                // If raw string value dones't conains time zone information, assume that the raw date time using the UTC time zone as its local time zone.
                // Otherwise we convert the date time in local time zone, then return its UTC date time value.
                return Regex.IsMatch(valueString, @"(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00))$") ?
                         dateTimeOffset.UtcDateTime :
                         dateTimeOffset.DateTime;
            }

            catch (Exception ex)
            {
                _logger.LogDebug("Error: Invalid resource value: {0}.  Exception Message: {1}", node.Value, ex);
            }
            return null;
        }

        private Object TryParseFhirArrayNode(ElementNode node)
        {
            try
            {
                ElementNode? parent = node.Parent;
                return $"[{string.Join(",", parent?[node.Name].Select(node => node.ToJson()))}]";
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Error: Invalid resource value: {0}. Exception Message: {1}", node.Value, ex);
            }
            return null;
        }
    }
}
