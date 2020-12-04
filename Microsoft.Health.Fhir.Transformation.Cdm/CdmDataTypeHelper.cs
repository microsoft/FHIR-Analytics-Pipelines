// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Collections.Generic;
using Microsoft.Health.Fhir.Transformation.Core;

namespace Microsoft.Health.Fhir.Transformation.Cdm
{
    public static class CdmDataTypeHelper
    {
        private static Dictionary<string, string> FhirTypeToCdmType = new Dictionary<string, string>()
        {
            { FhirTypeNames.Base64Binary, "string" },
            { FhirTypeNames.Boolean, "boolean" },
            { FhirTypeNames.Canonical, "string" },
            { FhirTypeNames.Code, "string" },
            { FhirTypeNames.Date, "datetime" },
            { FhirTypeNames.DateTime, "datetime" },
            { FhirTypeNames.Id, "string" },
            { FhirTypeNames.Integer, "integer" },
            { FhirTypeNames.Markdown, "string" },
            { FhirTypeNames.Number, "float" },
            { FhirTypeNames.Oid, "string" },
            { FhirTypeNames.PositiveInt, "integer" },
            { FhirTypeNames.String, "string" },
            { FhirTypeNames.UnsignedInt, "integer" },
            { FhirTypeNames.Uri, "string" },
            { FhirTypeNames.Url, "string" },
            { FhirTypeNames.Uuid, "string" },
            { FhirTypeNames.Xhtml, "string"},
            { FhirTypeNames.Instant, "datetime"},
            { FhirTypeNames.Decimal, "decimal"},
            { FhirTypeNames.Time, "datetime"},
        };

        public static string ConvertFhirTypeToCdmType(string fhirType)
        {
            if (FhirTypeToCdmType.ContainsKey(fhirType))
            {
                return FhirTypeToCdmType[fhirType];
            }

            return fhirType;
        }
    }
}
