// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions
{
    public class ParseJsonSchemaException : FhirSchemaException
    {
        public ParseJsonSchemaException(string message)
            : base(message)
        {
        }

        public ParseJsonSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
