﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Schema
{
    public class FhirSchemaException : Exception
    {
        public FhirSchemaException(string message)
            : base(message)
        {
        }

        public FhirSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
