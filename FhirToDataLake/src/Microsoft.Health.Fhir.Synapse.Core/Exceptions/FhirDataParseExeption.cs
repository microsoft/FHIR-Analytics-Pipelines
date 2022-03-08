// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.Fhir.Synapse.Core.Exceptions
{
    public class FhirDataParseExeption : Exception
    {
        public FhirDataParseExeption(string message)
            : base(message)
        {
        }

        public FhirDataParseExeption(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
