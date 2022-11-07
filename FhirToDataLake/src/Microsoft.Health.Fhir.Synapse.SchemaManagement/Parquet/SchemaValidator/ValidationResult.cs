// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet.SchemaValidator
{
    public class ValidationResult
    {
        public bool Success { get; set; }

        public string ErrorMessage { get; set; } = string.Empty;
    }
}
