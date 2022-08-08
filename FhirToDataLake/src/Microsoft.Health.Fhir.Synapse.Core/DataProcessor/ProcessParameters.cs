// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.Core.DataProcessor
{
    public class ProcessParameters
    {
        public ProcessParameters(string schemaType, string resourceType)
        {
            SchemaType = schemaType;
            ResourceType = resourceType;
        }

        public string SchemaType { get; }

        public string ResourceType { get; }
    }
}
