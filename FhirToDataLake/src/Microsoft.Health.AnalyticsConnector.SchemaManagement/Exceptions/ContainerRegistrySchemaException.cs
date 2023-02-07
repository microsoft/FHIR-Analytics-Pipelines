// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;

namespace Microsoft.Health.AnalyticsConnector.SchemaManagement.Exceptions
{
    public class ContainerRegistrySchemaException : FhirSchemaException
    {
        public ContainerRegistrySchemaException(string message)
            : base(message)
        {
        }

        public ContainerRegistrySchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
