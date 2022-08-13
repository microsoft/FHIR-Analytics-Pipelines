// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

namespace Microsoft.Health.Fhir.Synapse.JobManagement
{
    public interface IStorage
    {
        public string TableUrl { get; }

        public string TableName { get; }

        public string QueueUrl { get; }

        public string QueueName { get; }

        public bool UseConnectionString { get; }
    }
}