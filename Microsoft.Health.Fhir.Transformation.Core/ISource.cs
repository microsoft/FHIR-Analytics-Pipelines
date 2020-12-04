// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading.Tasks;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public interface ISource
    {
        public Task OpenAsync();

        public Task<string> ReadAsync();

        public Task CloseAsync();
    }
}
