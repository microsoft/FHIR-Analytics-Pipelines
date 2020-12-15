// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.

using System.Collections.Generic;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public interface ISink
    {
        public Task InitAsync();

        public Task WriteAsync(string tableName, string[] columns, Dictionary<string, (object valueObj, object typeObj)> item);

        public Task CompleteAsync();
    }
}
