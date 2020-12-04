// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Health.Fhir.Transformation.Core.Test
{
    [TestClass]
    public class OperationExecutionHelperTests
    {
        [TestMethod]
        public async Task GivenFunctionWithRetriableException_WhenExecute_OperationShouldBeretried()
        {
            int retryCount = 3;
            Func<Task<int>> func = () =>
            {
                if (retryCount == 0)
                {
                    return Task.FromResult(1);
                }
                retryCount--;
                throw new IOException();
            };

            await OperationExecutionHelper.InvokeWithTimeoutRetryAsync<int>(func, TimeSpan.FromSeconds(1), 3, 2, OperationExecutionHelper.IsRetrableException);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public async Task GivenFunctionWithLongExecutionTime_WhenExecute_OperationShouldBeretried()
        {
            int retryCount = 3;
            Func<Task<int>> func = async () =>
            {
                if (retryCount == 0)
                {
                    return 1;
                }
                retryCount--;
                await Task.Delay(TimeSpan.FromSeconds(10));
                return 0;
            };

            await OperationExecutionHelper.InvokeWithTimeoutRetryAsync<int>(func, TimeSpan.FromSeconds(1), 3, 2, OperationExecutionHelper.IsRetrableException);
            Assert.AreEqual(0, retryCount);
        }
    }
}
