﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class OperationExecutionHelper
    {
        private static readonly ILogger _logger = TransformationLogging.CreateLogger<OperationExecutionHelper>();

        public static readonly Predicate<Exception> IsRetrableException = (ex) =>
        {
            bool result = false;

            if (ex is IOException)
            {
                result = true;
            }

            return result;
        };

        public async static Task<T> InvokeWithTimeoutRetryAsync<T>(Func<Task<T>> func, TimeSpan timeout, int rertyCount, int delayInSec = 30, Predicate<Exception> isRetrableException = null)
        {
            while (true)
            {
                try
                {
                    var timeoutTask = Task.Delay(timeout);
                    var executionTask = func();
                    var completedTask = await Task.WhenAny(new Task[] { executionTask, timeoutTask }).ConfigureAwait(false);

                    if (completedTask == executionTask)
                    {
                        return await executionTask.ConfigureAwait(false);
                    }
                    else
                    {
                        throw new TimeoutException();
                    }
                }
                catch (TimeoutException)
                {
                    if (rertyCount-- > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(delayInSec)).ConfigureAwait(false);
                        _logger.LogWarning("Retry on timeout.");
                        continue;
                    }

                    _logger.LogCritical($"Retry failed for timeout exception.");
                    throw;
                }
                catch (Exception ex)
                {
                    if (isRetrableException?.Invoke(ex) ?? false && rertyCount-- > 0)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(delayInSec)).ConfigureAwait(false);
                        _logger.LogWarning($"Retry on exception. {ex.Message}");
                        continue;
                    }

                    _logger.LogCritical($"Retry failed for exception. {ex.Message}");
                    throw;
                }
            }
        }
    }
}
