// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Health.AnalyticsConnector.Common.Logging
{
    public static class DiagnosticLoggerRegistrationExtension
    {
        public static IServiceCollection AddDiagnosticLogger(this IServiceCollection services)
        {
            services.AddSingleton<IDiagnosticLogger, DiagnosticLogger>();

            return services;
        }
    }
}
