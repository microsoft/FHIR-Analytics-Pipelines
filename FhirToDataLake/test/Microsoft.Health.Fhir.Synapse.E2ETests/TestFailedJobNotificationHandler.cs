// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;
using MediatR;
using Microsoft.Health.Fhir.Synapse.Common.Notification;

namespace Microsoft.Health.Fhir.Synapse.E2ETests
{
    public class TestFailedJobNotificationHandler : INotificationHandler<FailedJobNotification>
    {
        public Task Handle(FailedJobNotification notification, CancellationToken cancellationToken)
        {
            E2ETests.IsFailedJobNotificationTriggered = true;
            return Task.CompletedTask;
        }
    }
}
