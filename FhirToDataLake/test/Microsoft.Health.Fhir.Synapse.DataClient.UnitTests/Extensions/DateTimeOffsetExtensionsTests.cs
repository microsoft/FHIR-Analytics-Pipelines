// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.DataClient.Extensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.DataClient.UnitTests.Extensions
{
    public class DateTimeOffsetExtensionsTests
    {
        public static IEnumerable<object[]> DateTimeOffsetInstantValues =>
            new List<object[]>
            {
                new object[] { default(DateTimeOffset), "0001-01-01T00:00:00+00:00" },
                new object[]
                {
                    new DateTimeOffset(2008, 5, 1, 8, 6, 32, new TimeSpan(1, 0, 0)),
                    "2008-05-01T08:06:32+01:00",
                },
                new object[]
                {
                    new DateTimeOffset(2021, 9, 9, 8, 0, 30, new TimeSpan(-8, 0, 0)),
                    "2021-09-09T08:00:30-08:00",
                },
            };

        [Theory]
        [MemberData(nameof(DateTimeOffsetInstantValues))]
        public void GivenADateTimeOffset_WhenTransformToInstantString_CorrectValueShouldBeReturned(
            DateTimeOffset dateTimeOffset,
            string expectedInstantValue)
        {
            var instantValue = dateTimeOffset.ToInstantString();
            Assert.Equal(expectedInstantValue, instantValue);
        }
    }
}
