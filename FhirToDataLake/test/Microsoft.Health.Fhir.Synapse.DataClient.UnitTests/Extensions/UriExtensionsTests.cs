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
    public class UriExtensionsTests
    {
        public static IEnumerable<object[]> UriWithParameters =>
            new List<object[]>
            {
                new object[]
                {
                    "https://test.example.com",
                    new List<KeyValuePair<string, string>>(),
                    "https://test.example.com/",
                },
                new object[]
                {
                    "https://test.example.com/",
                    new List<KeyValuePair<string, string>>(),
                    "https://test.example.com/",
                },
                new object[]
                {
                    "https://test.example.com",
                    new List<KeyValuePair<string, string>>
                    {
                        new KeyValuePair<string, string>("a", "123"),
                        new KeyValuePair<string, string>("b", "cfb"),
                    },
                    "https://test.example.com/?a=123&b=cfb",
                },
                new object[]
                {
                    "https://test.example.com",
                    new List<KeyValuePair<string, string>>
                    {
                        new KeyValuePair<string, string>("a", "123"),
                        new KeyValuePair<string, string>("ct", "Y29udGludWF0aW9udG9rZW4="),
                    },
                    "https://test.example.com/?a=123&ct=Y29udGludWF0aW9udG9rZW4%3d",
                },
                new object[]
                {
                    "https://test.example.com",
                    new List<KeyValuePair<string, string>>
                    {
                        new KeyValuePair<string, string>("_lastUpdateTime", "ge2021-01-01"),
                        new KeyValuePair<string, string>("_lastUpdateTime", "lt2021-08-01"),
                    },
                    "https://test.example.com/?_lastUpdateTime=ge2021-01-01&_lastUpdateTime=lt2021-08-01",
                },
            };

        [Theory]
        [MemberData(nameof(UriWithParameters))]
        public void GivenAUri_WhenAddParameter_CorrectValueShouldBeReturned(
            string uriValue,
            IEnumerable<KeyValuePair<string, string>> parameters,
            string expectedUriValue)
        {
            var uri = new Uri(uriValue);
            var newUri = uri.AddQueryString(parameters);
            Assert.Equal(expectedUriValue, newUri.ToString());
        }
    }
}
