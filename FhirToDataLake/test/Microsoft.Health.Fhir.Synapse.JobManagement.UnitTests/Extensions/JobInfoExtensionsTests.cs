// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using Microsoft.Health.Fhir.Synapse.JobManagement.Extensions;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.JobManagement.UnitTests.Extensions
{
    public class JobInfoExtensionsTests
    {
        public static IEnumerable<object[]> NullOrEmptyInput =>
            new List<object[]>
            {
                new object[]
                {
                    null,
                    null,
                },
                new object[]
                {
                    string.Empty,
                    string.Empty,
                },
            };

        public static IEnumerable<object[]> DifferentInputs =>
            new List<object[]>
            {
                // case sensitive
                new object[]
                {
                    "input string",
                    "Input string",
                },

                // space
                new object[]
                {
                    "input string",
                    "input string ",
                },

                // different string
                new object[]
                {
                    "input string",
                    "another string",
                },
            };

        [Theory]
        [MemberData(nameof(NullOrEmptyInput))]
        public void GivenNullOrEmptyString_WhenComputeHash_TheInputShouldBeReturn(string inputStr, string expectedHash)
        {
            var hash = inputStr.ComputeHash();
            Assert.Equal(expectedHash, hash);
        }

        [Fact]
        public void GivenValidString_WhenComputeHash_TheHashStringShouldBeReturn()
        {
            var inputStr = "input string";
            var hash = inputStr.ComputeHash();
            Assert.NotEmpty(hash);
        }

        [Fact]
        public void GivenTwoSameStrings_WhenComputeHash_TheHashStringsShouldBeTheSame()
        {
            var inputStr1 = "same input string";
            var hash1 = inputStr1.ComputeHash();

            var inputStr2 = "same input string";
            var hash2 = inputStr2.ComputeHash();
            Assert.Equal(hash1, hash2);
        }

        [Theory]
        [MemberData(nameof(DifferentInputs))]
        public void GivenDifferentStrings_WhenComputeHash_TheHashStringsShouldBeDifferent(string inputStr1, string inputStr2)
        {
            var hash1 = inputStr1.ComputeHash();
            var hash2 = inputStr2.ComputeHash();
            Assert.NotEqual(hash1, hash2);
        }
    }
}