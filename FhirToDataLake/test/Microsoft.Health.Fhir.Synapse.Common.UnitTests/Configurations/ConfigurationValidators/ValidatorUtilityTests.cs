// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.ConfigurationValidators;
using Microsoft.Health.Fhir.Synapse.Common.Exceptions;
using Microsoft.Health.Fhir.Synapse.Common.Models.Jobs;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Common.UnitTests.Configurations.ConfigurationValidators
{
    public class ValidatorUtilityTests
    {
        public static IEnumerable<object[]> GetInvalidImageReference()
        {
            yield return new object[] { "testacr.azurecr.io@v1" };
            yield return new object[] { "testacr.azurecr.io:templateset:v1" };
            yield return new object[] { "testacr.azurecr.io_v1" };
            yield return new object[] { "testacr.azurecr.io:v1" };
            yield return new object[] { "testacr.azurecr.io/" };
            yield return new object[] { "/testacr.azurecr.io" };
            yield return new object[] { "testacr.azurecr.io/name:" };
            yield return new object[] { "testacr.azurecr.io/:tag" };
            yield return new object[] { "testacr.azurecr.io/name@" };
            yield return new object[] { "testacr.azurecr.io/INVALID" };
            yield return new object[] { "testacr.azurecr.io/invalid_" };
            yield return new object[] { "testacr.azurecr.io/in*valid" };
            yield return new object[] { "testacr.azurecr.io/org/org/in*valid" };
            yield return new object[] { "testacr.azurecr.io/invalid____set" };
            yield return new object[] { "testacr.azurecr.io/invalid....set" };
            yield return new object[] { "testacr.azurecr.io/invalid._set" };
            yield return new object[] { "testacr.azurecr.io/_invalid" };

            // Invalid case sensitive
            yield return new object[] { "testacr.azurecr.io/Templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/TEMPLATESET:v1" };
        }

        public static IEnumerable<object[]> GetValidImageReference()
        {
            yield return new object[] { "testacr.azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/templateset@sha256:e6dcff9eaf7604aa7a855e52b2cda22c5cfc5cadaa035892557c4ff19630b612" };
            yield return new object[] { "testacr.azurecr.io/templateset" };
            yield return new object[] { "testacr.azurecr.io/org/templateset" };
            yield return new object[] { "testacr.azurecr.io/org/template-set" };
            yield return new object[] { "testacr.azurecr.io/org/template.set" };
            yield return new object[] { "testacr.azurecr.io/org/template__set" };
            yield return new object[] { "testacr.azurecr.io/org/template-----set" };
            yield return new object[] { "testacr.azurecr.io/org/template-set_set.set" };
            yield return new object[] { "testacr.azurecr.io/templateset:V1" };

            // Valid case sensitive
            yield return new object[] { "Testacr.azurecr.io/templateset:v1" };
            yield return new object[] { "TESTACR.azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.Azurecr.io/templateset:v1" };
            yield return new object[] { "testacr.azurecr.IO/templateset:v1" };
            yield return new object[] { "testacr.azurecr.io/templateset:V1" };
        }

        public static IEnumerable<object[]> GetInValidQueueOrTableName()
        {
            yield return new object[] { "1beginweithnumberic" };
            yield return new object[] { "agent_name" };
            yield return new object[] { "agent name" };
            yield return new object[] { string.Empty };
            yield return new object[] { "685c4e36859149cdb88e9a1b75485d7b" };
            yield return new object[] { " " };
        }

        public static IEnumerable<object[]> GetValidTableOrQueueName()
        {
            yield return new object[] { "agentname" };
            yield return new object[] { "agentName" };
            yield return new object[] { "agentName1" };
            yield return new object[] { "a12345" };
            yield return new object[] { "AGENT" };
            yield return new object[] { "a1g2e3n4t5" };
            yield return new object[] { "agent685c4e36859149cdb88e9a1b75485d7b" };
        }

        [Fact]
        public void GivenInvalidFilterConfiguration_WhenValidate_ExceptionShouldBeThrown()
        {
            var config = new FilterConfiguration
            {
                FilterScope = FilterScope.Group,
                GroupId = string.Empty,
            };

            Assert.Throws<ConfigurationErrorException>(() => ValidateUtility.ValidateFilterConfiguration(config));
        }

        [Theory]
        [MemberData(nameof(GetInValidQueueOrTableName))]
        public void GivenInvalidAgentName_WhenValidate_ExceptionShouldBeThrown(string name)
        {
            Assert.Throws<ConfigurationErrorException>(() => ValidateUtility.ValidateQueueOrTableName(name));
        }

        [Theory]
        [MemberData(nameof(GetValidTableOrQueueName))]
        public void GivenValidAgentName_WhenValidate_NoExceptionShouldBeThrown(string name)
        {
            Exception exception = Record.Exception(() => ValidateUtility.ValidateQueueOrTableName(name));
            Assert.Null(exception);
        }

        [Theory]
        [MemberData(nameof(GetInvalidImageReference))]
        public void GivenInvalidImageReference_WhenValidate_ExceptionShouldBeThrown(string imageReference)
        {
            Assert.Throws<ConfigurationErrorException>(() => ValidateUtility.ValidateImageReference(imageReference));
        }

        [Theory]
        [MemberData(nameof(GetValidImageReference))]
        public void GivenValidImageReference_WhenValidate_NoExceptionShouldBeThrown(string imageReference)
        {
            Exception exception = Record.Exception(() => ValidateUtility.ValidateImageReference(imageReference));
            Assert.Null(exception);
        }
    }
}
