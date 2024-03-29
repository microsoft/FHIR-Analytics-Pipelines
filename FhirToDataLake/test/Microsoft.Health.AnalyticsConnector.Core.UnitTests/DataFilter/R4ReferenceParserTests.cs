﻿// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.Common.Models.FhirSearch;
using Microsoft.Health.AnalyticsConnector.Core.DataFilter;
using Microsoft.Health.AnalyticsConnector.Core.Exceptions;
using Microsoft.Health.AnalyticsConnector.DataClient.Api;
using Xunit;

namespace Microsoft.Health.AnalyticsConnector.Core.UnitTests.DataFilter
{
    public class R4ReferenceParserTests
    {
        private readonly ApiDataSource _dataSource;

        private readonly R4ReferenceParser _referenceParser;

        private readonly NullLogger<R4ReferenceParser> _nullR4ReferenceParserLogger =
            NullLogger<R4ReferenceParser>.Instance;

        private const string _serverUrl = "https://example.com";

        public R4ReferenceParserTests()
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    ServerUrl = _serverUrl,
                    Authentication = AuthenticationType.None,
                },
            };

            _dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));
            _referenceParser = new R4ReferenceParser(_dataSource, _nullR4ReferenceParserLogger);
        }

        [Fact]
        public void GivenNullInputParameters_WhenInitialize_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(
                () => new R4ReferenceParser(null, _nullR4ReferenceParserLogger));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void GivenNullOrWhiteSpaceReference_WhenParseReference_ExceptionShouldBeThrown(string reference)
        {
            var exception = Assert.Throws<ReferenceParseException>(() => _referenceParser.Parse(reference));
            Assert.Equal("The reference string is null or white space.", exception.Message);
        }

        [Theory]
        [InlineData("Patient/123")]
        [InlineData("Patient/123/_history/1")]
        [InlineData("https://example.com/Patient/123")]
        [InlineData("https://example.com/Patient/123/_history/2")]
        public void GivenValidReference_WhenParseReference_ParsedReferenceShouldBeReturned(string reference)
        {
            FhirReference fhirReference = _referenceParser.Parse(reference);
            Assert.NotNull(fhirReference);
            Assert.Equal("Patient", fhirReference.ResourceType);
            Assert.Equal("123", fhirReference.ResourceId);
        }

        [Fact]
        public void GivenDataSourceServerUrlEndsWithSlash_WhenParseReference_ParsedReferenceShouldBeReturned()
        {
            var dataSourceConfiguration = new DataSourceConfiguration
            {
                FhirServer = new FhirServerConfiguration
                {
                    ServerUrl = "https://example.com/",
                    Authentication = AuthenticationType.None,
                },
            };

            var dataSource = new ApiDataSource(Options.Create(dataSourceConfiguration));
            var referenceParser = new R4ReferenceParser(dataSource, _nullR4ReferenceParserLogger);

            FhirReference fhirReference = referenceParser.Parse("https://example.com/Patient/123/_history/2");
            Assert.NotNull(fhirReference);
            Assert.Equal("Patient", fhirReference.ResourceType);
            Assert.Equal("123", fhirReference.ResourceId);
        }

        [Theory]
        [InlineData("patient/123")]
        [InlineData("Patient")]
        [InlineData("123")]
        [InlineData("unKnownType/123")]
        [InlineData("invalidUriFormat/Patient/pat10")]
        [InlineData("/123")]
        [InlineData("#p1")]
        public void GivenInvalidReference_WhenParseReference_ExceptionShouldBeThrown(string reference)
        {
            var exception = Assert.Throws<ReferenceParseException>(() => _referenceParser.Parse(reference));
            Assert.Equal($"Fail to parse reference {reference}.", exception.Message);
        }

        [Theory]
        [InlineData("http://somehwere.com/Patient/123")]
        [InlineData("http://fhir.hl7.org/svc/StructureDefinition/c8973a22-2b5b-4e76-9c66-00639c99e61b")]
        public void GivenExternalAbsoluteURL_WhenParseReference_ExceptionShouldBeThrown(string reference)
        {
            var exception = Assert.Throws<ReferenceParseException>(() => _referenceParser.Parse(reference));
            Assert.Equal($"The reference {reference} is an absolute URL pointing to an external resource.", exception.Message);
        }
    }
}
