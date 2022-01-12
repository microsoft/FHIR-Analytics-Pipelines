// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Health.Fhir.Synapse.Common.Configurations.Arrow;
using Microsoft.Health.Fhir.Synapse.SchemaManagement.Parquet;
using Xunit;

namespace Microsoft.Health.Fhir.Synapse.Parquet.CLR.UnitTests
{
    public class ParquetConverterWrapperTests
    {
        [Fact]
        public void GivenValidSchemaMapAndArrowConfiguration_WhenCreateParquetConverterWrapper_ParquetConverterWrapperShouldBeCreated()
        {
            _ = new ParquetConverterWrapper(TestConstants.TestSchemaMap);
            _ = new ParquetConverterWrapper(TestConstants.TestSchemaMap, new ArrowConfiguration());
        }

        [Fact]
        public void GivenANullSchemaMapOrArrowConfiguration_WhenCreateParquetConverterWrapper_ExceptionShouldBeThrown()
        {
            Assert.Throws<ArgumentNullException>(() => new ParquetConverterWrapper(null, new ArrowConfiguration()));
            Assert.Throws<ArgumentNullException>(() => new ParquetConverterWrapper(new Dictionary<string, FhirParquetSchemaNode>(), null));
            Assert.Throws<ArgumentNullException>(() => new ParquetConverterWrapper(null, null));
        }

        [Fact]
        public void GivenValidInputData_WhenConvertToParquetStream_CorrectStreamShouldBeReturned()
        {
            var parquetConverterWrapper = new ParquetConverterWrapper(TestConstants.TestSchemaMap, new ArrowConfiguration());
            var inputStream = new MemoryStream(Encoding.UTF8.GetBytes("{\"resourceType\":\"Patient\",\"id\":\"example\"}"));
            var outputStream = parquetConverterWrapper.ConvertToParquetStream("Patient", inputStream);
            Assert.True(outputStream.Length > 0);
        }

        [Fact]
        public void GivenNullOrEmptyInputStream_WhenConvertToParquetStream_ExceptionShouldBeThrown()
        {
            var parquetConverterWrapper = new ParquetConverterWrapper(TestConstants.TestSchemaMap);
            Assert.Throws<ArgumentException>(() => parquetConverterWrapper.ConvertToParquetStream("Patient", null));
            Assert.Throws<ArgumentException>(() => parquetConverterWrapper.ConvertToParquetStream("Patient", new MemoryStream(Encoding.UTF8.GetBytes(string.Empty))));
        }

        [Fact]
        public void GivenNullResourceType_WhenConvertToParquetStream_ExceptionShouldBeThrown()
        {
            var parquetConverterWrapper = new ParquetConverterWrapper(TestConstants.TestSchemaMap);
            Assert.Throws<ArgumentNullException>(() => parquetConverterWrapper.ConvertToParquetStream(null, new MemoryStream(Encoding.UTF8.GetBytes("content"))));
        }

        [Theory]
        [InlineData("")]
        [InlineData("\n")]
        [InlineData("InvalidResourceType")]
        public void GivenUnsupportedResourceType_WhenConvertToParquetStream_ExceptionShouldBeThrown(string resourceType)
        {
            var parquetConverterWrapper = new ParquetConverterWrapper(TestConstants.TestSchemaMap);
            Assert.Throws<ArgumentException>(() => parquetConverterWrapper.ConvertToParquetStream(resourceType, new MemoryStream(Encoding.UTF8.GetBytes("content"))));
        }
    }
}