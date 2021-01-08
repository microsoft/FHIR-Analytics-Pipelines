// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Files.DataLake;
using Microsoft.Health.Fhir.Transformation.BatchExecutor;
using Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Test
{
    [TestClass]
    public class AdlsCsvSinkTest
    {
        private Dictionary<string, (object valueObj, object typeObj)> _testItem1 =
            new Dictionary<string, (object valueObj, object typeObj)>() 
            { 
                { "c1", ("value1", "type1") },
                { "c2", (1, "type2") },
                { "c3", (DateTime.Parse("2000-01-01"), "type3") }
            };
        private Dictionary<string, (object valueObj, object typeObj)> _testItem2 =
            new Dictionary<string, (object valueObj, object typeObj)>()
            {
                { "c1", ("value1", "type1") },
                { "c2", (1, "type2") },
                { "c3", (DateTime.Parse("2000-01-01"), "type3") },
                { "c4", ("value4", "type4") },
            };
        private string[] _testColumns = new string[] { "c1", "c2", "c3"};

        private TestSettings _testSettings;
        public AdlsCsvSinkTest()
        {
            _testSettings = TestUtils.LoadTestSettings();
        }

        [TestMethod]
        public async Task GivenAnAdlsSink_WhenWriteData_DataShouldBeWriteToFolder()
        {
            if (string.IsNullOrEmpty(_testSettings.AdlsAccountName))
            {
                return;
            }

            int testCount = 100;
            string TestRunId = Guid.NewGuid().ToString("N");

            var credential = Program.GetClientSecretCredential(_testSettings.TenantId, _testSettings.ClientId, _testSettings.Secret);
            var sink = new AdlsCsvSink(_testSettings.AdlsAccountName, TestRunId, credential);
            await sink.InitAsync();
            sink.MaxLengthForSingleAppendBlock = 3; // Append & flush for every 5 items 
            sink.ConcurrentCount = 10;
            sink.CsvFilePath = (string tableName) =>
            {
                return $"data/{tableName}-{TestRunId}";
            };

            for (int i = 0; i < testCount; ++i)
            {
                await sink.WriteAsync("table1", _testColumns, _testItem1);
                await sink.WriteAsync("table2", _testColumns, _testItem2);
            }
            await sink.CompleteAsync();

            var fileSystemClient = await sink.CreateFileSystemClientIfNotExistAsync();

            foreach (string tableName in new string[] { "table1", "table2"})
            {
                var fileClient = fileSystemClient.GetFileClient(sink.CsvFilePath(tableName));
                using Stream fileStream = fileClient.Read().Value.Content;
                using StreamReader reader = new StreamReader(fileStream);
                string content = reader.ReadToEnd();

                string[] lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                // First line is headers 
                Assert.AreEqual(1 + testCount, lines.Length);
            }
        }

        [TestMethod]
        public async Task GivenAnAdlsSink_WhenWriteDataTimeOut_OperationShouldBeRetry()
        {
            if (string.IsNullOrEmpty(_testSettings.AdlsAccountName))
            {
                return;
            }
            string TestRunId = Guid.NewGuid().ToString("N");

            var credential = Program.GetClientSecretCredential(_testSettings.TenantId, _testSettings.ClientId, _testSettings.Secret);
            var sink = new AdlsCsvSink(_testSettings.AdlsAccountName, TestRunId, credential);
            await sink.InitAsync();
            sink.CsvFilePath = (string tableName) =>
            {
                return $"data/{tableName}-{TestRunId}";
            };
            var previousFunc = sink.AppendContentAsync;
            
            int retryCount = 1;
            sink.AppendContentAsync = async (DataLakeFileClient fileClient, MemoryStream dataStream, int startOffset) =>
            {
                if (retryCount-- > 0)
                {
                    await Task.Delay(TimeSpan.FromMinutes(60));
                }

                await previousFunc(fileClient, dataStream, startOffset);
            };
            sink.OperationTimeoutInSeconds = 10;
            await sink.WriteAsync("table1", _testColumns, _testItem1);
            await sink.CompleteAsync();

            var fileSystemClient = await sink.CreateFileSystemClientIfNotExistAsync();

            var fileClient = fileSystemClient.GetFileClient(sink.CsvFilePath("table1"));
            using Stream fileStream = fileClient.Read().Value.Content;
            using StreamReader reader = new StreamReader(fileStream);
            string content = reader.ReadToEnd();

            string[] lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            // First line is headers 
            Assert.AreEqual(2, lines.Length);
        }
    }
}
