// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.CommandLine;
using System.CommandLine.Binding;
using System.CommandLine.Invocation;
using System.Threading.Tasks;
using Azure.Identity;
using Microsoft.CommonDataModel.ObjectModel.Cdm;
using Microsoft.CommonDataModel.ObjectModel.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Transformation.Cdm;
using Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor;
using Microsoft.Health.Fhir.Transformation.Core;

namespace Microsoft.Health.Fhir.Transformation.BatchExecutor
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            TransformationLogging.LoggerFactory = LoggerFactory.Create(builder => {
                builder.AddFilter("Microsoft", LogLevel.Warning)
                       .AddFilter("System", LogLevel.Warning)
                       .AddFilter("Microsoft.Health.Fhir.Transformation", LogLevel.Information)
                       .AddConsole();
            });
            System.Net.ServicePointManager.DefaultConnectionLimit = 10 * 1024;

            ILogger logger = TransformationLogging.CreateLogger<Program>();

            var rootCommand = new RootCommand();

            var generateSchemaCommand = new Command("generate-schema")
            {
                new Option<string>("--clientId"),
                new Option<string>("--tenantId"),
                new Option<string>("--adlsAccount"),
                new Option<string>("--cdmFileSystem"),
                new Option<string>("--configurationContainer", getDefaultValue: () => "config"),
                new Option<string>("--clientSecret"),
                new Option<int>("--maxDepth", getDefaultValue: () => 3)
            };
            generateSchemaCommand.Handler = CommandHandler.Create<string, string, string, string, string, string, int>(
                async (clientId, tenantId, adlsAccount, cdmFileSystem, configurationContainer, clientSecret, maxDepth) =>
                {
                    logger.LogInformation("Start to generate CDM schema.");
                    ClientSecretCredential credential = GetClientSecretCredential(tenantId, clientId, clientSecret);

                    StorageDefinitionLoader configLoader = new StorageDefinitionLoader(GetStorageServiceEndpoint(adlsAccount), configurationContainer, credential, maxDepth);
                    TabularMappingDefinition[] mappings = configLoader.Load(); 
                    
                    AdlsCsvSink sink = new AdlsCsvSink(adlsAccount, cdmFileSystem, credential);
                    await sink.InitAsync();
                    await sink.CreateFileSystemClientIfNotExistAsync();

                    CdmCorpusDefinition defination = InitAdlscdmCorpusDefinition(adlsAccount, "/" + cdmFileSystem, tenantId, clientId, clientSecret);
                    CdmSchemaGenerator cdmSchemaGenerator = new CdmSchemaGenerator(defination);
                    await cdmSchemaGenerator.InitializeCdmFolderAsync(mappings, "adls");
                    logger.LogInformation("Generate CDM schema completed.");
                });
            rootCommand.AddCommand(generateSchemaCommand);

            var transformDataCommand = new Command("transform-data")
            {
                new Option<string>("--clientId"),
                new Option<string>("--tenantId"),
                new Option<string>("--adlsAccount"),
                new Option<string>("--cdmFileSystem"),
                new Option<string>("--inputBlobUri"),
                new Option<string>("--configurationContainer"),
                new Option<string>("--clientSecret"),
                new Option<string>("--operationId"),
                new Option<string>("--maxDepth"),
            };

            Func<string, string, string, string, string, string, string, string, int, Task> transformDataAction =
                async (clientId, tenantId, adlsAccount, cdmFileSystem, inputBlobUri, configurationContainer, operationId, clientSecret, maxDepth) =>
                {
                    logger.LogInformation("Start to transform data.");
                    ClientSecretCredential credential = GetClientSecretCredential(tenantId, clientId, clientSecret);

                    StorageDefinitionLoader configLoader = new StorageDefinitionLoader(GetStorageServiceEndpoint(adlsAccount), configurationContainer, credential, maxDepth);
                    TabularMappingDefinition[] mappings = configLoader.Load();

                    ISource source = new StorageBlobNdjsonSource(new Uri(inputBlobUri), credential)
                    {
                        ConcurrentCount = Environment.ProcessorCount * 2
                    };

                    AdlsCsvSink sink = new AdlsCsvSink(adlsAccount, cdmFileSystem, credential)
                    {
                        CsvFilePath = (string tableName) =>
                        {
                            return $"data/Local{tableName}/partition-data-{operationId}.csv";
                        },
                        ConcurrentCount = Environment.ProcessorCount * 2
                    };

                    TransformationExecutor executor = new TransformationExecutor(source,
                                                                                 sink,
                                                                                 mappings,
                                                                                 new BasicFhirElementTabularTransformer());
                    executor.ConcurrentCount = Environment.ProcessorCount * 2;
                    IProgress<(int, int)> progressHandler = new Progress<(int, int)>(progress =>
                    {
                        if (progress.Item1 % 100 == 0 || progress.Item2 % 100 == 0)
                        {
                            logger.LogInformation($"({progress.Item1} loaded, {progress.Item2} transformed) to CDM folder. {DateTime.UtcNow.ToLongTimeString()}");
                        }
                    });

                    await executor.ExecuteAsync(progressHandler);
                    logger.LogInformation("Transform data complete.");
                };
            transformDataCommand.Handler = HandlerDescriptor.FromDelegate(transformDataAction).GetCommandHandler();
            rootCommand.AddCommand(transformDataCommand);

            await rootCommand.InvokeAsync(args);
        }

        private static CdmCorpusDefinition InitAdlscdmCorpusDefinition(string account, string fileSystemRoot, string tenantId, string clientId, string secret)
        {
            var cdmCorpus = new CdmCorpusDefinition();
            cdmCorpus.Storage.Mount("adls", new ADLSAdapter($"{account}.dfs.core.windows.net", fileSystemRoot, tenantId, clientId, secret));
            cdmCorpus.Storage.DefaultNamespace = "adls";
            return cdmCorpus;
        }

        private static Uri GetStorageServiceEndpoint(string accountName)
        {
            return new Uri($"https://{accountName}.blob.core.windows.net");
        }

        public static ClientSecretCredential GetClientSecretCredential(string tenantId, string clientId, string clientSecret)
        {
            return new ClientSecretCredential(
                tenantId, clientId, clientSecret, new TokenCredentialOptions());
        }
    }
}
