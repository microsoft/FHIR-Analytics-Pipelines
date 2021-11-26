// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Threading.Tasks;
using Microsoft.CommonDataModel.ObjectModel.Cdm;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Fhir.Transformation.Core;
using Microsoft.Health.Fhir.Transformation.Core.TabularDefinition;

namespace Microsoft.Health.Fhir.Transformation.Cdm.Tool
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

            ILogger logger = TransformationLogging.CreateLogger<Program>();
            
            var rootCommand = new RootCommand()
            {
                new Option<string>("--config"),
                new Option<string>("--input"),
                new Option<string>("--output"),
                new Option<int>("--maxDepth", getDefaultValue: () => 3),
            };

            rootCommand.Handler = CommandHandler.Create(
                new Func<string, string, string, int, Task>(async (config, input, output, maxDepth) =>
            {
                LocalMappingDefinitionLoader configLoader = new LocalMappingDefinitionLoader(config, maxDepth);
                TabularMappingDefinition[] mappings = configLoader.Load();
                FhirElementTabularTransformer transformer = new BasicFhirElementTabularTransformer();

                logger.LogInformation("Start to generate CDM schema.");
                CdmCorpusDefinition defination = CdmSchemaGenerator.InitLocalcdmCorpusDefinition(output);
                CdmSchemaGenerator cdmSchemaGenerator = new CdmSchemaGenerator(defination);
                cdmSchemaGenerator.InitializeCdmFolderAsync(mappings).Wait();
                logger.LogInformation("Generate CDM schema completed.");

                string operationId = Guid.NewGuid().ToString("N");
                ISource source = new LocalNdjsonSource(input);
                ISink sink = new LocalCsvSink(output)
                {
                    CsvFilePath = (string tableName) =>
                    {
                        return $"data/Local{tableName}/partition-data-{operationId}.csv";
                    }
                };

                logger.LogInformation("Start to transform data.");
                IProgress<(int, int)> progressHandler = new Progress<(int, int)>(progress =>
                {
                    if (progress.Item1 % 100 == 0 || progress.Item2 % 100 == 0)
                    {
                        logger.LogInformation($"({progress.Item1} loaded, {progress.Item2} transformed) to CDM folder. {DateTime.UtcNow.ToLongTimeString()}");
                    }
                });

                TransformationExecutor executor = new TransformationExecutor(source, sink, mappings, transformer);
                await executor.ExecuteAsync(progressHandler);
                logger.LogInformation("Complete to transform data.");
            }));

            await rootCommand.InvokeAsync(args);
        }
    }
}
