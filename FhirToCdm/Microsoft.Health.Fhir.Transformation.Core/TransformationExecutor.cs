// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Task = System.Threading.Tasks.Task;

namespace Microsoft.Health.Fhir.Transformation.Core
{
    public class TransformationExecutor
    {
        private int _completedItemCount = 0;
        private int _loadedItemCount = 0;
        private ISource _source;
        private ISink _sink;
        private FhirElementTabularTransformer _transformer;
        private IEnumerable<TabularMappingDefinition> _tabularMappings;
        private LinkedList<Task<IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>>> _transformationTasks;

        public TransformationExecutor(ISource source, ISink sink, IEnumerable<TabularMappingDefinition> tabularMappings, FhirElementTabularTransformer transformer)
        {
            _source = source;
            _sink = sink;
            _transformer = transformer;
            _tabularMappings = tabularMappings;
            _transformationTasks = new LinkedList<Task<IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>>>();
        }

        public int ConcurrentCount
        {
            get;
            set;
        } = ExecutionConstants.DefaultConcurrentCount;

        public Func<string, FhirJsonParser, IEnumerable<TabularMappingDefinition>, FhirElementTabularTransformer, Task<IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>>> TransformToTabularEntitiesAsync
        {
            get;
            set;
        } = TransformToTabularEntitiesInternalAsync;

        public Func<IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>, ISink, Task> WriteEntitiesAsync
        {
            get;
            set;
        } = WriteEntitiesInternalAsync;

        public async Task ExecuteAsync(IProgress<(int loadedCount, int completedCount)> progress = null)
        {
            await _source.OpenAsync();
            await _sink.InitAsync();
            _completedItemCount = 0;
            _loadedItemCount = 0;

            try
            {
                string content;
                FhirJsonParser parser = new FhirJsonParser();

                while (!string.IsNullOrEmpty((content = await _source.ReadAsync())))
                {
                    Interlocked.Increment(ref _loadedItemCount);
                    while (_transformationTasks.Count() >= ConcurrentCount)
                    {
                        await CompleteFirstTaskAsync();
                        progress?.Report((_loadedItemCount, _completedItemCount));
                    }

                    _transformationTasks.AddLast(TransformToTabularEntitiesAsync(content, parser, _tabularMappings, _transformer));
                }

                while (_transformationTasks.Count() > 0)
                {
                    await CompleteFirstTaskAsync();
                }
            }
            finally
            {
                await _source.CloseAsync();
                await _sink.CompleteAsync();
            }
        }

        private async Task CompleteFirstTaskAsync()
        {
            var results = await _transformationTasks.First();
            await WriteEntitiesAsync(results, _sink);
            _transformationTasks.RemoveFirst();

            Interlocked.Increment(ref _completedItemCount);
        }

        private static async Task<IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)>> TransformToTabularEntitiesInternalAsync(string resourceContent, FhirJsonParser parser, IEnumerable<TabularMappingDefinition> tabularMappings, FhirElementTabularTransformer transformer)
        {
            return await Task.Run(() =>
            {
                Resource resource = parser.Parse<Resource>(resourceContent);
                var result = new List<(string tbaleName, string[] columns, Dictionary<string, (object, object)>[] entities)>();
                string resourceType = resource.ResourceType.ToString();
                foreach (var table in tabularMappings.Where(m => m.ResourceType.Equals(resourceType)))
                {
                    result.Add((table.TableName, table.Columns.Select(c => c.columnName).ToArray(), transformer.ToTabular(resource, table).ToArray()));
                }

                return result;
            });
        }

        private static async Task WriteEntitiesInternalAsync(IEnumerable<(string tableName, string[] columns, Dictionary<string, (object, object)>[] entities)> entityItems, ISink sink)
        {
            foreach (var entityItem in entityItems.SelectMany(item => item.entities.Select(entity => (item.tableName, item.columns, entity))))
            {
                await sink.WriteAsync(entityItem.tableName, entityItem.columns, entityItem.entity);
            }
        }
    }
}
