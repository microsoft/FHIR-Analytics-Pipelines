// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using EnsureThat;
using Microsoft.Extensions.Options;
using Microsoft.Health.AnalyticsConnector.Common;
using Microsoft.Health.AnalyticsConnector.Common.Configurations;
using Microsoft.Health.AnalyticsConnector.DataClient.Api.Dicom;
using Microsoft.Health.AnalyticsConnector.DataClient.Exceptions;

namespace Microsoft.Health.AnalyticsConnector.DataClient.Api
{
    /// <summary>
    /// The data source from service search API.
    /// </summary>
    public class ApiDataSource : IApiDataSource
    {
        public ApiDataSource(IOptions<DataSourceConfiguration> config)
        {
            EnsureArg.IsNotNull(config, nameof(config));

            var dataSourceType = config.Value.Type;

            switch (dataSourceType)
            {
                case DataSourceType.FHIR:
                    ServerUrl = AddSlashForUrl(
                        EnsureArg.IsNotNullOrEmpty(config.Value.FhirServer.ServerUrl, nameof(config.Value.FhirServer.ServerUrl)));
                    Authentication = config.Value.FhirServer.Authentication;
                    Audience = ServerUrl;
                    break;
                case DataSourceType.DICOM:
                    var dicomServerUrl = AddSlashForUrl(
                        EnsureArg.IsNotNullOrEmpty(config.Value.DicomServer.ServerUrl, nameof(config.Value.DicomServer.ServerUrl)));
                    ServerUrl = $"{dicomServerUrl}{DicomApiConstants.VersionMap[config.Value.DicomServer.ApiVersion]}/";
                    Authentication = config.Value.DicomServer.Authentication;
                    Audience = DicomApiConstants.DicomServerAudienceMap[config.Value.DicomServer.ServerEnvironmentGroup];
                    break;
                default:
                    // Should not be thrown.
                    throw new ApiSearchException($"Unsupported data source type: {dataSourceType}");
            }
        }

        public string ServerUrl { get; }

        public string Audience { get; }

        public AuthenticationType Authentication { get; }

        private static string AddSlashForUrl(string serverUrl)
        {
            EnsureArg.IsNotNullOrEmpty(serverUrl, nameof(serverUrl));

            // If the baseUri has relative parts (like /api), then the relative part must be terminated with a slash (like /api/).
            // Otherwise the relative part will be omitted when creating new search Uris. See https://docs.microsoft.com/en-us/dotnet/api/system.uri.-ctor?view=net-6.0
            return !serverUrl.EndsWith("/") ? $"{serverUrl}/" : serverUrl;
        }
    }
}
