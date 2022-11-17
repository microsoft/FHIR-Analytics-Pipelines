// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Dicom.Client.Models;
using Microsoft.Health.Fhir.Synapse.Common.Authentication;
using Microsoft.Health.Fhir.Synapse.Common.Configurations;
using Microsoft.Health.Fhir.Synapse.Common.Logging;
using Microsoft.Health.Fhir.Synapse.DataClient.Exceptions;
using Microsoft.Health.Fhir.Synapse.DataClient.Models.DicomApiOption;
using Newtonsoft.Json;

namespace Microsoft.Health.Fhir.Synapse.DataClient.Api
{
    public class DicomApiDataClient : IDicomDataClient
    {
        private readonly HttpClient _httpClient;
        private readonly IDiagnosticLogger _diagnosticLogger;
        private readonly IAccessTokenProvider _accessTokenProvider;
        private readonly ILogger<DicomApiDataClient> _logger;

        public DicomApiDataClient(
            IFhirApiDataSource dataSource,
            HttpClient httpClient,
            ITokenCredentialProvider tokenCredentialProvider,
            IDiagnosticLogger diagnosticLogger,
            ILogger<DicomApiDataClient> logger)
        {
            EnsureArg.IsNotNull(dataSource, nameof(dataSource));
            EnsureArg.IsNotNullOrEmpty(dataSource.FhirServerUrl, nameof(dataSource.FhirServerUrl));
            EnsureArg.IsNotNull(httpClient, nameof(httpClient));
            EnsureArg.IsNotNull(tokenCredentialProvider, nameof(tokenCredentialProvider));
            EnsureArg.IsNotNull(diagnosticLogger, nameof(diagnosticLogger));
            EnsureArg.IsNotNull(logger, nameof(logger));

            _httpClient = new HttpClient { BaseAddress = new Uri(dataSource.FhirServerUrl) };
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/dicom+json");
            _accessTokenProvider = new AzureAccessTokenProvider(
                tokenCredentialProvider.GetCredential(TokenCredentialTypes.External),
                diagnosticLogger,
                new Logger<AzureAccessTokenProvider>(new LoggerFactory()));
            _diagnosticLogger = diagnosticLogger;
            _logger = logger;

            string accessToken = null;
            if (dataSource.Authentication == AuthenticationType.ManagedIdentity)
            {
                // Currently we support accessing FHIR server endpoints with Managed Identity.
                // Obtaining access token against a resource uri only works with Azure API for FHIR now.
                // To do: add configuration for OSS FHIR server endpoints.

                // The thread-safe AzureServiceTokenProvider class caches the token in memory and retrieves it from Azure AD just before expiration.
                // https://docs.microsoft.com/en-us/dotnet/api/overview/azure/service-to-service-authentication#using-the-library
                accessToken = _accessTokenProvider.GetAccessTokenAsync("https://dicom.healthcareapis.azure.com").Result;
            }

            _httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
        }

        public async Task<List<string>> GetMetadataAsync(ChangeFeedOptions changeFeedOptions, CancellationToken cancellationToken = default)
        {
            var result = new List<string>();
            var changeFeedEntries = await GetChangeFeedEntriesAsync(changeFeedOptions, cancellationToken);
            var tasks = changeFeedEntries
                .Select(entry => GetInstanceMetadataAsync(entry.StudyInstanceUid, entry.SeriesInstanceUid, entry.SopInstanceUid, cancellationToken));

            return new List<string>(await Task.WhenAll(tasks));
        }

        public async Task<long> GetLatestSequenceAsync(bool includeMetadata, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await _httpClient.GetAsync($"/v1/changefeed/latest?includemetadata={includeMetadata}", cancellationToken);
                var result = await response.Content.ReadAsStringAsync(cancellationToken);
                var changeFeedEntry = JsonConvert.DeserializeObject<ChangeFeedEntry>(result);

                return changeFeedEntry.Sequence;
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Querying latest changefeed failed. Reason: {ex.Message}");
                _logger.LogError($"Querying latest changefeed failed. Reason: {ex.Message}");
                throw new FhirSearchException("Querying latest changefeed failed.", ex);
            }
        }

        private async Task<string> GetInstanceMetadataAsync(string studyInstanceUid, string seriesInstanceUid, string sopInstanceUid, CancellationToken cancellationToken = default)
        {
            try
            {
                var response = await _httpClient.GetAsync($"/v1/studies/{studyInstanceUid}/series/{seriesInstanceUid}/instances/{sopInstanceUid}/metadata", cancellationToken);

                return await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Retrieving instance metadata failed. Reason: {ex.Message}");
                _logger.LogError($"Retrieving instance metadata failed. Reason: {ex.Message}");
                throw new FhirSearchException("Retrieving instance metadata failed.", ex);
            }
        }

        private async Task<List<ChangeFeedEntry>> GetChangeFeedEntriesAsync(ChangeFeedOptions changeFeedOptions, CancellationToken cancellationToken = default)
        {
            if (changeFeedOptions.IncludeMetadata)
            {
                _diagnosticLogger.LogError("includemetadata should be set to false in changefeed.");
                _logger.LogInformation("includemetadata should be set to false in changefeed.");
                throw new FhirSearchException("includemetadata should be set to false in changefeed.");
            }

            try
            {
                var response = await _httpClient.GetAsync(
                    $"/v1/changefeed?offset={changeFeedOptions.Offset}&limit={changeFeedOptions.Limit}&includeMetadata={changeFeedOptions.IncludeMetadata}",
                    cancellationToken);

                var content = await response.Content.ReadAsStringAsync(cancellationToken);

                return JsonConvert.DeserializeObject<List<ChangeFeedEntry>>(content)
                    .Where(x => x.State == ChangeFeedState.Current && x.Action == ChangeFeedAction.Create)
                    .ToList();
            }
            catch (Exception ex)
            {
                _diagnosticLogger.LogError($"Querying changefeed failed. Reason: {ex.Message}");
                _logger.LogError($"Querying changefeed failed. Reason: {ex.Message}");
                throw new FhirSearchException("Querying changefeed failed.", ex);
            }
        }
    }
}
