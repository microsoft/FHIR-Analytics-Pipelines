# Health check 

* Health check is a background service, which will not stop until task canceled. There is a loop to run health check engine at set intervals (_healthCheckTimeIntervalInSeconds_ in config).
* Health check engine will execute a list of async health checker tasks with a timeout limitation (_healthCheckTimeoutInSeconds_ in config).
* There are five health checkers : filter ACR, schema ACR, FHIR server, azure blob and scheduler service. Scheduler service is an internal component and is critical for our pipeline service.
Other components are external components and incritical.

# Unit Tests

## Checkers
### Filter & Schema ACR 

1. **Null Input Parameters** - if one of the input parameters is null, should throw `ArgumentNullException`.

2.  **Invalid Filter Location**
    1. If ImageReference's format invalid, healthcheck failed.
    2. If ImageReference's not exist, healthcheck failed.

3. **Real ACR test**

    Set environment variable - _TestContainerRegistryServer_, _TestContainerRegistryPassword_

* Health check will succeed if ACR is accessble.
* Health check will fail if ACR is unaccessble.
* The test will skip if environment variable is not given.

### FHIR Server

1. **Null Input Parameters** - if one of the input parameters is null, should throw `ArgumentNullException`.

2. **Healthy FHIR search** - Mock a FHIR server client which returns search results as expected, health check will succeed.

3. **Unhealthy FHIR search** - Mock a FHIR server client which throws exception when searching, health check will fail.

### Azure Blob

1. **Null Input Parameters** - if one of the input parameters is null, should throw `ArgumentNullException`.

2. **Healthy azure blob** - Mock an azure blob which can update/get/move/delete successfully, health check will succeed.

3. **Unhealthy azure blob** - Health check will fail if any of update/get/move/delete functions fail.

### Scheduler Service

1. **Null Input Parameters** - if scheduler service is a null instance, should throw `ArgumentNullException`.

2. **Healthy scheduler service** - If scheduler service's heartbeat is not expired, the healthy check will succeed.

2. **Unhealthy scheduler service** - If scheduler service's heartbeat is expired, service is inactive and healthy check will succeed.

## Health check engine

1. Given a list of health checkers, after running engine, the health check result will return as expected with correct status.

2. **Timeout** - If the health check exceeds time limitation, the status of checkers is unhealthy.
# E2E Tests

## ACR
1. **Invalid image reference format**

The pipeline will stop and throw ConfigurationErrorException

2. **Not exist registry url**
```
Health check component SchemaAzureContainerRegistry:CanRead: read ACR quwanacr.azurecr.io/notfound:default failed: Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions.ContainerRegistryTokenException: Failed to get ACR access token with AAD access token.
       ---> System.Net.Http.HttpRequestException: No such host is known. (quwanacr.azurecr.io:443)
       ---> System.Net.Sockets.SocketException (11001): No such host is known.
       ...
```

3.  **Image Not Exist**: Given valid image reference but tag or digest not exist.

```
Health check component SchemaAzureContainerRegistry:CanRead: read ACR quwanacr0408.azurecr.io/customizedtemplate@notfound failed: Microsoft.Azure.ContainerRegistry.Models.AcrErrorsException: Operation returned an invalid status code 'NotFound'

```
4. **Image with invalid mediatype** (will throw exception only for using digest) : Given valid image reference with digest which mediatype is not `application/vnd.docker.distribution.manifest.v2+json`
```
Health check component SchemaAzureContainerRegistry:CanRead: read ACR quwanacr0408.azurecr.io/customizedtemplate@sha256:59e2eb0855af9419c889ba40246c61e49381cb8604efca00c0525b02e62254ba failed: Microsoft.Azure.ContainerRegistry.Models.AcrErrorsException: Operation returned an invalid status code 'NotFound'
```
5. **Authentication Failed**
```
Health check component FilterAzureContainerRegistry:CanRead: read ACR xiatia.azurecr.io/test:test failed: Microsoft.Health.Fhir.Synapse.SchemaManagement.Exceptions.ContainerRegistryTokenException: Failed to exchange ACR refresh token: ACR server xiatia.azurecr.io is unauthorized.
```
## FHIR Server

1. **Empty FHIR Server Url**

The pipeline will stop and throw ConfigurationErrorException

2. **Invalid FHIR server url**

```
Health check component FhirService:CanRead is unhealthy. Failed reason: Read from FHIR server failed.Input string was not in a correct format.
```

3. **Not exist FHIR Server**

Will **retry 5 times** (around 64 seconds) and will **open circuit breaker** after several health checks.

```
 Health check component FhirService:CanRead: read FHIR server failed: Microsoft.Health.Fhir.Synapse.DataClient.Exceptions.FhirSearchException: Search FHIR server failed. Search url: 'https://notexist.azurehealthcareapis.com/Patient?_count=1000'.
       ---> System.Net.Http.HttpRequestException: No such host is known. (notexist.azurehealthcareapis.com:443)
       ---> System.Net.Sockets.SocketException (11001): No such host is known.
       ...
```

4. **Authentication Failed**
```
Health check component FhirService:CanRead: read FHIR server failed: Microsoft.Health.Fhir.Synapse.DataClient.Exceptions.FhirSearchException: Search FHIR server failed. Search url: 'https://ferris-service.fhir.azurehealthcareapis.com/Patient?_count=1000'.
       ---> System.Net.Http.HttpRequestException: Response status code does not indicate success: 403 (Forbidden).
         at System.Net.Http.HttpResponseMessage.EnsureSuccessStatusCode()
       ...
```
## Azure Blob
1. **Empty azure blob url**

The pipeline will stop and throw ConfigurationErrorException

2. **Invalid azure blob url format**

Will retry 6 times.

```
Health check component AzureBlobStorage:CanReadWrite: write content to storage account failed: Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions.AzureBlobOperationFailedException: Create container testtest failed.
       ---> System.AggregateException: Retry failed after 6 tries. Retry settings can be adjusted in ClientOptions.Retry. (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)) (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)) (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)) (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)) (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)) (No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443))
       ---> Azure.RequestFailedException: No such host is known. (yufeidfs041fjcttqe2ogcig.core.windows.net:443)
```

3. **Azure blob without datalake**
```
 Health check component AzureBlobStorage:CanReadWrite: check hierachical namespace enabled in storage account failed: Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions.AzureBlobOperationFailedException: Failed to delete blob directory '__healthcheck__/e274abca7c2d41bfb2f71e8bb8951b1b/target'.
       ---> System.AggregateException: Retry failed after 6 tries. Retry settings can be adjusted in ClientOptions.Retry. (No such host is known. (testtongwu0624.dfs.core.windows.net:443)) (No such host is known. (testtongwu0624.dfs.core.windows.net:443)) (No such host is known. (testtongwu0624.dfs.core.windows.net:443)) (No such host is known. (testtongwu0624.dfs.core.windows.net:443)) (No such host is known. (testtongwu0624.dfs.core.windows.net:443)) (No such host is known. (testtongwu0624.dfs.core.windows.net:443))
       ---> Azure.RequestFailedException: No such host is known. (testtongwu0624.dfs.core.windows.net:443)
       ---> System.Net.Http.HttpRequestException: No such host is known. (testtongwu0624.dfs.core.windows.net:443)
       ---> System.Net.Sockets.SocketException (11001): No such host is known.
```

4. **Azure blob with datalake but hierachical namespace disabled**

```
Health check component AzureBlobStorage:CanReadWrite: check hierachical namespace enabled in storage account failed: Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions.AzureBlobOperationFailedException: Failed to move blob directory '__healthcheck__/2bb26ce9a3fe4d3ca2c6608b486aa771/source' to '__healthcheck__/2bb26ce9a3fe4d3ca2c6608b486aa771/target'.
       ---> Azure.RequestFailedException: The source path for a rename operation does not exist.
RequestId:b23b3f9d-301f-001f-3465-eb3934000000
Time:2022-10-29T07:12:34.8281038Z
      Status: 404 (The source path for a rename operation does not exist.)
      ErrorCode: SourcePathNotFound
      ...
```

5. **Authentication failed**
```
Health check component AzureBlobStorage:CanReadWrite: write content to storage account failed: Microsoft.Health.Fhir.Synapse.DataWriter.Exceptions.AzureBlobOperationFailedException: Create container testtest failed.
       ---> Azure.RequestFailedException: This request is not authorized to perform this operation.
RequestId:e837c6e1-a01e-0003-6766-eb6472000000
Time:2022-10-29T07:17:37.0617338Z
      Status: 403 (This request is not authorized to perform this operation.)
      ErrorCode: AuthorizationFailure
```