 param (
    [switch]$schema = $false
 )

$activity = Get-Content 'activity.json' | Out-String | ConvertFrom-Json

$clientId = $activity.typeProperties.extendedProperties.clientId
$tenantId = $activity.typeProperties.extendedProperties.tenantId
$adlsAccount = $activity.typeProperties.extendedProperties.adlsAccount
$adlsFileSystem = $activity.typeProperties.extendedProperties.adlsFileSystem
$blobUri = $activity.typeProperties.extendedProperties.blobUrl
$clientSecret = $activity.typeProperties.extendedProperties.clientSecret
$configurationContainer = $activity.typeProperties.extendedProperties.configurationContainer
$operationId = $activity.typeProperties.extendedProperties.operationId
$maxDepth = $activity.typeProperties.extendedProperties.maxDepth

if (-not $schema) {
    .\Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor.exe transform-data --clientId $clientId --tenantId $tenantId --adlsAccount $adlsAccount --cdmFileSystem $adlsFileSystem --inputBlobUri "`"$blobUri`"" --configurationContainer $configurationContainer --clientSecret $clientSecret --operationId $operationId --maxDepth $maxDepth
}
else {
    .\Microsoft.Health.Fhir.Transformation.Cdm.BatchExecutor.exe generate-schema --clientId $clientId --tenantId $tenantId --adlsAccount $adlsAccount --cdmFileSystem $adlsFileSystem --configurationContainer $configurationContainer --clientSecret $clientSecret --maxDepth $maxDepth
}