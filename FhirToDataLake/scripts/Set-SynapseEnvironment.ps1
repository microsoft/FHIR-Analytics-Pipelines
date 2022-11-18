
<#
.SYNOPSIS
    Create database on Synapse SQL pool, and create default EXTERNAL TABLEs and VIEWs for customers.
.DESCRIPTION
    Create database on Synapse SQL pool, and create default EXTERNAL TABLEs and VIEWs for customers. 
    Customers can query data on created EXTERNAL TABLEs and VIEWs on Synapse. 
.PARAMETER SynapseWorkspaceName
    Name of Synapse workspace instance, will create tag on it and create EXTERNAL TABLEs and VIEWs in its serverless SQL pool.
.PARAMETER Database
    Default: fhirdb
    Name of database to be created on Synapse serverless SQL server pool.
.PARAMETER StorageName
    Name of storage where parquet FHIR data be exported to.
.PARAMETER Container
    Default: fhir
    Name of container on storage where parquet FHIR data be exported to.
.PARAMETER ResultPath
    Default: result
    Path to the parquet FHIR data.
.PARAMETER FhirVersion
    Default: R4
    The fhir version of Parquet data on the storage.
.PARAMETER SqlScriptCollectionPath
    Default: sql/Resources
    Path to the sql scripts directory to create EXTERNAL TABLEs and VIEWs.
.PARAMETER MasterKey
    Default: "FhirSynapseLink0!"
    Master key that will be set in created database. Database need to have master key then we can create EXTERNAL TABLEs and VIEWs on it.
.PARAMETER Concurrent
    Default: 15
    Max concurrent tasks number that will be used to upload placeholder files and execute SQL scripts.
.PARAMETER CustomizedSchemaImage
    Customized schema image reference.
#>

[cmdletbinding()]
Param(
    [parameter(Mandatory=$true)]
    [string]$SynapseWorkspaceName,
    [string]$Database = "fhirdb",
    [parameter(Mandatory=$true)]
    [string]$StorageName,
    [string]$Container = "fhir",
    [string]$ResultPath = "result",
    [string]$FhirVersion = "R4",
    [string]$SqlScriptCollectionPath = "sql",
    [string]$MasterKey = "FhirSynapseLink0!",
    [int]$Concurrent = 15,
    [string]$CustomizedSchemaImage
)

# TODO: Align Tags here and ARM template, maybe save schemas in Storage/ACR and run remotely.
$Tags = @{
    "FhirAnalyticsPipeline" = "FhirToDataLake"
    "FhirSchemaVersion" = "v0.4.0"
}

$JobName = "FhirSynapseJob"
$PlaceHolderName = ".readme.txt"
$CustomizedTemplateDirectory = "CustomizedSchema"

$OrasDirectoryPath = "Oras"
$OrasAppPath = "oras.exe"
$OrasWinUrl = "https://github.com/deislabs/oras/releases/download/v0.12.0/oras_0.12.0_windows_amd64.tar.gz"
$ErrorActionPreference = "Stop"

$FhirVersion = $FhirVersion.ToUpper()
if ($FhirVersion -eq "R4")
{
    $SqlScriptCollectionPath = Join-Path $SqlScriptCollectionPath "r4" | Join-Path -ChildPath "Resources"
}
elseif ($FhirVersion -eq "R5")
{
    $SqlScriptCollectionPath = Join-Path $SqlScriptCollectionPath "r5" | Join-Path -ChildPath "Resources"
}
else
{
    throw "The FHIR version '$FhirVersion' is not supported."
}

function Start-Retryable-Job {
    [CmdletBinding()]
    Param(
        [string]$Name, [scriptblock]$ScriptBlock, [Object[]]$ArgumentList 
    )

    $prefixBlock = '
        $retryCount = 0
        $retryMax = 2
        $delay = 300
            
        do {
            try 
            {
    '
    $suffixBlock = '
                return
            }
            catch [Exception]
            {
                $retryCount++
                if ($retryCount -le $retryMax)
                {
                    Start-Sleep -Milliseconds $delay
                }
                else 
                {
                    throw
                }
            }
        } while ($true)
    '
    $executeBlock = [ScriptBlock]::Create($prefixBlock + $ScriptBlock.ToString() + $suffixBlock)

    Start-Job -Name $Name -ScriptBlock $executeBlock -ArgumentList $ArgumentList | Out-Null
}

function Execute_File
{
    param([string]$fileName, [string[]]$argumentList)
    & $fileName $argumentList

    if ($LastExitCode -ne 0) {
        throw "Failed for executing '$fileName $argumentList'."
    }

    Write-Host "Finish executing '$fileName $argumentList'." -ForegroundColor Green
}

function New-FhirDatabase
{
    param([string]$serviceEndpoint, [string]$databaseName)

    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    try {
        Invoke-Sqlcmd -ServerInstance $serviceEndpoint -Database "master" -AccessToken $sqlAccessToken `
            -Query "CREATE DATABASE $databaseName" -ErrorAction Stop
    }
    catch {
        Write-Host "Create database '$databaseName' on '$serviceEndpoint' failed: $($_.ToString())" 
        throw
    }
}

function Remove-FhirDatabase
{
    param([string]$serviceEndpoint, [string]$databaseName)

    [System.Data.SqlClient.SqlConnection]::ClearAllPools() 
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    try {
        Invoke-Sqlcmd -ServerInstance $serviceEndpoint -Database "master" -AccessToken $sqlAccessToken `
            -Query "DROP DATABASE $databaseName" -ErrorAction Stop
    }
    catch {
        Write-Host "Remove database '$databaseName' on '$serviceEndpoint' failed: $($_.ToString())" 
        throw
    }
}

function Set-InitializeEnvironment
{
    param([string]$serviceEndpoint, [string]$databaseName, [string]$masterKey, [string]$storageName, [string]$container, [string]$resultPath)

    $locationPath = "https://$storageName.blob.core.windows.net/$container/$resultPath"

    $initializeEnvironmentSql = "
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$masterKey';
    
    CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
    WITH IDENTITY = 'Managed Identity';
    
    
    CREATE EXTERNAL DATA SOURCE ParquetSource WITH (
        LOCATION = '$locationPath',
        CREDENTIAL = SynapseIdentity
    );
    
    CREATE EXTERNAL FILE FORMAT ParquetFormat WITH (  FORMAT_TYPE = PARQUET );
    
    GO
    CREATE SCHEMA fhir;
    
    GO
    USE [master]"

    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

    try {
        Invoke-Sqlcmd -ServerInstance $serviceEndpoint -Database $databaseName -AccessToken $sqlAccessToken `
            -Query $initializeEnvironmentSql -ErrorAction Stop
    }
    catch {
        Write-Host "Initialize environment on '$databaseName' of '$serviceEndpoint' failed: $($_.ToString())" 
        throw
    }
}

function New-ContainerIfNotExists
{
    param([string]$storageName, [string]$containerName)

    $storageContext = New-AzStorageContext -StorageAccountName $storageName -UseConnectedAccount -ErrorAction stop
    if(Get-AzStorageContainer -Name $containerName -Context $storageContext -ErrorAction SilentlyContinue)  {
        Write-Host "Container '$containerName' already exists." -ForegroundColor Green 
    }
    else{
        Write-Host "Create container '$containerName'." -ForegroundColor Green 
        New-AzStorageContainer -Name $containerName -Context $storageContext -ErrorAction Stop
    }
}

function New-PlaceHolderBlobs
{
    param([string]$storageName, [string]$container, [string]$resultPath, [string[]]$schemaTypes)

    $storageContext = New-AzStorageContext -StorageAccountName $storageName -UseConnectedAccount -ErrorAction stop
    $expiryTime = (Get-Date).AddMinutes(30)
    $sasToken = New-AzStorageContainerSASToken -Name $container -Permission rwd -ExpiryTime $expiryTime -context $storageContext -ErrorAction stop

    foreach ($schemaType in $schemaTypes) {
        $blobName = "$resultPath/$schemaType/.readme.txt"
        
        $jobs = @(Get-Job -Name $JobName -ErrorAction Ignore)
        if ($jobs.Count -ge $Concurrent) {
            $finishedJob = (Get-Job -Name $JobName | Wait-Job -Any)

            if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
                Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
                Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
                throw "Creating placeholder blob failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
            }
            else {
                Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
                Remove-Job -Job $finishedJob
            }
        }

        # Create placeholder blobs
        Write-Host " -> Begin to create placeholder blob '$blobName'."

        Start-Retryable-Job -Name $JobName -ScriptBlock{
            $storageContext = New-AzStorageContext -StorageAccountName $args[3] -SasToken $args[4] -ErrorAction stop
            Set-AzStorageBlobContent `
                -File $args[0]`
                -Container $args[1] `
                -Blob $args[2] `
                -Context $storageContext `
                -Force `
                -ErrorAction stop
            Write-Host " -> Finished creating placeholder blob '$($args[2])'."
        } -ArgumentList "$(Get-Location)/$PlaceHolderName", $container, $blobName, $storageName, $sasToken
    }

    foreach ($finishedJob in (Get-Job -Name $JobName | Wait-Job)) {
        if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
            Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
            Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
            throw "Creating placeholder blob failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
        }
        else {
            Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
            Remove-Job -Job $finishedJob
        }
    }
}

function New-TableAndViewsForResources
{
    param([string]$serviceEndpoint, [string]$databaseName, [string]$masterKey, [string]$storageName, [string]$container)

    $files = Get-ChildItem $SqlScriptCollectionPath -Filter "*.sql"
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

    Write-Host "Start to create default TABLEs and VIEWs on '$databaseName' of '$serviceEndpoint'" -ForegroundColor Green 
    
    foreach ($file in $files) {
        $jobs = @(Get-Job -Name $JobName -ErrorAction Ignore)
        if ($jobs.Count -ge $Concurrent) {
            $finishedJob = (Get-Job -Name $JobName | Wait-Job -Any)

            if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
                Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
                Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
                throw "Creating Table and Views job failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
            }
            else {
                Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
                Remove-Job -Job $finishedJob
            }
        }
        $filePath = $file.FullName

        # Create TABLES and VIEWs for resouces
        Write-Host " -> Begin to execute script '$filePath'"
        Start-Job -Name $JobName -ScriptBlock{
            Invoke-Sqlcmd `
                -ServerInstance $args[0] `
                -Database $args[1] `
                -AccessToken $args[2] `
                -InputFile $args[3] `
                -ConnectionTimeout 120 `
                -ErrorAction Stop
            Write-Host " -> Finished executing script '$($args[3])'"
        } -ArgumentList $serviceEndpoint, $databaseName, $sqlAccessToken, $filePath | Out-Null
    }

    foreach ($finishedJob in (Get-Job -Name $JobName | Wait-Job)) {
        if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
            Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
            Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
            throw "Creating Table and Views job failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
        }
        else {
            Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
            Remove-Job -Job $finishedJob
        }
    }
}

function Get-OrasExeApp {
    param ([string]$orasDirectoryPath, [string]$orasAppPath, [string]$orasUrl)

    $orasGzFile = "oras.tar.gz"
    # Check if oras.exe have already been downloaded
    if ((test-Path -Path $orasAppPath))
    {
        Write-Host "Oras application have already exist" -ForegroundColor Green 
        return
    }

    Write-Host "Start to download oras application from $orasUrl" -ForegroundColor Green 
    try {
        Invoke-WebRequest $orasUrl -OutFile $orasGzFile -ErrorAction Stop
    }
    catch {
        Write-Host "Download oras application from $orasUrl failed: $($_.ToString())" -BackgroundColor Red
        throw
    }

    # Create Oras directory for unpacking Oras artifact
    if (!(test-path -path $orasDirectoryPath))
    {
        new-item -path $orasDirectoryPath -Itemtype Directory | Out-Null
    }

    $unpackParameters = @(
        '-xvf'
        $orasGzFile
        '-C'
        $orasDirectoryPath
    )

    Execute_File -fileName 'tar' -argumentList $unpackParameters

    Copy-Item "$orasDirectoryPath/$orasAppPath" -Destination "."
    Remove-Item $orasGzFile
    Write-Host "Finish download oras application from $orasUrl" -ForegroundColor Green 
}

function Get-CustomizedSchemaImage {
    param ([string]$orasAppPath, [string]$schemaImageReference, [string]$customizedTemplateDirectory)
    
    $registryName = $schemaImageReference.Substring(0, $schemaImageReference.IndexOf('.azurecr.io'))
    Connect-AzContainerRegistry -Name $registryName -ErrorAction stop

    # Leverage the oras to pull the image from Container Registry.
    $orasParameters = @(
        'pull'
        $schemaImageReference
    )
    
    Execute_File -fileName "./$orasAppPath" -argumentList $orasParameters

    $compressPackages = Get-ChildItem -Path * -Include '*.tar.gz' -Name
    Write-Host "Successfully pull the customized schema image: $compressPackages" -ForegroundColor Green

    # Unpack the image compressed package to customized template directory    
    if (!(Test-Path $customizedTemplateDirectory)){
        New-Item $customizedTemplateDirectory -ItemType Directory
    }

    foreach ($compressPackage in $compressPackages){
        $unpackParameters = @(
            '-xvf'
            $compressPackage
            '-C'
            $customizedTemplateDirectory
        )

        Execute_File -fileName 'tar' -argumentList $unpackParameters
    }
}

function Get-CustomizedSchemaType {
    param ([string]$resourceType)

    return "$($resourceType)_Customized"
}

function Get-CustomizedTableSql {
    param ([string]$schemaType, [PSCustomObject]$schemaObject)

    $customizedTableProperties = ""
    foreach ($property in $schemaObject.properties.psobject.properties){
        $sqlType = switch($property.Value.type) 
        {
            'number' { 'float' ; Break }
            'integer' { 'bigint' ; Break }
            'boolean' { 'bit' ; Break }
            'string' { 'NVARCHAR(4000)' ; Break }
            Default {
                Write-Host "Invalid property type in '$schemaType.$($property.Name)': $($property.Value.type)"
                throw
            }
        }

        $customizedTableProperties += "    [$($property.Name)] $sqlType,"
    }

    $createCustomizedTableSql = "CREATE EXTERNAL TABLE [fhir].[$schemaType] (
        $customizedTableProperties
        ) WITH (
            LOCATION='/$schemaType/**',
            DATA_SOURCE = ParquetSource,
            FILE_FORMAT = ParquetFormat
        );"
        
    return $createCustomizedTableSql
}

function New-CustomizedTables
{
    param([string]$serviceEndpoint, [string]$databaseName, [string]$masterKey, [string]$customizedSchemaDirectory)

    $schemaFiles = Get-ChildItem -Path $(Join-Path -Path $customizedSchemaDirectory -ChildPath *) -Include '*.schema.json' -Name
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

    Write-Host "Start to create customized Tables on '$databaseName' of '$serviceEndpoint'" -ForegroundColor Green 
    
    foreach ($schemaFile in $schemaFiles){
        $jobs = @(Get-Job -Name $JobName -ErrorAction Ignore)
        if ($jobs.Count -ge $Concurrent) {
            $finishedJob = (Get-Job -Name $JobName | Wait-Job -Any)

            if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
                Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
                Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
                throw "Create customized Table failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
            }
            else {
                Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
                Remove-Job -Job $finishedJob
            }
        }

        $schemaFilePath = Join-Path -Path $customizedSchemaDirectory -ChildPath $schemaFile
        $schemaObject = Get-Content $schemaFilePath | Out-String | ConvertFrom-Json -ErrorAction stop
        $resourceType = $schemaFile.Substring(0, $schemaFile.IndexOf('.schema.json'))
        $schemaType = Get-CustomizedSchemaType -resourceType $resourceType
    
        $sql = Get-CustomizedTableSql -schemaType $schemaType -schemaObject $schemaObject -ErrorAction stop

        Write-Host "Begin to create customized table for $schemaType" -ForegroundColor Green 
        Start-Job -Name $JobName -ScriptBlock{
            Invoke-Sqlcmd `
                -ServerInstance $args[0] `
                -Database $args[1] `
                -AccessToken $args[2] `
                -Query $args[3] `
                -ConnectionTimeout 120 `
                -ErrorAction Stop
            Write-Host "Finished creating customized table for $($args[4])"
        } -ArgumentList $serviceEndpoint, $databaseName, $sqlAccessToken, $sql, $schemaType  | Out-Null
    }

    foreach ($finishedJob in (Get-Job -Name $JobName | Wait-Job)) {
        if (($finishedJob.State -eq 'Failed') -or ($finishedJob.ChildJobs[0].State -eq 'Failed')) {
            Write-Host "$($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
            Get-Job -Name $JobName | Wait-Job | Remove-Job | Out-Null
            throw "Create customized Table failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
        }
        else {
            Write-Host $finishedJob.ChildJobs[0].Information[0].MessageData.Message
            Remove-Job -Job $finishedJob
        }
    }
}

$stopwatch =  [system.diagnostics.stopwatch]::StartNew()

Get-Job -Name $JobName -ErrorAction Ignore | Remove-Job | Out-Null

###
# Try get Synapse instance.
###
try {
    $synapse = Get-AzSynapseWorkspace -Name $SynapseWorkspaceName -ErrorAction Stop
}
catch {
    Write-Host "Get Synapse instance '$synapseWorkspaceName' failed: $($_.ToString())."
    throw
}


# Get synapse serverless SQL server endpoint
$synapseSqlServerEndpoint = $synapse.ConnectivityEndpoints.sqlOnDemand

# Test connection to Synapse SQL server.
try {
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    $dbId = Invoke-Sqlcmd -ServerInstance $synapseSqlServerEndpoint -Database "master" -AccessToken $sqlAccessToken `
        -Query "SELECT DB_ID('$Database')" -ErrorAction Stop
}
catch {
    Write-Host "Failed to connect to '$synapseSqlServerEndpoint': $($_.ToString())."
    throw
}
# Throw exception if database already exists.
if ([string]$dbId.Column1)
{
    throw "Database '$Database' already exist, please use another database name or drop it."
}

# Create tag on Synapse
Update-AzTag -ResourceId $synapse.Id -Tag $Tags -Operation "Merge" | Out-Null 
Write-Host "Created Tags on Synapse '$synapseWorkspaceName'." -ForegroundColor Green 

###
# 1.Create container on Storage if not exists.
###
try {
    New-ContainerIfNotExists `
        -storageName $StorageName `
        -containerName $Container
}
catch {
    Write-Host "Create container '$Container' on '$StorageName' failed: $($_.ToString())."
    throw
}

###
# 2.Create Tables and Views for default schema data.
###
# a). Create placeholder blobs for default schema data.
try{
    $sqlFiles = Get-ChildItem $SqlScriptCollectionPath -Filter "*.sql" -Name 
    $defaultSchemaTypes = $sqlFiles | ForEach-Object {$($_ -split "\.")[0]}
    New-PlaceHolderBlobs -storage $StorageName -container $Container -resultPath $ResultPath -schemaTypes $defaultSchemaTypes
}
catch
{
    Write-Host "Create placeholder blobs for default schema data failed: $($_.ToString())."
    throw
}

# b). Create database on SQL pool.
New-FhirDatabase `
    -databaseName $Database `
    -serviceEndpoint $synapseSqlServerEndpoint

# Try to create TABLEs and VIEWs for all resource types.
# And will try to drop database if failed to create TABLEs and VIEWs.
try{
    # c). Initialize database environment.
    Set-InitializeEnvironment `
        -serviceEndpoint $synapseSqlServerEndpoint `
        -databaseName $Database `
        -masterKey $MasterKey `
        -storage $StorageName `
        -container $Container `
        -resultPath $ResultPath

    # d). Create TABLEs and VIEWs on Synapse.
    New-TableAndViewsForResources `
        -serviceEndpoint $synapseSqlServerEndpoint `
        -databaseName $Database `
        -masterKey $MasterKey `
        -storage $StorageName `
        -container $Container
}
catch{
    Write-Host "Create TABLEs and VIEWs for default schema data failed, will try to drop database '$Database'." -ForegroundColor Red
    Remove-FhirDatabase -serviceEndpoint $synapseSqlServerEndpoint -databaseName $Database
    throw
}

###
# 3. Create Tables for customized schema data.
###
if ($CustomizedSchemaImage) {
    Write-Host "Create Tables for customized schema data, use schema image from $($CustomizedSchemaImage)." -ForegroundColor Green 
    try {
        # a). Download oras application if it not exist.
        Get-OrasExeApp `
            -orasDirectoryPath $OrasDirectoryPath `
            -orasAppPath $OrasAppPath `
            -orasUrl $OrasWinUrl

        # b). Pull and parse customized schema from Container Registry.
        Get-CustomizedSchemaImage `
            -orasAppPath $OrasAppPath `
            -schemaImageReference $CustomizedSchemaImage `
            -customizedTemplateDirectory $CustomizedTemplateDirectory

        $customizedSchemaDirectory = Join-Path -Path $CustomizedTemplateDirectory -ChildPath "Schema"
        
        # c). Create placeholder blobs for customized schema data.
        $sqlFiles = Get-ChildItem $customizedSchemaDirectory -Filter "*.schema.json" -Name 
        $customizedSchemaTypes = $sqlFiles | ForEach-Object { Get-CustomizedSchemaType -resourceType $($_ -split "\.")[0] }
        New-PlaceHolderBlobs -storage $StorageName -container $Container -resultPath $ResultPath -schemaTypes $customizedSchemaTypes

        # d). Create customized TABLEs Synapse.
        New-CustomizedTables `
            -serviceEndpoint $synapseSqlServerEndpoint `
            -databaseName $Database `
            -masterKey $MasterKey `
            -customizedSchemaDirectory $customizedSchemaDirectory
    }
    catch{
        Write-Host "Create TABLEs for customized schema data failed, will try to drop database '$Database'." -ForegroundColor Red
        Remove-FhirDatabase -serviceEndpoint $synapseSqlServerEndpoint -databaseName $Database
        throw
    }
}

$stopwatch.Stop()
$time = $stopwatch.Elapsed.TotalSeconds

Write-Host "Create Synapse environment finished with $time seconds." -ForegroundColor Green 

[System.Data.SqlClient.SqlConnection]::ClearAllPools() 
