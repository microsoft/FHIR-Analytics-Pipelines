
<#
.SYNOPSIS
    Create database on Synapse SQL pool, and create default EXTERNAL TABLEs and VIEWs for customers.
.DESCRIPTION
    Create database on Synapse SQL pool, and create default EXTERNAL TABLEs and VIEWs for customers. 
    Customers can query data on created EXTERNAL TABLEs and VIEWs on Synapse. 
.PARAMETER SqlServerEndpoint
    Synapse SQL server endpoint, need to be the serverless SQL server otherwise cannot create EXTERNALE TABLEs and VIEWs on it.
    E.g. "example.sql.azuresynapse.net".
.PARAMETER Database
    Default: fhirdb
    Name of database name to be created on Synapse serverless SQL server pool.
.PARAMETER Storage
    Name of storage where parquet FHIR data be exported to.
.PARAMETER Container
    Default: fhir
    Name of container on storage where parquet FHIR data be exported to.
.PARAMETER ResultPath
    Default: result
    Path to the parquet FHIR data.
.PARAMETER MasterKey
    Default: "FhirSynapseLink0!"
    Master key that will be set in created database. Database need to have master key then we can create EXTERNAL TABLEs and VIEWs on it.
.PARAMETER Concurrent
    Default: 25
    Max concurrent tasks number that will be used to upload place holder files and execute SQL scripts.
#>

[cmdletbinding()]
Param(
    [parameter(Mandatory=$true)]
    [string]$SqlServerEndpoint,
    [string]$Database = "fhirdb",
    [parameter(Mandatory=$true)]
    [string]$Storage,
    [string]$Container = "fhir",
    [string]$ResultPath = "result",
    [string]$MasterKey = "FhirSynapseLink0!",
    [int]$Concurrent = 25
)

$jobName = "FhirSynapseJob"
$readmePath = ".readme.txt"
$sqlScriptCollectionPath = "sql/Resources"

function New-CustomDatabase
{
    param([string]$serviceEndpoint, [string]$databaseName)

    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    try {
        Invoke-Sqlcmd -ServerInstance $serviceEndpoint -Database "master" -AccessToken $sqlAccessToken `
            -Query "CREATE DATABASE $($databaseName)" -ErrorAction Stop
    }
    catch {
        Write-Host "Create database '$databaseName' on '$serviceEndpoint' failed: $($_.ToString())" 
        throw
    }
}

function Remove-CustomDatabase
{
    param([string]$serviceEndpoint, [string]$databaseName)

    [System.Data.SqlClient.SqlConnection]::ClearAllPools() 
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    try {
        Invoke-Sqlcmd -ServerInstance $serviceEndpoint -Database "master" -AccessToken $sqlAccessToken `
            -Query "DROP DATABASE $($databaseName)" -ErrorAction Stop
    }
    catch {
        Write-Host "Remove database '$databaseName' on '$serviceEndpoint' failed: $($_.ToString())" 
        throw
    }
}

function Set-InitializeEnvironment
{
    param([string]$serviceEndpoint, [string]$databaseName, [string]$masterKey, [string]$storage, [string]$container, [string]$resultPath)

    $locationPath = "https://$storage.blob.core.windows.net/$container/$resultPath"

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

    $storageContext = New-AzStorageContext -StorageAccountName $storageName -UseConnectedAccount
    if(Get-AzStorageContainer -Name $containerName -Context $storageContext -ErrorAction SilentlyContinue)  {
        Write-Host " -> Container '$containerName' already exists." -ForegroundColor Green 
    }
    else{
        Write-Host " -> Create container '$containerName'." -ForegroundColor Green 
        New-AzStorageContainer -Name $containerName -Context $storageContext -ErrorAction Stop
    }
}

function New-PlaceHolderBlobs
{
    param([string]$storage, [string]$container, [string]$resultPath)
    $files = Get-ChildItem $sqlScriptCollectionPath -Filter "*.sql" -Name
    foreach ($f in $files) {
        # Upload placeholder files.
        $resourceType = $($f -split "\.")[0]
        $blobName = "$resultPath/$resourceType/.readme.txt"
        
        $jobs = @(Get-Job -Name $jobName -ErrorAction Ignore)
        if ($jobs.Count -ge $Concurrent) {
            $finishedJob = (Get-Job -Name $jobName | Wait-Job -Any)
            Remove-Job -Job $finishedJob

            if ($finishedJob.State -eq 'Failed') {
                Write-Host " -> $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
                Get-Job -Name $jobName | Wait-Job | Remove-Job | Out-Null
                throw " -> Uploading readme files to storage failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
            }
        }

        # Create TABLES and VIEWs for resouces
        Write-Host " -> Upload blob '$blobName'."

        Start-Job -Name $jobName -ScriptBlock{
            $storageContext = New-AzStorageContext -StorageAccountName $args[3] -UseConnectedAccount
            Set-AzStorageBlobContent `
                -File $args[0]`
                -Container $args[1] `
                -Blob $args[2] `
                -Context $storageContext `
                -Force `
                -ErrorAction stop
        } -ArgumentList "$(Get-Location)/$readmePath", $container, $blobName, $storage | Out-Null
    }

    foreach ($finishedJob in (Get-Job -Name $jobName | Wait-Job)) {
        Remove-Job -Job $finishedJob

        if ($finishedJob.State -eq 'Failed') {
            Write-Host " -> $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
            Get-Job -Name $jobName | Wait-Job | Remove-Job | Out-Null
            throw " -> Uploading readme files to storage failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
        }
    }
}

function New-TableAndViewsForResources
{
    param([string]$serviceEndpoint, [string]$databaseName, [string]$masterKey, [string]$storage, [string]$container)

    $files = Get-ChildItem $sqlScriptCollectionPath -Filter "*.sql" -Name
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token

    Write-Host " -> Start creating TABLEs and VIEWs on '$databaseName' of '$serviceEndpoint'" -ForegroundColor Green 
    
    foreach ($file in $files) {
        $jobs = @(Get-Job -Name $jobName -ErrorAction Ignore)
        if ($jobs.Count -ge $Concurrent) {
            $finishedJob = (Get-Job -Name $jobName | Wait-Job -Any)
            Remove-Job -Job $finishedJob

            if ($finishedJob.State -eq 'Failed') {
                Write-Host " -> $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
                Get-Job -Name $jobName | Wait-Job | Remove-Job | Out-Null
                throw "Creating Table and Views job failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
            }
        }
        $filePath = "$sqlScriptCollectionPath/$file"

        # Create TABLES and VIEWs for resouces
        Write-Host " -> Executing script $filePath"
        Start-Job -Name $jobName -ScriptBlock{
            Invoke-Sqlcmd `
                -ServerInstance $args[0] `
                -Database $args[1] `
                -AccessToken $args[2] `
                -InputFile $args[3] `
                -ConnectionTimeout 120 `
                -ErrorAction Stop
        } -ArgumentList $serviceEndpoint, $databaseName, $sqlAccessToken, "$(Get-Location)/$filePath" | Out-Null
    }

    foreach ($finishedJob in (Get-Job -Name $jobName | Wait-Job)) {
        Remove-Job -Job $finishedJob

        if ($finishedJob.State -eq 'Failed') {
            Write-Host " -> $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)" -ForegroundColor Red
            Get-Job -Name $jobName | Wait-Job | Remove-Job | Out-Null
            throw "Creating Table and Views job failed: $($finishedJob.ChildJobs[0].JobStateInfo.Reason.Message)"
        }
    }
}

$stopwatch =  [system.diagnostics.stopwatch]::StartNew()

Get-Job -Name $jobName -ErrorAction Ignore | Remove-Job | Out-Null


# Test connection to Synapse SQL server.
try {
    $sqlAccessToken = (Get-AzAccessToken -ResourceUrl https://database.windows.net).Token
    $dbId = Invoke-Sqlcmd -ServerInstance $SqlServerEndpoint  -Database "master" -AccessToken $sqlAccessToken `
        -Query "SELECT DB_ID('$Database')" -ErrorAction Stop
}
catch {
    Write-Host "Failed to connect to '$SqlServerEndpoint': $($_.ToString())."
    throw
}
# Throw exception if database already exists.
if ([string]$dbId.Column1)
{
    throw "Database '$Database' already exist, please use another database name or drop it."
}

try {
    ###
    # 1.Create container on Storage if not exists.
    ###
    New-ContainerIfNotExists `
        -storageName $Storage `
        -containerName $Container
}
catch {
    Write-Host "Create container '$Container' on '$Storage' failed: $($_.ToString())."
    throw
}

try{
    ###
    # 2. Place holder blobs
    ###
    New-PlaceHolderBlobs -storage $Storage -container $Container -resultPath $ResultPath
}
catch
{
    Write-Host "Create place holder blobs failed: $($_.ToString())."
    throw
}

###
# 1. Create database.
###
New-CustomDatabase `
    -databaseName $Database `
    -serviceEndpoint $SqlServerEndpoint 
    
# Try to create TABLEs and VIEWs for all resource types.
# And will try to drop database if failed to create TABLEs and VIEWs.
try{
    ###
    # 2. Initialize database environment.
    ###
    Set-InitializeEnvironment `
        -serviceEndpoint $SqlServerEndpoint `
        -databaseName $Database `
        -masterKey $MasterKey `
        -storage $Storage `
        -container $Container `
        -resultPath $ResultPath

    ###
    # 3. Create TABLEs and VIEWs on Synapse.
    ###
    New-TableAndViewsForResources `
        -serviceEndpoint $SqlServerEndpoint `
        -databaseName $Database `
        -masterKey $MasterKey `
        -storage $Storage `
        -container $Container

}
catch{
    Write-Host " -> Create TABLEs and VIEWs Failed, will try to drop database '$Database'." -ForegroundColor Red
    Remove-CustomDatabase -serviceEndpoint $SqlServerEndpoint -databaseName $Database
    throw
}

$stopwatch.Stop()
$time = $stopwatch.Elapsed.TotalSeconds

Write-Host " -> Create Synapse environment finished with $time seconds." -ForegroundColor Green 

[System.Data.SqlClient.SqlConnection]::ClearAllPools() 
