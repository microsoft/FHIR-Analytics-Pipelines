param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

$configContent = (Get-Content $Config) | ConvertFrom-Json
$storageAccount = $configContent.templateParameters.AdlsAccountForCdm

# Create staging container, will be used in CDM to synapse pipline

$context = New-AzStorageContext -StorageAccountName $storageAccount -UseConnectedAccount
$container = Get-AzStorageContainer -Name $configContent.stagingContainer -context $context -ErrorAction Ignore
if(-not $container) {
    Write-Host "Creating staging container..."
    New-AzStorageContainer -Name $configContent.stagingContainer -Context $context
}
else {
    Write-Host "Staging container exists"
}


# Deploy CDM to synapse pipelines

Write-Host "Deploying..."
$count = 0
foreach ($entity in $configContent.templateParameters.Entities){
    $count++
    Write-Host "Deploy the $entity [$($count)/$($configContent.templateParameters.Entities.count)]"
    
    $templateParameters = @{
        DataFactoryName = $configContent.templateParameters.DataFactoryName; `
        SynapseWorkspace =$configContent.templateParameters.SynapseWorkspace; `
        DedicatedSqlPool = $configContent.templateParameters.DedicatedSqlPool; `
        AdlsAccountForCdm = $configContent.templateParameters.AdlsAccountForCdm; `
        CdmRootLocation = $configContent.templateParameters.CdmRootLocation; `
        CdmLocalEntity = $entity
    }

    New-AzResourceGroupDeployment `
        -Name DeployLocalTemplate `
        -ResourceGroupName $configContent.resourceGroup `
        -TemplateFile $configContent.templateFilePath `
        -TemplateParameterObject $templateParameters `
        -verbose
}
Write-Host "Complete!"
