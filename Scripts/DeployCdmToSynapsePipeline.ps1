param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

$configContent = (Get-Content $Config) | ConvertFrom-Json

# Create staging container, will be used in CDM to synapse pipline

$storageAccount = $configContent.TemplateParameters.AdlsAccountForCdm
$stagingContainer = $configContent.TemplateParameters.StagingContainer

$context = New-AzStorageContext -StorageAccountName $storageAccount -UseConnectedAccount
$container = Get-AzStorageContainer -Name $stagingContainer -context $context -ErrorAction Ignore
if(-not $container) {
    Write-Host "Creating staging container..."
    New-AzStorageContainer -Name $stagingContainer -Context $context
}
else {
    Write-Host "Staging container exists"
}


# Deploy CDM to synapse pipelines

Write-Host "Deploying..."
$count = 0
foreach ($entity in $configContent.TemplateParameters.Entities){
    $count++
    Write-Host "Deploy the $entity [$($count)/$($configContent.TemplateParameters.Entities.count)]"
    
    $templateParameters = @{
        DataFactoryName = $configContent.TemplateParameters.DataFactoryName; `
        SynapseWorkspace =$configContent.TemplateParameters.SynapseWorkspace; `
        DedicatedSqlPool = $configContent.TemplateParameters.DedicatedSqlPool; `
        AdlsAccountForCdm = $configContent.TemplateParameters.AdlsAccountForCdm; `
        CdmRootLocation = $configContent.TemplateParameters.CdmRootLocation; `
        CdmLocalEntity = $entity
    }

    New-AzResourceGroupDeployment `
        -Name DeployLocalTemplate `
        -ResourceGroupName $configContent.ResourceGroup `
        -TemplateFile $configContent.TemplateFilePath `
        -TemplateParameterObject $templateParameters `
        -verbose
}
Write-Host "Complete!"
