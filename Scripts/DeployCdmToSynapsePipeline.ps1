param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

$configContent = (Get-Content $Config) | ConvertFrom-Json

$Public  = @( Get-ChildItem -Path "$PSScriptRoot\Public\*.ps1" )
@($Public) | ForEach-Object {
    . $_.FullName
}


# Create staging container, will be used in CDM to synapse pipline

New-StagingContainer `
    -StorageAccount $configContent.templateParameters.AdlsAccountForCdm `
    -StagingContainer $configContent.stagingContainer


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
