param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

$configContent = (Get-Content $Config) | ConvertFrom-Json

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
        StagingContainer = $configContent.TemplateParameters.StagingContainer; `
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
