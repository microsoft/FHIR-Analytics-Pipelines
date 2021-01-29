param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config
)

<#
    $Public  = @( Get-ChildItem -Path "$PSScriptRoot\Public\*.ps1" )
    @($Public) | ForEach-Object {
        . $_.FullName
    }

    $configContent = (Get-Content $Config) | ConvertFrom-Json
#>

Write-Host "Deploying..."

foreach ($entity in $configContent.templateParameters.Entities){
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
