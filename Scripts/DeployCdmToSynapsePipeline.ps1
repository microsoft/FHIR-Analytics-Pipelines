param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Config,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ConfigM,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ConfigL
)

$configContentM = (Get-Content $ConfigM) | ConvertFrom-Json
$configContent = (Get-Content $Config) | ConvertFrom-Json
$configContentL = (Get-Content $ConfigL) | ConvertFrom-Json
$groupCount = 40
$j = $groupCount

# Deploy CDM to synapse pipelines

Write-Host "Deploying Master..."
   
$templateParametersM = @{
    DataFactoryName = $configContentM.TemplateParameters.DataFactoryName; `
    MasterPipelineName = $configContentM.TemplateParameters.MasterPipelineName;
}

New-AzResourceGroupDeployment `
    -Name DeployLocalTemplateM `
    -ResourceGroupName $configContentM.ResourceGroup `
    -TemplateFile $configContentM.TemplateFilePath `
    -TemplateParameterObject $templateParametersM `
    -verbose

Write-Host "Deploying Children..."

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
        MasterPipelineName = $configContent.TemplateParameters.MasterPipelineName; `
        CdmLocalEntity = $entity; 
    }

    New-AzResourceGroupDeployment `
        -Name DeployLocalTemplate `
        -ResourceGroupName $configContent.ResourceGroup `
        -TemplateFile $configContent.TemplateFilePath `
        -TemplateParameterObject $templateParameters `
        -verbose
}

Write-Host "Deploying Loop..."

for($i = 0; $i -le ($configContentL.TemplateParameters.Entities).length; ($i = $i + $groupCount), ($j = $j + $groupCount)){
    
    $templateParameters = @{
        DataFactoryName = $configContentL.TemplateParameters.DataFactoryName; `
        MasterPipelineName = $configContentL.TemplateParameters.MasterPipelineName; `
        CdmLocalEntities = $configContentL.TemplateParameters.Entities[$i..$j]; 
    }

    New-AzResourceGroupDeployment `
        -Name DeployLocalTemplateL `
        -ResourceGroupName $configContentL.ResourceGroup `
        -TemplateFile $configContentL.TemplateFilePath `
        -TemplateParameterObject $templateParameters `
        -verbose
}

Write-Host "Complete!"
