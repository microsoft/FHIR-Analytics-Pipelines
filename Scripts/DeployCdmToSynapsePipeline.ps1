
function Connect-ToAz {
    # Connect to Azure account
    Connect-AzAccount

    # Select Azure subscription
    # Set-AzContext [SubscriptionID/SubscriptionName]
}

function ReadJsonFile {
    $content = (Get-Content "config.json") | ConvertFrom-Json
    Write-Host $content
    Write-Host $content.properties
}

function New-DeploySynapsePipeline {

    param (
        $ResourceGroupName,
        $TemplateFilePath,
        $ParamaterFilePath
    )

    Get-AzResourceGroup -Name $ResourceGroupName -ErrorVariable notPresent -ErrorAction SilentlyContinue

    if ($notPresent)
    {
        Write-Host "Creating resource group '$ResourceGroupName'..."
        
        New-AzResourceGroup `
            -Name $ResourceGroupName `
            -Location "Central US"
    }
    
    New-AzResourceGroupDeployment `
        -Name DeployLocalTemplate `
        -ResourceGroupName $ResourceGroupName `
        -TemplateFile $TemplateFilePath `
        -TemplateParameterFile $ParamaterFilePath `
        -verbose
}

Import-Module Az.Storage

$resourceGroupName = "quwan20210128"
$templateFilePath = "../Templates/cdmToSynapse.json"
$paramaterFilePath = "../Templates/cdmToSynapse.parameters.json"

$manifestName = "default.manifest.cdm.json"

Connect-ToAz

Write-Host "Deploying..."

# New-DeploySynapsePipeline -ResourceGroupName $resourceGroupName -TemplateFilePath $templateFilePath -ParamaterFilePath $paramaterFilePath

# Get-AzureStorageBlob -Container "cdm" -Blob $manifestName | Get-AzureStorageBlobContent

# Get-AzureStorageBlobContent -Container "fhirtocdmstorage" -Blob $manifestName
function test-pull {
    $storageAccount = 'fhirtocdmstorage'
    $resourceGroupName = 'quwan20210128'
    $container = 'cdm'
    $manifestName = 'default.manifest.cdm.json'

    $file = "./.$manifestName"

    $context = New-AzStorageContext -StorageAccountName $storageAccount -UseConnectedAccount
    $blob = Get-AzStorageBlobContent -Blob $blob -Container $container -Context $context -Destination $file -Force
}

test-pull


Write-Host "Complete!"