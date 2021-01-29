function Get-ManifestFromStorage {
    param (
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [String]$StorageAccount,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [String]$Container,

        [Parameter(Mandatory = $false)]
        [String]$Blob = "default.manifest.cdm.json",

        [Parameter(Mandatory = $false)]
        [String]$Destination = "manfest.cdm.json"
    )
    
    Import-Module Az.Storage

    $context = New-AzStorageContext -StorageAccountName $StorageAccount -UseConnectedAccount
    $null = Get-AzStorageBlobContent -Blob $Blob -Container $Container -Context $context -Destination $Destination -Force
    $content = (Get-Content $Destination) | ConvertFrom-Json
    return $content
}

