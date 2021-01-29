function New-StagingContainer {
    param (
        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [String]$StorageAccount,

        [Parameter(Mandatory = $true)]
        [ValidateNotNullOrEmpty()]
        [String]$StagingContainer
    )
    
    Import-Module Az.Storage

    $context = New-AzStorageContext -StorageAccountName $StorageAccount -UseConnectedAccount
    $null = New-AzStorageContainer `
        -Name $StagingContainer `
        -Context $context `
}

