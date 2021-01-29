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
    $container = Get-AzStorageContainer -Name $StagingContainer -context $context -ErrorAction Ignore
    if(-not $container) {
        Write-Host "Creating staging container..."
        New-AzStorageContainer -Name $StagingContainer -Context $context
    }
    else {
        Write-Host "Staging container exists"
    }
}

