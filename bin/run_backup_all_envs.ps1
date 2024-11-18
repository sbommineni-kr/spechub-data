# Example usage:
# Run backup for all environments:
# .\run_backup_all_envs.ps1 -StorageConnection ""
#
# Run backup for specific environment:
# .\run_backup_all_envs.ps1 -StorageConnection "" -Environment "dev"

param(
    [Parameter(Mandatory=$true)]
    [string]$StorageConnection,
    [Parameter(Mandatory=$false)]
    [string]$Environment
)

$environments = @("dev", "test", "uat", "prod")
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$scriptName = $MyInvocation.MyCommand.Name

# Check if environment argument is provided
if ($Environment) {
    # Check if provided environment is valid
    if ($environments -contains $Environment) {
        $environments = @($Environment)
    }
    else {
        Write-Host "$timestamp $scriptName : Error: Invalid environment. Allowed values are: $($environments -join ' ')"
        exit 1
    }
}

foreach ($env in $environments) {
    Write-Host "$timestamp $scriptName : Running backup for environment: $env"
    Write-Host "$timestamp $scriptName : Directory: $PSScriptRoot"
    
    $result = python "$PSScriptRoot/../etl/mongodb/load_spechub_backup_adls.py" `
        -c "$PSScriptRoot/../config/datalake.yaml" `
        -j "mongodb" `
        -n "load_spechub_backup_adls" `
        -a "{`"storage_connection`":`"$StorageConnection`"}" `
        -v $env

    if ($LASTEXITCODE -eq 0) {
        Write-Host "$timestamp $scriptName : Successfully completed backup for environment: $env"
    } else {
        Write-Host "$timestamp $scriptName : Failed to complete backup for environment: $env" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}