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

# Check if environment argument is provided
if ($Environment) {
    # Check if provided environment is valid
    if ($environments -contains $Environment) {
        $environments = @($Environment)
    }
    else {
        Write-Host "Error: Invalid environment. Allowed values are: $($environments -join ' ')"
        exit 1
    }
}

foreach ($env in $environments) {
    Write-Host "Running backup for environment: $env"
    Write-Host "Directory: $PSScriptRoot"
    
    python "$PSScriptRoot/../etl/mongodb/load_spechub_backup_adls.py" `
        -c "$PSScriptRoot/../config/datalake.yaml" `
        -j "mongodb" `
        -n "load_spechub_backup_adls" `
        -a "{`"storage_connection`":`"$StorageConnection`"}" `
        -v $env
        
    Write-Host "Completed backup for environment: $env"
}