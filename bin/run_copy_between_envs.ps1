# Example command: .\run_copy_between_envs.ps1 -storage_connection <storage_connection> -source_env test -target_env dev

param(
    [Parameter(Mandatory=$true)]
    [string]$storage_connection,
    
    [Parameter(Mandatory=$true)]
    [string]$source_env,
    
    [Parameter(Mandatory=$true)]
    [string]$target_env
)

# Set common logging prefix
$SCRIPT_NAME = "[$(Split-Path $MyInvocation.MyCommand.Path -Leaf)]"

# Validate environments
$valid_environments = @("dev", "test", "uat", "prod")
if (-not ($valid_environments -contains $source_env) -or -not ($valid_environments -contains $target_env)) {
    Write-Host "$SCRIPT_NAME Error: Invalid environment. Allowed values are: $($valid_environments -join ', ')"
    exit 1
}

# Execute the Python script
Write-Host "$SCRIPT_NAME Copying data from $source_env to $target_env"
$pythonScript = Join-Path (Split-Path $PSScriptRoot) "etl\mongodb\load_copy_between_envs.py"
$configPath = Join-Path (Split-Path $PSScriptRoot) "config\datalake.yaml"

try {
    python $pythonScript `
        -c $configPath `
        -j "mongodb" `
        -n "load_copy_between_envs" `
        -a "{""source_env"":""$source_env"",""target_env"":""$target_env"",""storage_connection"":""$storage_connection""}"
    
    Write-Host "$SCRIPT_NAME Copy operation completed successfully"
} catch {
    Write-Host "$SCRIPT_NAME Error: Copy operation failed"
    exit 1
}
