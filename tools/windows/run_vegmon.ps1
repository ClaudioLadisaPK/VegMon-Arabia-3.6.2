# Lancia VegMon V03 sulla VM Windows con il profilo vm.
# Esempi:
#   powershell -ExecutionPolicy Bypass -File tools\windows\run_vegmon.ps1 -Region R12 -Month 2026-07 -OutputsDir outputs_test\R12_vm
#   powershell -ExecutionPolicy Bypass -File tools\windows\run_vegmon.ps1 -AllPending        (sequenza operativa su outputs\)
param(
    [string]$Region = "",
    [string]$Month = "",
    [string]$OutputsDir = "",
    [switch]$AllPending,
    [string]$FromMonth = "2026-03",
    [string]$ExtraArgs = ""
)
$ErrorActionPreference = "Stop"
$CondaPrefix = "C:\ProgramData\anaconda3\envs\msimne"
$PythonExe = Join-Path $CondaPrefix "python.exe"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$env:CONDA_PREFIX = $CondaPrefix
$env:PATH = "$CondaPrefix;$CondaPrefix\Scripts;$CondaPrefix\Library\bin;$env:PATH"
$env:GDAL_DATA = "$CondaPrefix\Library\share\gdal"
$env:PROJ_LIB = "$CondaPrefix\Library\share\proj"
Set-Location $ProjectRoot

$Common = @("--profile", "vm", "--aoi-simplify-m", "5", "--http-timeout", "120", "--http-max-retry", "10", "--network-wait-max-seconds", "3600")
if ($AllPending) {
    $RunArgs = @("--next-pending-month", "--from-month", $FromMonth, "--all-regions") + $Common
} else {
    if (-not $Region -or -not $Month) { throw "Specificare -Region e -Month, oppure -AllPending" }
    $RunArgs = @("--region", $Region, "--month", $Month) + $Common
}
if ($OutputsDir) { $RunArgs += @("--outputs-dir", $OutputsDir) }
if ($ExtraArgs) { $RunArgs += $ExtraArgs.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries) }
Write-Host "Avvio: $PythonExe 3.6.2.py $($RunArgs -join ' ')"
& $PythonExe "3.6.2.py" @RunArgs
exit $LASTEXITCODE
