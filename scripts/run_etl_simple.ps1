<#
scripts/run_etl_simple.ps1
Run simple pandas-based ETL (no Spark JVM issues)
Usage:
  .\scripts\run_etl_simple.ps1 -CabType yellow -Year 2019 -Month 01
#>
param(
    [Parameter(Mandatory=$true)][string]$CabType,
    [Parameter(Mandatory=$true)][int]$Year,
    [Parameter(Mandatory=$true)][int]$Month
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Push-Location $repoRoot

Write-Host "ETL: $CabType $Year-$($Month.ToString('D2'))" -ForegroundColor Cyan

python spark_jobs\etl_simple.py --cab-type $CabType --year $Year --month $Month 2>&1 | Select-String "\[ETL\]"

$exitCode = $LASTEXITCODE
Pop-Location

if ($exitCode -eq 0) {
    Write-Host "DONE - Check: aws s3 ls s3://nyc-yellowcab-data-as-2025/tlc/curated/$CabType/ --recursive" -ForegroundColor Green
} else {
    Write-Host "FAILED (exit code $exitCode)" -ForegroundColor Red
}

exit $exitCode
