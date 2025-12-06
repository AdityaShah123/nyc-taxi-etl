<#
scripts/run_etl.ps1
Run ETL job for NYC taxi data on S3 with proper Spark configuration.
Usage:
  .\scripts\run_etl.ps1 -CabType yellow -Year 2019 -Month 01
  .\scripts\run_etl.ps1 -CabType green -Year 2019 -Month 01
  .\scripts\run_etl.ps1 -CabType fhv -Year 2015 -Month 01
  .\scripts\run_etl.ps1 -CabType fhvhv -Year 2019 -Month 01
#>

param(
    [Parameter(Mandatory=$true)][string]$CabType,
    [Parameter(Mandatory=$true)][int]$Year,
    [Parameter(Mandatory=$true)][int]$Month
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Push-Location $repoRoot

Write-Host "Running ETL: $CabType $Year-$($Month.ToString('D2'))" -ForegroundColor Cyan

# Use relative paths from repo root
# Exclude slf4j-api to avoid class loader conflicts with Spark's bundled SLF4J
& ".\spark-4.0.1-bin-hadoop3\bin\spark-submit.cmd" `
  --master local[1] `
  --driver-memory 2g `
  --packages "org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:s3:2.17.283" `
  --exclude-packages "org.slf4j:slf4j-api" `
  --conf "spark.driver.userClassPathFirst=true" `
  --conf "spark.executor.userClassPathFirst=true" `
  --conf "spark.network.timeout=120s" `
  --conf "spark.sql.shuffle.partitions=2" `
  ".\spark_jobs\etl_yellow_s3.py" `
  --input-base "s3a://nyc-yellowcab-data-as-2025/tlc/raw" `
  --output-base "s3a://nyc-yellowcab-data-as-2025/tlc/curated" `
  --cab-type $CabType `
  --year $Year `
  --month $Month

$exitCode = $LASTEXITCODE
Pop-Location

if ($exitCode -eq 0) {
    Write-Host "SUCCESS: ETL completed" -ForegroundColor Green
    Write-Host "Check S3: aws s3 ls s3://nyc-yellowcab-data-as-2025/tlc/curated/$CabType/ --recursive" -ForegroundColor Yellow
} else {
    Write-Host "FAILED: ETL job exited with code $exitCode" -ForegroundColor Red
}

exit $exitCode
