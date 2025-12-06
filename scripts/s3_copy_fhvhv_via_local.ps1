# s3_copy_fhvhv_via_local.ps1
# Usage:
# 1) Dry-run: .\s3_copy_fhvhv_via_local.ps1 -DryRun
# 2) Actual run: .\s3_copy_fhvhv_via_local.ps1

param([switch]$DryRun)

$bucket = "nyc-yellowcab-data-as-2025"
$srcPrefix = "tlc/raw/fhv/"
$dstPrefix = "tlc/raw/fhvhv/"
$tmpDir = Join-Path $env:TEMP "s3_fhvhv_tmp"

if (-not (Test-Path $tmpDir)) { New-Item -ItemType Directory -Path $tmpDir | Out-Null }

# get listing of objects under srcPrefix
Write-Host "Listing S3 objects under s3://$bucket/$srcPrefix ..."
$raw = & aws s3 ls "s3://$bucket/$srcPrefix" --recursive 2>&1
if ($LASTEXITCODE -ne 0) {
  Write-Error "aws s3 ls failed. Output:`n$raw"
  exit 1
}

# parse keys and filter for fhvhv files
$keys = @()
foreach ($line in $raw -split "`n") {
  $l = $line.Trim()
  if ($l -eq '') { continue }
  # format: "2025-01-01 00:00:00    12345 tlc/raw/fhv/2019/fhvhv_tripdata_2019-02.parquet"
  $parts = $l -split '\s+'
  $key = $parts[-1]
  if ($key -match "fhvhv_tripdata_") {
    $keys += $key
  }
}

if ($keys.Count -eq 0) {
  Write-Host "No fhvhv objects found under $srcPrefix."
  exit 0
}

Write-Host "Found $($keys.Count) fhvhv keys to handle."

foreach ($key in $keys) {
  $fileName = [System.IO.Path]::GetFileName($key)
  $localPath = Join-Path $tmpDir $fileName
  # keep same year subfolder if present in key
  $relative = $key.Substring($srcPrefix.Length).TrimStart('/')
  $dstKey = $dstPrefix + $relative.Replace('\','/')

  Write-Host "----"
  Write-Host "Source: s3://$bucket/$key"
  Write-Host "Dest  : s3://$bucket/$dstKey"
  Write-Host "Local : $localPath"

  if ($DryRun) {
    Write-Host "[DRYRUN] aws s3 cp s3://$bucket/$key $localPath"
    Write-Host "[DRYRUN] aws s3 cp $localPath s3://$bucket/$dstKey"
    continue
  }

  # download
  Write-Host "Downloading..."
  & aws s3 cp "s3://$bucket/$key" $localPath
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Download failed for $key — skipping."
    if (Test-Path $localPath) { Remove-Item $localPath -Force }
    continue
  }

  # upload
  Write-Host "Uploading to destination..."
  & aws s3 cp $localPath "s3://$bucket/$dstKey"
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Upload failed for $dstKey — leaving local file for inspection."
    continue
  }

  # optional: verify existence quickly (head)
  $head = & aws s3api head-object --bucket $bucket --key $dstKey 2>&1
  if ($LASTEXITCODE -ne 0) {
    Write-Warning "Warning: head-object for $dstKey failed, but upload exit-code was 0."
  }

  # delete local temp
  Remove-Item $localPath -Force
  Write-Host "Done: $key -> $dstKey (temp removed)."
}

Write-Host "All done. Temp dir: $tmpDir"
