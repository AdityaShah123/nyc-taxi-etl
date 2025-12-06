<#
scripts/disable_local_aws_jars.ps1
Safely rename local AWS/Hadoop JARs in the repo `jars` folder by appending `.disabled` so Spark will not load them.
This script performs a reversible rename (adds `.disabled`). It only acts on a small set of known file patterns.
Usage (from repo root):
  .\scripts\disable_local_aws_jars.ps1
#>

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$jarDir = Join-Path $repoRoot "jars"
if (-Not (Test-Path $jarDir)) {
    Write-Host "No jars directory found at $jarDir" -ForegroundColor Yellow
    return
}

$patterns = @("aws-java-sdk-bundle*.jar", "hadoop-aws*.jar", "aws-core*.jar", "s3-*.jar")

foreach ($p in $patterns) {
    $files = Get-ChildItem -Path $jarDir -Filter $p -File -ErrorAction SilentlyContinue
    foreach ($f in $files) {
        $target = "$($f.FullName).disabled"
        if (-Not (Test-Path $target)) {
            Rename-Item -Path $f.FullName -NewName "$($f.Name).disabled"
            Write-Host "Renamed $($f.Name) -> $($f.Name).disabled"
        } else {
            Write-Host "Already disabled: $($f.Name)" -ForegroundColor Gray
        }
    }
}

Write-Host "Done. You can re-enable by renaming files removing the `.disabled` suffix." -ForegroundColor Green
