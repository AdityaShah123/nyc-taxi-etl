<#
scripts/load_env.ps1
Load environment variables from the repository `.env` file into the current PowerShell session.
Usage (recommended to persist in current session):
  . .\scripts\load_env.ps1   # dot-source the script (note the leading dot) so variables remain in session
Or run non-dot-sourced:
  & .\scripts\load_env.ps1
#>

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$envPathCandidate = Join-Path $scriptDir "..\.env"
try {
    $envPath = (Resolve-Path -Path $envPathCandidate -ErrorAction Stop).Path
} catch {
    Write-Host ".env not found at: $envPathCandidate" -ForegroundColor Yellow
    return
}

Write-Host "Loading env from: $envPath"

Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if ([string]::IsNullOrWhiteSpace($line)) { return }
    if ($line.StartsWith("#")) { return }
    $parts = $line -split "=", 2
    if ($parts.Length -ne 2) { return }
    $name = $parts[0].Trim()
    $value = $parts[1].Trim()
    # remove surrounding quotes if present (single or double)
    if ($value.StartsWith('"') -and $value.EndsWith('"')) {
      $value = $value.Trim('"')
    } elseif ($value.StartsWith("'") -and $value.EndsWith("'")) {
      $value = $value.Trim("'")
    }
    if ($name -eq '') { return }
    Set-Item -Path "Env:$name" -Value $value -Force
    Write-Host ("Set env: {0}" -f $name)
}

Write-Host "Done. Use `Get-ChildItem Env:` to inspect environment variables." -ForegroundColor Green
