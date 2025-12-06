# NYC Taxi Analysis Setup
Write-Host "Setting up NYC Taxi Analysis Environment..." -ForegroundColor Cyan
Write-Host "
1. Activating virtual environment..." -ForegroundColor Yellow
& ".\sparkprojenv\Scripts\Activate.ps1"
Write-Host "
2. Loading environment variables..." -ForegroundColor Yellow
. .\scripts\load_env.ps1
Write-Host "
3. Verifying AWS credentials..." -ForegroundColor Yellow
if ($env:AWS_ACCESS_KEY_ID -and $env:AWS_SECRET_ACCESS_KEY) {
    Write-Host "    AWS credentials loaded" -ForegroundColor Green
} else {
    Write-Host "    AWS credentials not found!" -ForegroundColor Red
    exit 1
}
Write-Host "
4. Installing packages..." -ForegroundColor Yellow
pip install -q python-dotenv scipy jupyter
Write-Host "
5. Creating output directory..." -ForegroundColor Yellow
$outputDir = "data\local_output\analytics"
if (!(Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    Write-Host "    Created $outputDir" -ForegroundColor Green
} else {
    Write-Host "    Directory exists" -ForegroundColor Green
}
Write-Host "
Environment Ready!" -ForegroundColor Green
Write-Host "Run: jupyter notebook notebooks/comprehensive_trip_analysis.ipynb" -ForegroundColor Yellow
