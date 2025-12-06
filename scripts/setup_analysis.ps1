# NYC Taxi Comprehensive Analysis Setup
# Run this before executing the analysis notebook

Write-Host "Setting up NYC Taxi Analysis Environment..." -ForegroundColor Cyan

# Activate virtual environment
Write-Host "`n1. Activating virtual environment..." -ForegroundColor Yellow
& ".\sparkprojenv\Scripts\Activate.ps1"

# Load environment variables
Write-Host "`n2. Loading environment variables from .env..." -ForegroundColor Yellow
. .\scripts\load_env.ps1

# Verify AWS credentials
Write-Host "`n3. Verifying AWS credentials..." -ForegroundColor Yellow
if ($env:AWS_ACCESS_KEY_ID -and $env:AWS_SECRET_ACCESS_KEY) {
    Write-Host "   ✓ AWS_ACCESS_KEY_ID: $($env:AWS_ACCESS_KEY_ID.Substring(0, 10))..." -ForegroundColor Green
    Write-Host "   ✓ AWS_SECRET_ACCESS_KEY: [HIDDEN]" -ForegroundColor Green
    Write-Host "   ✓ AWS_DEFAULT_REGION: $env:AWS_DEFAULT_REGION" -ForegroundColor Green
} else {
    Write-Host "   ✗ AWS credentials not found! Please set them in .env file." -ForegroundColor Red
    exit 1
}

# Install missing packages
Write-Host "`n4. Installing required packages..." -ForegroundColor Yellow
pip install -q python-dotenv scipy jupyter

# Create output directory
Write-Host "`n5. Creating output directories..." -ForegroundColor Yellow
$outputDir = "data\local_output\analytics"
if (!(Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    Write-Host "   ✓ Created $outputDir" -ForegroundColor Green
} else {
    Write-Host "   ✓ Directory already exists: $outputDir" -ForegroundColor Green
}

Write-Host "`n" + "="*60 -ForegroundColor Cyan
Write-Host "Environment Ready!" -ForegroundColor Green
Write-Host "="*60 -ForegroundColor Cyan
Write-Host "`nTo start analysis:" -ForegroundColor Yellow
Write-Host "  jupyter notebook notebooks/comprehensive_trip_analysis.ipynb" -ForegroundColor White
Write-Host "`nOR run the notebook in VS Code." -ForegroundColor White
Write-Host ""
