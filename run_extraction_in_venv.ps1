# Extract all 216 tables to Parquet format
# IMPORTANT: This must run from your virtualenv directory where pyarrow is installed

param(
    [string]$ProjectPath = "C:\python\python\4O_to_snowflake_ETL_project"
)

Write-Host "
================================================================================
EXTRACTION RUNNER - FIND YOUR VIRTUALENV
================================================================================

Your prompt shows: (.venv) C:\python\python\4O_to_snowflake_ETL_project>

This means your .venv is in: $ProjectPath

If this path is incorrect, run:
  powershell -ExecutionPolicy Bypass -File run_extraction_in_venv.ps1 -ProjectPath 'C:\your\actual\path'

================================================================================
" -ForegroundColor Cyan

# Check if directory exists
if (-not (Test-Path $ProjectPath)) {
    Write-Host "ERROR: Project directory not found: $ProjectPath" -ForegroundColor Red
    Write-Host "
Please provide the correct path to your project directory containing the .venv folder.

Usage:
  powershell -ExecutionPolicy Bypass -File run_extraction_in_venv.ps1 -ProjectPath 'C:\your\actual\path'
" -ForegroundColor Yellow
    exit 1
}

# Check if .venv exists
$venvPath = Join-Path $ProjectPath ".venv"
if (-not (Test-Path $venvPath)) {
    Write-Host "ERROR: Virtual environment not found at: $venvPath" -ForegroundColor Red
    Write-Host "
Looking for .venv in: $ProjectPath

Please verify the project directory is correct and contains a .venv folder.
" -ForegroundColor Yellow
    exit 1
}

cd $ProjectPath

Write-Host "
================================================================================
ACTIVATING VIRTUAL ENVIRONMENT
================================================================================
" -ForegroundColor Cyan

# Activate virtualenv
& "$venvPath\Scripts\Activate.ps1"

Write-Host "
================================================================================
RUNNING EXTRACTION FOR ALL 216 TABLES
================================================================================
" -ForegroundColor Cyan

Write-Host "
This will take 15-30 minutes. Please do NOT close this window.
Full output will be saved to a timestamped file.
" -ForegroundColor Yellow

# Create output filename with timestamp
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$outputFile = "extraction_run_$timestamp.txt"

Write-Host "
Output file: $outputFile
" -ForegroundColor Green

# Run extraction and save output
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath $outputFile

Write-Host "
================================================================================
EXTRACTION COMPLETE
================================================================================
" -ForegroundColor Cyan

Write-Host "
Output saved to: $outputFile

Please check this file for:
  1. Final line: 'Extraction run complete: X of Y tables processed'
  2. Any ERROR messages: 'ERROR: Extraction stopped unexpectedly:'
  3. How many tables were actually processed

Share this file with your team along with:
  - Your Excel sheet with table row counts
  - Any ERROR messages shown
  - Disk space information
" -ForegroundColor Yellow

Write-Host "
Press any key to close this window...
" -ForegroundColor Gray

$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
