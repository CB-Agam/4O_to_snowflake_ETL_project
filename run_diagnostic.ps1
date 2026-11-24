# Run All Diagnostics for Extraction Issue

Write-Host "================================================================================`n" -ForegroundColor Cyan
Write-Host "EXTRACTION ISSUE DIAGNOSTICS" -ForegroundColor Cyan
Write-Host "================================================================================`n" -ForegroundColor Cyan

$ProjectRoot = "C:\ETL\snowglake to 4D"
cd $ProjectRoot

# Check 1: Disk Space
Write-Host "[STEP 1] Checking Disk Space..." -ForegroundColor Yellow
Write-Host ""
$drive = Get-Volume | Where-Object {$_.DriveLetter -eq 'C'}
$freeGB = [math]::Round($drive.SizeRemaining / 1GB, 2)
$totalGB = [math]::Round($drive.Size / 1GB, 2)
Write-Host "C: Drive Space"
Write-Host "  Total: $totalGB GB"
Write-Host "  Free:  $freeGB GB"
Write-Host "  Used:  $([math]::Round(($drive.Size - $drive.SizeRemaining) / 1GB, 2)) GB"
Write-Host ""

# Check 2: Export folder size
Write-Host "[STEP 2] Checking Export Folder Size..." -ForegroundColor Yellow
Write-Host ""
if (Test-Path exports) {
    $exportsSize = (Get-ChildItem -Path exports -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
    Write-Host "  Exports folder: $([math]::Round($exportsSize, 2)) GB"
} else {
    Write-Host "  Exports folder: Does not exist yet"
}
Write-Host ""

# Check 3: Table count
Write-Host "[STEP 3] Discovering Tables in 4D..." -ForegroundColor Yellow
Write-Host ""
$output = python -c "from scripts.extract_table_to_csv import _discover_tables; from connections.fourd_connection import connect_to_4d, close_4d_connection; c=connect_to_4d(); t=_discover_tables(c); print(f'Total tables: {len(t)}'); close_4d_connection(c)" 2>&1
Write-Host $output
Write-Host ""

# Check 4: Run extraction with diagnostics
Write-Host "[STEP 4] Running Extraction with Diagnostics..." -ForegroundColor Yellow
Write-Host "         (This may take several minutes...)" -ForegroundColor Yellow
Write-Host ""

$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = "extraction_diagnostic_$timestamp.txt"

python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath $logFile

Write-Host ""
Write-Host "================================================================================`n" -ForegroundColor Cyan
Write-Host "DIAGNOSTICS COMPLETE" -ForegroundColor Cyan
Write-Host "================================================================================`n" -ForegroundColor Cyan
Write-Host "Full output saved to: $logFile" -ForegroundColor Green
Write-Host ""
Write-Host "To share this with support, please provide:" -ForegroundColor Yellow
Write-Host "1. The file: $logFile"
Write-Host "2. Your Excel sheet with table row counts"
Write-Host "3. Any ERROR messages shown above"
Write-Host ""
