@echo off
REM Extract all 216 tables to Parquet format using the virtualenv

REM Find the virtualenv location (it should be indicated by your command prompt)
REM From your output: (.venv) C:\python\python\4O_to_snowflake_ETL_project
REM This means your .venv is in C:\python\python\4O_to_snowflake_ETL_project

REM Change to your project directory
cd /d "C:\python\python\4O_to_snowflake_ETL_project" 2>nul
IF ERRORLEVEL 1 (
    echo ERROR: Directory not found: C:\python\python\4O_to_snowflake_ETL_project
    echo Please check that your project directory path is correct
    pause
    exit /b 1
)

echo.
echo ================================================================================
echo ACTIVATING VIRTUAL ENVIRONMENT
echo ================================================================================
echo.

REM Activate the virtualenv
call .venv\Scripts\activate.bat
IF ERRORLEVEL 1 (
    echo ERROR: Failed to activate virtualenv
    pause
    exit /b 1
)

echo Virtual environment activated successfully
echo.
echo ================================================================================
echo RUNNING EXTRACTION FOR ALL 216 TABLES
echo ================================================================================
echo.
echo This will take 15-30 minutes. Please do NOT close this window.
echo.

REM Run the extraction with diagnostic output
python scripts\extract_table_to_csv.py --table all --format parquet --no-file-logging 2>&1 | Tee-Object -FilePath "extraction_run_%%DATETIME%%.txt"

echo.
echo ================================================================================
echo EXTRACTION COMPLETE
echo ================================================================================
echo.
echo Check the log file for any ERROR messages.
pause
