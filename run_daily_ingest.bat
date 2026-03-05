@echo off
setlocal EnableExtensions
chcp 65001 >nul

cd /d "%~dp0"

call "%~dp0setup_env.bat"
if not "%ERRORLEVEL%"=="0" exit /b 1

set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found: %PYTHON_EXE%
  exit /b 1
)

set "DB_PATH=press_unified.db"
set "PREVIEW_JSON=ingest_preview_daily.json"
set "SERVICE_KEY="

if not "%~1"=="" set "SERVICE_KEY=%~1"

if "%SERVICE_KEY%"=="" if exist "service_key.txt" (
  set /p SERVICE_KEY=<service_key.txt
)

if "%SERVICE_KEY%"=="" if not "%DATA_GO_SERVICE_KEY%"=="" (
  set "SERVICE_KEY=%DATA_GO_SERVICE_KEY%"
)

if "%SERVICE_KEY%"=="" (
  echo [ERROR] SERVICE_KEY is empty.
  echo 1^) Create service_key.txt with the key in the first line
  echo 2^) Set DATA_GO_SERVICE_KEY environment variable
  echo 3^) Run: run_daily_ingest.bat ^<YOUR_SERVICE_KEY^>
  exit /b 1
)

for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyyMMdd_HHmmss')"') do set "TS=%%i"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString('yyyyMMdd')"') do set "END_DATE=%%i"
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).AddDays(-5).ToString('yyyyMMdd')"') do set "START_DATE=%%i"

if not exist "logs" mkdir "logs"
set "LOG_FILE=logs\ingest_%TS%.log"

echo [INFO] Daily ingestion started
echo [INFO] Date window: %START_DATE% ~ %END_DATE%
echo [INFO] Log file: %LOG_FILE%

"%PYTHON_EXE%" unified_press_ingest.py --service-key "%SERVICE_KEY%" --start-date %START_DATE% --end-date %END_DATE% --db-path "%DB_PATH%" --preview-json "%PREVIEW_JSON%" --config ingest_config.yaml 1>"%LOG_FILE%" 2>&1

set "EXIT_CODE=%ERRORLEVEL%"
type "%LOG_FILE%"

if not "%EXIT_CODE%"=="0" (
  echo [ERROR] Daily ingestion failed. Exit code: %EXIT_CODE%
  exit /b %EXIT_CODE%
)

echo [INFO] Daily ingestion completed successfully.
echo [INFO] DB updated: %DB_PATH%
exit /b 0
