@echo off
setlocal
chcp 65001 >nul

cd /d "%~dp0"

call "%~dp0setup_env.bat"
if not "%ERRORLEVEL%"=="0" (
  pause
  exit /b 1
)

set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found: %PYTHON_EXE%
  pause
  exit /b 1
)

set "HOST=127.0.0.1"
set "PORT=8080"
set "DB_PATH=press_unified.db"

if not exist "%DB_PATH%" (
  echo [ERROR] DB file not found: %DB_PATH%
  echo Run unified ingestion first to create the database.
  pause
  exit /b 1
)

echo Starting dashboard...
echo URL: http://%HOST%:%PORT%
start "" "http://%HOST%:%PORT%"

"%PYTHON_EXE%" local_dashboard.py --db-path "%DB_PATH%" --host %HOST% --port %PORT%

echo.
echo Dashboard stopped.
pause
