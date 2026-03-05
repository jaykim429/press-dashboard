@echo off
setlocal EnableExtensions
chcp 65001 >nul

cd /d "%~dp0"

set "PY_BOOTSTRAP="
where py >nul 2>nul
if %ERRORLEVEL%==0 set "PY_BOOTSTRAP=py -3"
if not defined PY_BOOTSTRAP (
  where python >nul 2>nul
  if %ERRORLEVEL%==0 set "PY_BOOTSTRAP=python"
)

if not defined PY_BOOTSTRAP (
  echo [ERROR] Python 3 is not installed.
  echo Install Python 3.10+ and run this file again.
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo [INFO] Creating virtual environment...
  %PY_BOOTSTRAP% -m venv .venv
  if not "%ERRORLEVEL%"=="0" (
    echo [ERROR] Failed to create .venv
    exit /b 1
  )
)

set "VENV_PY=.venv\Scripts\python.exe"
if not exist "%VENV_PY%" (
  echo [ERROR] venv python not found: %VENV_PY%
  exit /b 1
)

echo [INFO] Installing/updating dependencies...
"%VENV_PY%" -m pip install --upgrade pip >nul 2>nul
if exist "requirements.txt" (
  "%VENV_PY%" -m pip install -r requirements.txt
) else (
  "%VENV_PY%" -m pip install requests beautifulsoup4
)
if not "%ERRORLEVEL%"=="0" (
  echo [ERROR] Failed to install dependencies.
  exit /b 1
)

echo [INFO] Environment is ready.
exit /b 0
