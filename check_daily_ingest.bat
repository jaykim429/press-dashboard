@echo off
setlocal EnableExtensions
chcp 65001 >nul

cd /d "%~dp0"

set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found: %PYTHON_EXE%
  exit /b 1
)

set "DB_PATH=press_unified.db"
set "LOG_DIR=logs"
set "LATEST_LOG="
set "HAS_ERROR=0"

if not exist "%LOG_DIR%" (
  echo [WARN] logs folder not found: %LOG_DIR%
  goto :DB_CHECK
)

for /f "delims=" %%f in ('dir /b /a:-d /o:-n "%LOG_DIR%\ingest_*.log" 2^>nul') do (
  set "LATEST_LOG=%%f"
  goto :GOT_LOG
)

:GOT_LOG
if "%LATEST_LOG%"=="" (
  echo [WARN] No ingest log found under %LOG_DIR%
  goto :DB_CHECK
)

echo ==================================================
echo [INFO] Latest log: %LOG_DIR%\%LATEST_LOG%
echo ==================================================

type "%LOG_DIR%\%LATEST_LOG%"

findstr /i /c:"[ERROR]" "%LOG_DIR%\%LATEST_LOG%" >nul && set "HAS_ERROR=1"
findstr /i /c:"Traceback" "%LOG_DIR%\%LATEST_LOG%" >nul && set "HAS_ERROR=1"
findstr /i /c:"Daily ingestion completed successfully." "%LOG_DIR%\%LATEST_LOG%" >nul
if not "%ERRORLEVEL%"=="0" (
  echo [WARN] Success marker not found in latest log.
  set "HAS_ERROR=1"
)

if "%HAS_ERROR%"=="1" (
  echo [WARN] Log indicates potential failure. Check details above.
) else (
  echo [OK] Log check passed.
)

:DB_CHECK
echo.
echo ==================================================
echo [INFO] DB check (%DB_PATH%)
echo ==================================================

if not exist "%DB_PATH%" (
  echo [ERROR] DB file not found: %DB_PATH%
  exit /b 2
)

"%PYTHON_EXE%" -c "import sqlite3; conn=sqlite3.connect('press_unified.db'); c=conn.cursor(); c.execute(\"select count(*) from articles where first_seen_at between datetime('now','-1 day') and datetime('now')\"); day_cnt=c.fetchone()[0]; c.execute(\"select source_channel, count(*) from articles where first_seen_at between datetime('now','-1 day') and datetime('now') group by source_channel order by count(*) desc\"); rows=c.fetchall(); c.execute(\"select max(published_at) from articles\"); max_pub=(c.fetchone() or [None])[0]; c.execute(\"select min(published_at) from (select published_at from articles where published_at is not null and trim(published_at) != '' order by published_at desc limit 30)\"); min_in_top=(c.fetchone() or [None])[0]; print(f'[INFO] Rows inserted in last 24h: {day_cnt}'); print('[INFO] Per-channel counts (last 24h):'); print('  - (none)' if not rows else ''); [print(f'  - {ch}: {cnt}') for ch,cnt in rows]; print(f'[INFO] Latest published_at in DB: {max_pub}'); print(f'[INFO] Oldest published_at among latest 30 rows: {min_in_top}'); conn.close()"
if not "%ERRORLEVEL%"=="0" (
  echo [ERROR] DB query failed.
  exit /b 3
)

echo.
echo [DONE] Batch health check finished.
if "%HAS_ERROR%"=="1" exit /b 10
exit /b 0
