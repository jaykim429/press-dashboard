@echo off
setlocal

:: Get the path to the daily ingest batch file
set "INGEST_BAT=%~dp0run_daily_ingest.bat"

:: Unregister potential existing tasks to avoid conflicts
schtasks /Delete /TN "PressDataIngest_0800" /F >nul 2>&1
schtasks /Delete /TN "PressDataIngest_1200" /F >nul 2>&1
schtasks /Delete /TN "PressDataIngest_1600" /F >nul 2>&1

:: Create scheduled task for 8:00 AM on weekdays
schtasks /Create /TN "PressDataIngest_0800" /TR "\"%INGEST_BAT%\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 08:00 /F
if %ERRORLEVEL%==0 (
    echo [INFO] Scheduled task PressDataIngest_0800 created successfully.
) else (
    echo [ERROR] Failed to schedule task for 08:00.
)

:: Create scheduled task for 12:00 PM on weekdays
schtasks /Create /TN "PressDataIngest_1200" /TR "\"%INGEST_BAT%\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 12:00 /F
if %ERRORLEVEL%==0 (
    echo [INFO] Scheduled task PressDataIngest_1200 created successfully.
) else (
    echo [ERROR] Failed to schedule task for 12:00.
)

:: Create scheduled task for 4:00 PM (16:00) on weekdays
schtasks /Create /TN "PressDataIngest_1600" /TR "\"%INGEST_BAT%\"" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST 16:00 /F
if %ERRORLEVEL%==0 (
    echo [INFO] Scheduled task PressDataIngest_1600 created successfully.
) else (
    echo [ERROR] Failed to schedule task for 16:00.
)

echo [INFO] All tasks scheduled. You can verify them in Windows Task Scheduler.
