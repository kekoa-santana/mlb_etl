@echo off
REM Scheduled wrapper for daily_refresh.py.
REM Redirects stdout+stderr to a timestamped log so scheduled failures are
REM inspectable after the fact.

setlocal
set ETL_DIR=C:\Users\kekoa\Documents\data_analytics\mlb_fantasy_ETL
set LOG_DIR=C:\Users\kekoa\Documents\data_analytics\player_profiles\logs

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Build YYYY-MM-DD_HHMM stamp (locale-independent via PowerShell).
for /f "usebackq delims=" %%t in (`powershell -NoProfile -Command "Get-Date -Format 'yyyy-MM-dd_HHmm'"`) do set STAMP=%%t

set LOG_FILE=%LOG_DIR%\daily_refresh_scheduled_%STAMP%.log

cd /d "%ETL_DIR%"
"%ETL_DIR%\myenv\Scripts\python.exe" "%ETL_DIR%\daily_refresh.py" > "%LOG_FILE%" 2>&1
exit /b %ERRORLEVEL%
