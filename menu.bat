@echo off
title UnknownDatabase Scanner
color 0B
cd /d "%~dp0"

:: ── Find Python ────────────────────────────────────────────────────────────
set PYTHON=

python --version >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    set PYTHON=python
    goto :found_python
)

py --version >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    set PYTHON=py
    goto :found_python
)

for %%V in (314 313 312 311 310) do (
    if exist "%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe" (
        set PYTHON=%LOCALAPPDATA%\Programs\Python\Python%%V\python.exe
        goto :found_python
    )
)

color 0C
echo.
echo  [ERROR] Python not found!
echo.
echo  Download Python from: https://www.python.org/downloads/
echo  Make sure to check: [x] Add Python to PATH
echo.
pause
exit /b 1

:found_python
:menu
cls
echo.
echo  ==========================================
echo    UnknownDatabase Scanner Beta V2.0
echo  ==========================================
echo.
"%PYTHON%" check_version.py 2>nul
echo.
echo    1.  Start scanner
echo    2.  Import database
echo    3.  Delete database
echo    4.  Migrate database (one-time after update)
echo    5.  Recount filter statistics (fix filter counts)
echo.
echo    0.  Exit
echo.
set /p KEUZE=  Choice:

if "%KEUZE%"=="1" goto :start_scanner
if "%KEUZE%"=="2" goto :import_db
if "%KEUZE%"=="3" goto :delete_db
if "%KEUZE%"=="4" goto :migrate_db
if "%KEUZE%"=="5" goto :recount
if "%KEUZE%"=="0" goto :exit

echo.
echo  Invalid choice. Please try again.
timeout /t 1 >nul
goto :menu

:: ── 1. Start scanner ───────────────────────────────────────────────────────
:start_scanner
cls
echo.
echo  ==========================================
echo    Starting scanner...
echo  ==========================================
echo.
echo  Browser will open automatically at http://localhost:3000
echo  Press Ctrl+C to stop the scanner.
echo.
"%PYTHON%" server.py

echo.
echo  Scanner stopped.

:: Check if migration is needed after server runs
"%PYTHON%" check_version.py >nul 2>nul
if %ERRORLEVEL% EQU 1 goto :offer_migrate_after_run
pause
goto :menu

:offer_migrate_after_run
echo.
echo  ==========================================
echo   *** DATABASE UPDATE AVAILABLE ***
echo  ==========================================
echo.
"%PYTHON%" check_version.py
echo.
set /p MIGRATE=  Migrate now? (y/n):
if /i "%MIGRATE%"=="y" goto :migrate_db
pause
goto :menu

:: ── 2. Import database ────────────────────────────────────────────────────
:import_db
cls
echo.
echo  ==========================================
echo    Import database
echo  ==========================================
echo.
echo  Enter the full path to your .txt file.
echo.
set "BESTAND="
set /p BESTAND=  Path to file:

if "%BESTAND%"=="" (
    echo  No path entered.
    pause
    goto :menu
)

set BESTAND=%BESTAND:"=%

if not exist "%BESTAND%" (
    echo.
    echo  [ERROR] File not found:
    echo  %BESTAND%
    echo.
    pause
    goto :menu
)

echo.
set /p DB_NAME=  Name for this database (e.g. "Odido"):
if "%DB_NAME%"=="" set DB_NAME=Database 1

echo.
echo  File     : %BESTAND%
echo  Database : %DB_NAME%
echo.
echo  Starting import...
echo  (Large files may take 10-60 minutes - keep this window open!)
echo.
"%PYTHON%" import.py "%BESTAND%" "%DB_NAME%"
echo.
pause
goto :menu

:: ── 3. Delete database ────────────────────────────────────────────────────
:delete_db
cls
echo.
echo  ==========================================
echo    Delete database
echo  ==========================================
echo.
echo  NOTE: Make sure the scanner (option 1) is not running!
echo.
"%PYTHON%" manage.py delete
pause
goto :menu

:: ── 4. Migrate database ───────────────────────────────────────────────────
:migrate_db
cls
echo.
echo  ==========================================
echo    Migrate database
echo  ==========================================
echo.
echo  This updates an existing database:
echo    - Adds IBAN column (if missing)
echo    - Extracts phone numbers from activity logs
echo    - Adds filter flag columns (f_notes, f_kvk, f_password, etc.)
echo    - Adds identity columns (id_number, birthdate, nationality, gender)
echo    - Rebuilds all indexes (makes filters instant)
echo.
"%PYTHON%" migrate_menu.py
echo.
pause
goto :menu

:: ── 5. Recount filter statistics ──────────────────────────────────────────
:recount
cls
echo.
echo  ==========================================
echo    Recount filter statistics
echo  ==========================================
echo.
echo  Computes and caches filter counts (IBAN, log, summons, etc.)
echo  so the Overzicht filter badges are instant on next start.
echo.
echo  (May take a few minutes for large databases)
echo.
"%PYTHON%" recount.py
echo.
pause
goto :menu

:: ── 0. Exit ────────────────────────────────────────────────────────────────
:exit
exit /b 0
