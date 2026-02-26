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
echo  [FOUT] Python niet gevonden!
echo.
echo  Download Python via: https://www.python.org/downloads/
echo  Vink aan: [x] Add Python to PATH
echo.
pause
exit /b 1

:found_python
:menu
cls
echo.
echo  ==========================================
echo    UnknownDatabase Scanner
echo  ==========================================
echo.
echo    1.  Scanner starten
echo    2.  Database importeren
echo    3.  Database verwijderen
echo    4.  Database migreren (eenmalig na update)
echo.
echo    0.  Afsluiten
echo.
set /p KEUZE=  Keuze:

if "%KEUZE%"=="1" goto :start_scanner
if "%KEUZE%"=="2" goto :import_db
if "%KEUZE%"=="3" goto :delete_db
if "%KEUZE%"=="4" goto :migrate_db
if "%KEUZE%"=="0" goto :exit

echo.
echo  Ongeldige keuze. Probeer opnieuw.
timeout /t 1 >nul
goto :menu

:: ── 1. Start scanner ───────────────────────────────────────────────────────
:start_scanner
cls
echo.
echo  ==========================================
echo    Scanner starten...
echo  ==========================================
echo.
echo  Browser opent automatisch op http://localhost:3000
echo  Druk Ctrl+C om de scanner te stoppen.
echo.
"%PYTHON%" server.py
echo.
echo  Scanner gestopt.
pause
goto :menu

:: ── 2. Import database ────────────────────────────────────────────────────
:import_db
cls
echo.
echo  ==========================================
echo    Database importeren
echo  ==========================================
echo.
echo  Sleep een .txt bestand hierop, of geef het pad op.
echo.
set "BESTAND="
set /p BESTAND=  Pad naar bestand:

if "%BESTAND%"=="" (
    echo  Geen pad opgegeven.
    pause
    goto :menu
)

set BESTAND=%BESTAND:"=%

if not exist "%BESTAND%" (
    echo.
    echo  [FOUT] Bestand niet gevonden:
    echo  %BESTAND%
    echo.
    pause
    goto :menu
)

echo.
set /p DB_NAME=  Naam voor deze database (bijv. "Odido"):
if "%DB_NAME%"=="" set DB_NAME=Database 1

echo.
echo  Bestand  : %BESTAND%
echo  Database : %DB_NAME%
echo.
echo  Importeren gestart...
echo  (Grote bestanden kunnen 10-60 minuten duren - laat dit venster open!)
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
echo    Database verwijderen
echo  ==========================================
echo.
echo  LET OP: Zorg dat de scanner (optie 1) niet actief is!
echo.
"%PYTHON%" manage.py delete
pause
goto :menu

:: ── 4. Migrate database ───────────────────────────────────────────────────
:migrate_db
cls
echo.
echo  ==========================================
echo    Database migreren
echo  ==========================================
echo.
echo  Dit repareert een bestaande database:
echo    - Voegt IBAN kolom toe
echo    - Extraheert telefoonnummers uit logs
echo    - Maakt nieuwe indexes aan
echo.

"%PYTHON%" manage.py list

set /p MIG_DB=  Naam van de database om te migreren (Enter = database.db):
if "%MIG_DB%"=="" (
    if not exist "%~dp0database.db" (
        echo.
        echo  [FOUT] database.db niet gevonden. Importeer eerst een database.
        pause
        goto :menu
    )
    "%PYTHON%" migrate.py
) else (
    :: Find the file for this database name
    "%PYTHON%" -c "import json,sys,pathlib; m=json.loads(pathlib.Path('databases.json').read_text('utf-8')); e=next((x for x in m if x['name']==sys.argv[1]),None); sys.exit(0 if e else 1); print(e['file'])" "%MIG_DB%" >nul 2>nul
    if %ERRORLEVEL% NEQ 0 (
        echo.
        echo  [FOUT] Database niet gevonden: %MIG_DB%
        pause
        goto :menu
    )
    for /f "delims=" %%F in ('"%PYTHON%" -c "import json,sys,pathlib; m=json.loads(pathlib.Path('databases.json').read_text('utf-8')); e=next((x for x in m if x['name']==sys.argv[1]),None); print(e['file'] if e else '')" "%MIG_DB%"') do set DB_FILE=%%F
    if "%DB_FILE%"=="" (
        echo  [FOUT] Bestand niet gevonden voor database: %MIG_DB%
        pause
        goto :menu
    )
    "%PYTHON%" migrate.py "%DB_FILE%"
)
echo.
pause
goto :menu

:: ── 0. Exit ────────────────────────────────────────────────────────────────
:exit
exit /b 0
