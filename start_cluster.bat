@echo off
rem =====================================================================
rem  ComfyUI Cluster launcher
rem
rem  - If the manager is already running, it does nothing.
rem  - Otherwise it activates the virtual environment and starts the app
rem    in its own window (so you can see the log and stop it with Ctrl+C).
rem  - This launcher window then closes itself, leaving just the app window.
rem
rem  Options:   open    also open the dashboard in your browser
rem             silent  never pause for a keypress (for Task Scheduler / Startup)
rem
rem  Uses port 8189 unless the CLUSTER_PORT environment variable is set.
rem =====================================================================

cd /d "%~dp0"

set "OPEN="
set "SILENT="
for %%A in (%*) do (
    if /i "%%A"=="open" set "OPEN=1"
    if /i "%%A"=="silent" set "SILENT=1"
)

if not defined CLUSTER_PORT set "CLUSTER_PORT=8189"
set "URL=http://localhost:%CLUSTER_PORT%/cluster"

rem ---- 1. Already running? -------------------------------------------
call :is_running
if defined RUNNING goto :already

rem ---- 2. Find the virtual environment --------------------------------
set "VENV="
if exist ".venv\Scripts\activate.bat" set "VENV=.venv"
if not defined VENV if exist "venv\Scripts\activate.bat" set "VENV=venv"
if not defined VENV goto :no_venv
if not exist "run.py" goto :no_run

rem ---- 3. Start it in its own window ----------------------------------
echo Starting ComfyUI Cluster in its own window...
start "ComfyUI Cluster" cmd /k ""%VENV%\Scripts\activate.bat" && python run.py"

rem Nothing more to do unless the browser should be opened: close this
rem launcher window so only the app window is left.
if not defined OPEN goto :quit

rem ---- 4. Wait until it is listening (up to about 20 seconds) ---------
set /a TRIES=0
:wait
ping -n 2 127.0.0.1 >nul
call :is_running
if defined RUNNING goto :started
set /a TRIES+=1
if %TRIES% LSS 20 goto :wait
echo.
echo The app did not start within 20 seconds.
echo Check the "ComfyUI Cluster" window for error messages.
goto :done

:started
start "" "%URL%"
goto :quit

:already
echo ComfyUI Cluster is already running on port %CLUSTER_PORT%.
if defined OPEN start "" "%URL%"
if defined OPEN goto :quit
goto :done

:no_venv
echo Could not find a virtual environment (.venv or venv) in this folder.
echo Create one with:
echo     python -m venv .venv
echo     .venv\Scripts\activate
echo     pip install -r requirements.txt
goto :done

:no_run
echo run.py was not found in this folder. Keep this file next to run.py.
goto :done

:done
rem Only reached when there is something to read (an error or "already running").
if not defined SILENT pause
exit /b

:quit
exit /b

rem ---- helper: sets RUNNING=1 if something is listening on the port ---
:is_running
set "RUNNING="
netstat -ano | findstr /R /C:":%CLUSTER_PORT% .*LISTENING" >nul && set "RUNNING=1"
exit /b
