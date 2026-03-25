REM BUILD_ID: 2026-03-25_free_gui_launcher_v1
@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
if "%ROOT:~-1%"==":" set "ROOT=%ROOT%\"
cd /d "%ROOT%"

title LoneWolf Fang Free GUI

set "PYTHONW="
set "PYTHON="

if exist "%ROOT%python_runtime\pythonw.exe" set "PYTHONW=%ROOT%python_runtime\pythonw.exe"
if exist "%ROOT%python_runtime\python.exe" set "PYTHON=%ROOT%python_runtime\python.exe"

if not defined PYTHONW if exist "%ROOT%python_runtime\Scripts\pythonw.exe" set "PYTHONW=%ROOT%python_runtime\Scripts\pythonw.exe"
if not defined PYTHON if exist "%ROOT%python_runtime\Scripts\python.exe" set "PYTHON=%ROOT%python_runtime\Scripts\python.exe"

if not defined PYTHONW if exist "%ROOT%.venv\Scripts\pythonw.exe" set "PYTHONW=%ROOT%.venv\Scripts\pythonw.exe"
if not defined PYTHON if exist "%ROOT%.venv\Scripts\python.exe" set "PYTHON=%ROOT%.venv\Scripts\python.exe"

if not defined PYTHONW if exist "%LocalAppData%\Programs\Python\Python313\pythonw.exe" set "PYTHONW=%LocalAppData%\Programs\Python\Python313\pythonw.exe"
if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python313\python.exe"

if not defined PYTHONW if exist "%LocalAppData%\Programs\Python\Python312\pythonw.exe" set "PYTHONW=%LocalAppData%\Programs\Python\Python312\pythonw.exe"
if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python312\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python312\python.exe"

if not defined PYTHONW if exist "%LocalAppData%\Programs\Python\Python311\pythonw.exe" set "PYTHONW=%LocalAppData%\Programs\Python\Python311\pythonw.exe"
if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python311\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python311\python.exe"

if not defined PYTHONW (
    echo [ERROR] pythonw.exe was not found.
    echo.
    echo Expected locations:
    echo   %ROOT%python_runtime\pythonw.exe
    echo   %ROOT%python_runtime\Scripts\pythonw.exe
    echo   %ROOT%.venv\Scripts\pythonw.exe
    echo   %LocalAppData%\Programs\Python\Python313\pythonw.exe
    echo   %LocalAppData%\Programs\Python\Python312\pythonw.exe
    echo   %LocalAppData%\Programs\Python\Python311\pythonw.exe
    echo.
    echo Run the local installer first or prepare Python locally.
    pause
    exit /b 1
)

if not defined PYTHON (
    echo [ERROR] python.exe was not found.
    echo.
    echo Expected locations:
    echo   %ROOT%python_runtime\python.exe
    echo   %ROOT%python_runtime\Scripts\python.exe
    echo   %ROOT%.venv\Scripts\python.exe
    echo   %LocalAppData%\Programs\Python\Python313\python.exe
    echo   %LocalAppData%\Programs\Python\Python312\python.exe
    echo   %LocalAppData%\Programs\Python\Python311\python.exe
    echo.
    pause
    exit /b 1
)

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "LWF_HOME=%ROOT%"
set "LWF_RUNTIME_ROOT=%ROOT%runtime"
set "LWF_CONFIGS_ROOT=%ROOT%configs"
set "LWF_MARKET_DATA_ROOT=%ROOT%market_data"
set "PYTHONPATH=%ROOT%app;%PYTHONPATH%"

"%PYTHON%" -c "import app.cli.app_main; import app.gui.main_window; from PySide6.QtWidgets import QApplication" >nul 2>nul
if errorlevel 1 (
    echo [ERROR] LoneWolf Fang Free GUI import check failed.
    echo.
    echo Missing dependency is likely present.
    echo Typical candidates:
    echo   PySide6
    echo   keyring
    echo   requests
    echo.
    echo Opening diagnostic mode...
    "%PYTHON%" -m app.cli.app_main
    pause
    exit /b 1
)

start "" "%PYTHONW%" -m app.cli.app_main --gui
exit /b 0
