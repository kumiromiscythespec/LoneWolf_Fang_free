REM BUILD_ID: 2026-03-25_free_shell_launcher_v1
@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
if "%ROOT:~-1%"==":" set "ROOT=%ROOT%\"
cd /d "%ROOT%"

set "PYTHON="
set "PYTHON_DIR="

if exist "%ROOT%python_runtime\python.exe" set "PYTHON=%ROOT%python_runtime\python.exe"
if exist "%ROOT%python_runtime\python.exe" set "PYTHON_DIR=%ROOT%python_runtime"

if not defined PYTHON if exist "%ROOT%python_runtime\Scripts\python.exe" set "PYTHON=%ROOT%python_runtime\Scripts\python.exe"
if not defined PYTHON_DIR if exist "%ROOT%python_runtime\Scripts\python.exe" set "PYTHON_DIR=%ROOT%python_runtime\Scripts"

if not defined PYTHON if exist "%ROOT%.venv\Scripts\python.exe" set "PYTHON=%ROOT%.venv\Scripts\python.exe"
if not defined PYTHON_DIR if exist "%ROOT%.venv\Scripts\python.exe" set "PYTHON_DIR=%ROOT%.venv\Scripts"

if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python313\python.exe"
if not defined PYTHON_DIR if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYTHON_DIR=%LocalAppData%\Programs\Python\Python313"

if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python312\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python312\python.exe"
if not defined PYTHON_DIR if exist "%LocalAppData%\Programs\Python\Python312\python.exe" set "PYTHON_DIR=%LocalAppData%\Programs\Python\Python312"

if not defined PYTHON if exist "%LocalAppData%\Programs\Python\Python311\python.exe" set "PYTHON=%LocalAppData%\Programs\Python\Python311\python.exe"
if not defined PYTHON_DIR if exist "%LocalAppData%\Programs\Python\Python311\python.exe" set "PYTHON_DIR=%LocalAppData%\Programs\Python\Python311"

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
set "PATH=%PYTHON_DIR%;%PATH%"

title LoneWolf Fang Free Shell

echo ============================================================
echo LoneWolf Fang Free Shell
echo ============================================================
echo Root   : %ROOT%
echo Python : %PYTHON%
echo.
echo Example commands:
echo   python -m app.cli.app_main
echo   python backtest.py --help
echo   python runner.py --help
echo.
echo Supported run modes in free:
echo   PAPER
echo   REPLAY
echo   BACKTEST
echo ============================================================
echo.

cmd /k
