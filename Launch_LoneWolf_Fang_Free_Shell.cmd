REM BUILD_ID: 2026-03-31_free_shared_python_launcher_v1
REM BUILD_ID: 2026-04-19_free_shared_market_data_shell_contract_v1
@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
if "%ROOT:~-1%"==":" set "ROOT=%ROOT%\"
cd /d "%ROOT%"

set "LWF_SHARED_ROOT=%LocalAppData%\LoneWolfFang"
set "LWF_SHARED_DATA_ROOT=%LWF_SHARED_ROOT%\data"
set "LWF_PRODUCT_DATA_ROOT=%LWF_SHARED_DATA_ROOT%\free"
set "PYTHON=%LWF_SHARED_ROOT%\venvs\free\Scripts\python.exe"
set "PYTHON_DIR=%LWF_SHARED_ROOT%\venvs\free\Scripts"

if not exist "%PYTHON%" (
    echo [ERROR] free venv python.exe was not found.
    echo Expected: %PYTHON%
    echo Run LoneWolf Fang free Setup first.
    pause
    exit /b 1
)

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "LWF_HOME=%ROOT%"
set "LWF_RUNTIME_ROOT=%LWF_PRODUCT_DATA_ROOT%\runtime"
set "LWF_CONFIGS_ROOT=%LWF_PRODUCT_DATA_ROOT%\configs"
set "LWF_MARKET_DATA_ROOT=%LWF_SHARED_DATA_ROOT%\market_data"
set "PYTHONPATH=%ROOT%app;%PYTHONPATH%"
set "PATH=%PYTHON_DIR%;%PATH%"

title LoneWolf Fang Free Shell

echo ============================================================
echo LoneWolf Fang Free Shell
echo ============================================================
echo Root   : %ROOT%
echo Python : %PYTHON%
echo Market : %LWF_MARKET_DATA_ROOT%
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
