REM BUILD_ID: 2026-03-31_free_shared_python_launcher_v1
@echo off
setlocal

set "ROOT=%~dp0"
cd /d "%ROOT%"

title LoneWolf Fang Free GUI

set "NATIVE_LAUNCHER=%ROOT%LoneWolfFangFreeLauncher.exe"
if exist "%NATIVE_LAUNCHER%" (
    "%NATIVE_LAUNCHER%" %*
    exit /b %ERRORLEVEL%
)

set "PS_SCRIPT=%ROOT%Launch_LoneWolf_Fang_Free_GUI.ps1"
if not exist "%PS_SCRIPT%" (
    echo [ERROR] Launch_LoneWolf_Fang_Free_GUI.ps1 was not found.
    echo Expected: %PS_SCRIPT%
    pause
    exit /b 1
)

powershell.exe ^
  -NoLogo ^
  -NoProfile ^
  -ExecutionPolicy Bypass ^
  -File "%PS_SCRIPT%" ^
  %*
set "EXITCODE=%ERRORLEVEL%"

if not "%EXITCODE%"=="0" (
    echo.
    echo [FAILED] Launcher exited with code %EXITCODE%.
    echo Check %LocalAppData%\LoneWolfFang\logs\launcher_free.log for details.
    pause
    exit /b %EXITCODE%
)

exit /b 0
