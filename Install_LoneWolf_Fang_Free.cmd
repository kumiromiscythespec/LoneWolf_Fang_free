REM BUILD_ID: 2026-03-25_free_installer_wrapper_v1
@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
if "%ROOT:~-1%"==":" set "ROOT=%ROOT%\"
cd /d "%ROOT%"

title LoneWolf Fang Free Installer

set "PS_SCRIPT=%ROOT%\packaging\install_free_local.ps1"

if not exist "%PS_SCRIPT%" (
    echo [ERROR] install_free_local.ps1 was not found.
    echo Expected: %PS_SCRIPT%
    pause
    exit /b 1
)

echo ============================================================
echo LoneWolf Fang Free Installer
echo ============================================================
echo Install root: %ROOT%
echo.

powershell.exe ^
  -NoLogo ^
  -NoProfile ^
  -ExecutionPolicy Bypass ^
  -File "%PS_SCRIPT%" ^
  -InstallRoot "%ROOT%" ^
  %*

set "EXITCODE=%ERRORLEVEL%"

echo.
if not "%EXITCODE%"=="0" (
    echo [FAILED] Installer exited with code %EXITCODE%.
    echo.
    echo Please review the PowerShell output above.
    pause
    exit /b %EXITCODE%
)

echo [OK] LoneWolf Fang Free installation finished successfully.
echo.
echo Next step:
echo   Use the desktop shortcut "LoneWolf Fang Free GUI".
echo.
pause
exit /b 0
