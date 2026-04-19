REM BUILD_ID: 2026-04-02_free_user_scope_installer_cmd_v1
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
echo Package root: %ROOT%
echo.
echo Press Enter at the prompt to install to the default Program Files path,
echo or enter a custom install directory when asked.
echo.

powershell.exe ^
  -NoLogo ^
  -NoProfile ^
  -ExecutionPolicy Bypass ^
  -File "%PS_SCRIPT%" ^
  %*

set "EXITCODE=%ERRORLEVEL%"

echo.
if not "%EXITCODE%"=="0" (
    echo [FAILED] Installer exited with code %EXITCODE%.
    echo Check %%LocalAppData%%\LoneWolfFang\logs\installer_free.log for details.
    pause
    exit /b %EXITCODE%
)

echo [OK] Installation finished successfully.
echo.
echo Next step:
echo   Use the desktop shortcut or run LoneWolfFangFreeLauncher.exe from the installed folder.
echo.
pause
exit /b 0
