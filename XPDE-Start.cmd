@echo off
setlocal
title XPDE - Start

pushd "%~dp0"
echo.
echo [XPDE] Menjalankan core, dashboard, dan MT5 bridge...
echo.

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\start-realtime.ps1"
set "XPDE_EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%XPDE_EXIT_CODE%"=="0" (
    echo [XPDE] Gagal dijalankan. Periksa folder work untuk log error.
    echo.
    pause
    popd
    exit /b %XPDE_EXIT_CODE%
)

echo [XPDE] Berjalan dalam mode live shadow.
echo [XPDE] Dashboard: http://localhost:3000/
start "" "http://localhost:3000/"
echo.
pause
popd
exit /b 0
