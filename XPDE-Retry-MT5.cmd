@echo off
setlocal
title XPDE - Retry MT5

pushd "%~dp0"
echo.
echo [XPDE] Menghubungkan ulang MT5 bridge...
echo.

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\retry-mt5-bridge.ps1"
set "XPDE_EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%XPDE_EXIT_CODE%"=="0" (
    echo [XPDE] Retry gagal. Pastikan MetaTrader 5 terbuka dan sudah login.
) else (
    echo [XPDE] MT5 bridge berhasil dijalankan ulang.
)
echo.
pause
popd
exit /b %XPDE_EXIT_CODE%
