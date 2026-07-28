@echo off
setlocal
title XPDE - Stop

pushd "%~dp0"
echo.
echo [XPDE] Menghentikan seluruh proses XPDE...
echo.

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\stop-realtime.ps1"
set "XPDE_EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%XPDE_EXIT_CODE%"=="0" (
    echo [XPDE] Sebagian proses mungkin belum berhenti. Periksa pesan di atas.
) else (
    echo [XPDE] Seluruh proses yang dilacak sudah dihentikan.
)
echo.
pause
popd
exit /b %XPDE_EXIT_CODE%
