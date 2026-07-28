@echo off
setlocal
title XPDE - Install

pushd "%~dp0"
echo.
echo [XPDE] Menyiapkan dependency lokal...
echo.

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\setup.ps1"
set "XPDE_EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%XPDE_EXIT_CODE%"=="0" (
    echo [XPDE] Instalasi gagal. Periksa pesan error di atas.
) else (
    echo [XPDE] Instalasi selesai. Selanjutnya klik XPDE-Start.cmd.
)
echo.
pause
popd
exit /b %XPDE_EXIT_CODE%
