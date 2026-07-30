@echo off
setlocal
cd /d "%~dp0"

if not exist "mcp\.venv\Scripts\python.exe" (
  py -m venv "mcp\.venv"
  if errorlevel 1 exit /b 1
)

"mcp\.venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 1

"mcp\.venv\Scripts\python.exe" -m pip install -e ".\mcp"
if errorlevel 1 exit /b 1

echo.
echo XPDE Read-Only MCP installed successfully.
echo Python: %CD%\mcp\.venv\Scripts\python.exe
echo Module: xpde_mcp.server
endlocal
