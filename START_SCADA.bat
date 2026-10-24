@echo off
cd /d "%~dp0"
if not exist node_modules (
  echo Dependencies not found. Please run npm install first.
  pause
  exit /b 1
)
start "LCEPL WebSCADA" /min cmd /k "cd /d "%~dp0" && npm start"
exit
