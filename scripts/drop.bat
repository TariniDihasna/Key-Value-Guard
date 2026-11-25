@echo off
rem Usage: scripts\drop.bat nodeN 0.3     (node N loses 30 percent of its outgoing messages)
if "%~2"=="" (
  echo Usage: scripts\drop.bat nodeN PROBABILITY
  exit /b 1
)
set N=%~1
set N=%N:node=%
call "%~dp0client.bat" --target localhost:900%N% fault drop %~2