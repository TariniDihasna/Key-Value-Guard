@echo off
rem Usage: scripts\skew.bat nodeN -5000     (moves node N's clock by this many milliseconds)
if "%~2"=="" (
  echo Usage: scripts\skew.bat nodeN MS
  exit /b 1
)
set N=%~1
set N=%N:node=%
call "%~dp0client.bat" --target localhost:900%N% fault skew %~2