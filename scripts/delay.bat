@echo off
rem Usage: scripts\delay.bat nodeN MS  (adds a delay to everything node N sends)
if "%~2"=="" (
    echo Usage: scripts\delay.bat nodeN MS 
    exit /b 1
)

set N=%~1
set N=%N:node=%
call "%~dp0.client.bat" --target localhost:900%N% fault delay %~2
