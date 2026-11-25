@echo off
setlocal enabledelayedexpansion
rem Usage: scripts\partition.bat node3
if "%~1"=="" (
  echo Usage: scripts\partition.bat nodeN
  exit /b 1
)
set ISOLATED=%~1
set ISO_NUM=!ISOLATED:node=!
set OTHERS=
for %%N in (node1 node2 node3 node4 node5) do (
  if not "%%N"=="!ISOLATED!" (
    if defined OTHERS (set OTHERS=!OTHERS!,%%N) else (set OTHERS=%%N)
  )
)
rem 1. the isolated node ignores everybody else
call "%~dp0client.bat" --target localhost:900!ISO_NUM! fault block !OTHERS!
rem 2. everybody else ignores the isolated node
for %%N in (node1 node2 node3 node4 node5) do (
  if not "%%N"=="!ISOLATED!" (
    set NUM=%%N
    set NUM=!NUM:node=!
    call "%~dp0client.bat" --target localhost:900!NUM! fault block !ISOLATED!
  )
)
echo Partition done: !ISOLATED! is cut off.