@echo off
rem Usage: scripts\clear-faults.bat (removes every injected faults on all 5 nodes)
for %%N in (1 2 3 4 5) do call "%~dp0client.bat" --target localhost:900%%N fault clear 
echo All faults cleared.