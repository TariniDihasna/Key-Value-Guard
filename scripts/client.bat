@echo off
rem Example: scripts\client.bat --target localhost:9001 put name Tarini
java -jar "%~dp0..\client\target\client.jar" %*
