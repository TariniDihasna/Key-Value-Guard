@echo off
rem Starts 5 nodes, each in its own window (ports 9001 to 9005).
rem Closing a window = crashing that node. Run "mvn clean package" first.
set JAR=%~dp0..\node\target\node.jar
set PEERS=localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005

if not exist "%JAR%" (
  echo node.jar not found. Run "mvn clean package" in the project folder first.
  exit /b 1
)

for %%I in (1 2 3 4 5) do (
  start "node%%I" cmd /k java -jar "%JAR%" --id node%%I --port 900%%I --peers %PEERS%
)
echo Started 5 nodes. Close a window to crash that node.
