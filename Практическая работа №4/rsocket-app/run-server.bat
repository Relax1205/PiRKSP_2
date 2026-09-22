@echo off
chcp 65001 >nul
set LIB=lib
set OUT=out
if not exist "%OUT%" (
    echo Compiling sources...
    javac -encoding UTF-8 -d "%OUT%" -cp "%LIB%\*" src\suppliers\rsocket\domain\*.java src\suppliers\rsocket\server\*.java src\suppliers\rsocket\client\*.java
)
java -Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -cp "%OUT%;%LIB%\*" suppliers.rsocket.server.ServerMain
pause
