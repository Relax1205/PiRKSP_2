@echo off
rem Использование: run.bat 1 [SUM^|MAX^|MIN] [размер]  ^|  run.bat 2  ^|  run.bat 3 [кол-во файлов]
chcp 65001 > nul
cd /d "%~dp0"
if not exist out mkdir out
javac -encoding UTF-8 -d out src\task1\*.java src\task2\*.java src\task3\*.java || exit /b 1
set TASK=%1
if "%TASK%"=="" set TASK=1
shift
java -Dfile.encoding=UTF-8 -cp out task%TASK%.Main %1 %2
