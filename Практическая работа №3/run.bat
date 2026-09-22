@echo off
chcp 65001 >nul
if not exist out mkdir out
javac -encoding UTF-8 -cp "lib\rxjava-3.1.8.jar;lib\reactive-streams-1.0.4.jar" -d out src\darkstore\reactive\*.java src\darkstore\reactive\model\*.java src\darkstore\reactive\data\*.java src\darkstore\reactive\pipeline\*.java || exit /b 1
java -Dfile.encoding=UTF-8 -cp "out;lib\rxjava-3.1.8.jar;lib\reactive-streams-1.0.4.jar" darkstore.reactive.Main %*
