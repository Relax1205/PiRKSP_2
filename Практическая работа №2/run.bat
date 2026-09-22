@echo off
chcp 65001 >nul
if not exist out mkdir out
javac -encoding UTF-8 -cp lib\commons-io-2.15.1.jar -d out src\suppliers\*.java || exit /b 1
java -Dfile.encoding=UTF-8 -cp out;lib\commons-io-2.15.1.jar suppliers.Main %*
