@echo off
setlocal enabledelayedexpansion

rem Builds polymarket_feed.exe with a MinGW-w64 g++ on PATH.
rem
rem Setup (one-time):
rem   1. Install MSYS2 from https://www.msys2.org
rem   2. Open "MSYS2 MinGW64" terminal and run:
rem        pacman -Syu
rem        (close/reopen terminal, then run pacman -Syu again if prompted)
rem        pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-boost ^
rem                  mingw-w64-x86_64-openssl mingw-w64-x86_64-nlohmann-json
rem   3. Add C:\msys64\mingw64\bin to your Windows PATH (System Properties ->
rem      Environment Variables -> Path -> New), then open a fresh cmd/PowerShell.
rem   4. Verify: g++ --version
rem
rem Usage:
rem   build.bat          builds polymarket_feed.exe
rem   build.bat clean    removes build artifacts
rem   build.bat run      builds, then runs it

if /i "%~1"=="clean" goto :clean
if /i "%~1"=="run"   goto :build_and_run
goto :build

:build
where g++ >nul 2>nul
if errorlevel 1 (
    echo [ERROR] g++ not found on PATH. See the setup notes at the top of this file.
    exit /b 1
)

set CXXFLAGS=-std=c++17 -O2 -Wall -Wextra -pthread -D_WIN32_WINNT=0x0601
rem Boost.System is header-only since Boost 1.69 -- recent MSYS2 Boost
rem packages often don't ship libboost_system.a at all, so it's dropped here.
rem If your Boost is old enough to need it, add -lboost_system back in.
set LDFLAGS=-lssl -lcrypto -lws2_32 -lmswsock -pthread

echo Compiling main.cpp...
g++ %CXXFLAGS% -c main.cpp -o main.o
if errorlevel 1 goto :failed

echo Compiling ws_client.cpp...
g++ %CXXFLAGS% -c ws_client.cpp -o ws_client.o
if errorlevel 1 goto :failed

echo Linking polymarket_feed.exe...
g++ %CXXFLAGS% main.o ws_client.o -o polymarket_feed.exe %LDFLAGS%
if errorlevel 1 goto :failed

echo.
echo Build succeeded: polymarket_feed.exe
goto :eof

:build_and_run
call "%~f0"
if errorlevel 1 exit /b 1
echo Running polymarket_feed.exe (Ctrl+C to stop)...
polymarket_feed.exe
goto :eof

:clean
echo Removing build artifacts...
del /q main.o ws_client.o polymarket_feed.exe 2>nul
echo Done.
goto :eof

:failed
echo.
echo [ERROR] Build failed. Common causes:
echo   - boost/openssl/nlohmann-json not installed via pacman (see setup notes above)
echo   - g++ from a different (non-MinGW64) MSYS2 environment is on PATH instead
exit /b 1